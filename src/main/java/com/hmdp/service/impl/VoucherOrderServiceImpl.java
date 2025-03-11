package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisWorker;

import com.hmdp.utils.UserHolder;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisWorker redisWorker;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    @Resource
    private RedissonClient redissonClient2;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    //线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {
        private String queueName = "stream.orders";
        @Override
        public void run() {
            while(true) {
                try {
                    // 获取消息队列消息 xreadgroup group g1 c1 count 1 block 2000 streams stream.order >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    // 判断消息获取是否成功
                     if(list == null || list.isEmpty()) {
                         // 不存在,说明没有消息，继续下一次循环
                         continue;
                     }

                     // 解析消息订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 存在,可以下单
                    //创建订单
                    handVoucherOrder(voucherOrder);

                    // ACK 确认 sack stream.orders
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());

                }catch (Exception e) {
                    log.error("处理订单异常",e);

                        handlePendingList();

                }
            }
        }

        private void handlePendingList()  {
            while(true) {
                try {
                    // 获取PendingList 消息 xreadgroup group g1 c1 count 1 block 2000 streams stream.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );

                    // 判断消息获取是否成功
                    if(list == null || list.isEmpty()) {
                        // 不存在,说明没有消息，结束循环
                       break;
                    }

                    // 解析消息订单信息
                    MapRecord<String, Object, Object> record = list.get(0);

                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    // 存在,可以下单
                    //创建订单
                    handVoucherOrder(voucherOrder);

                    // ACK 确认 sack stream.orders
                    stringRedisTemplate.opsForStream().acknowledge(queueName,"g1",record.getId());

                }catch (Exception e) {
                    log.error("处理订单异常",e);
                   try {
                       Thread.sleep(20);
                   }catch (InterruptedException e1) {
                       e1.printStackTrace();
                   }
                }
            }
        }
    }

    // 阻塞队列
//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024*1024);

//    @PostConstruct
//    private void init() {
//        SECKILL_ORDER_EXECUTOR.submit(new VoucherHandler());
//    }
//    private class VoucherHandler implements Runnable{
//        @Override
//        public void run() {
//            while(true){
//                // 获取订单信息
//                try {
//                    VoucherOrder voucherOrder = orderTasks.take();
//
//                    //创建订单
//                    handVoucherOrder(voucherOrder);
//                } catch (InterruptedException e) {
//                    log.error("处理订单异常");
//                    throw new RuntimeException(e);
//                }
//
//            }
//        }
//    }

    private void handVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();

        RLock lock = redissonClient.getLock("lock:order:"+userId);

        boolean isLock = lock.tryLock();

        if(!isLock){
            log.error("不允许重复下单");
           return ;
        }

        try {
             proxy.createVoucherOrder(voucherOrder);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    private IVoucherOrderService proxy;


    @Override
    public Result seckillVoucher(Long voucherId) {
        //获取用户
        Long userId = UserHolder.getUser().getId();

        // 获取订单
        long orderId = redisWorker.nextId("order");

        // 执行lua脚本
        Long res = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(),String.valueOf(orderId)
        );

        // 判断结果是否为0
        int r = res.intValue();
        if(r != 0){
            // 不为 0,没有购买资格
            return Result.fail(r == 1 ? "库存不足":"不能重复下单");
        }

        // 获取代理对象(事务)
        proxy = (IVoucherOrderService) AopContext.currentProxy();

        // 返回id
        return Result.ok(orderId);
    }

//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        //获取用户
//        Long userId = UserHolder.getUser().getId();
//
//        // 执行lua脚本
//        Long res = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//        // 判断结果是否为0
//        int r = res.intValue();
//        if(r != 0){
//            // 不为 0,没有购买资格
//            return Result.fail(r == 1 ? "库存不足":"不能重复下单");
//        }
//        //2.2 有购买资格，把下单 信息存储到阻塞队列
//        VoucherOrder voucherOrder = new VoucherOrder();
//
//       long orderId = redisWorker.nextId("order");
//
//       voucherOrder.setId(orderId);
//
//       voucherOrder.setUserId(userId);
//
//       voucherOrder.setVoucherId(voucherId);
//
//       // 放入阻塞队列
//        // 创建阻塞队列
//        orderTasks.add(voucherOrder);
//
//        // 获取代理对象(事务)
//         proxy = (IVoucherOrderService) AopContext.currentProxy();
//
//        // 返回id
//        return Result.ok(orderId);
//    }


//    @Override
//
//    public Result seckillVoucher(Long voucherId) {
//        // 查询优惠劵
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//
//        // 判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            // 未开始
//            return Result.fail("秒杀未开始");
//        }
//        // 判断是否结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已经结束");
//        }
//        // 判断库存是否充足
//        if(voucher.getStock() < 1){
//            return Result.fail("库存不足");
//        }
//
//        Long userId = UserHolder.getUser().getId();
//
//        // 创建锁对象
//        // SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + userId);
//
//        // 获取锁
//        boolean isLock = lock.tryLock();
//
//        if(!isLock){
//           // 获取锁失败，返回错误或重试
//            return Result.fail("一个人只运行下一单");
//        }
//
//            // 获取代理对象
//        try {
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }finally {
//            // 释放锁
//            lock.unlock();
//        }
//    }


    @Transactional
    public  Result createVoucherOrder(Long voucherId) {

        // 查询订单
        Long userId = UserHolder.getUser().getId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();

        VoucherOrder voucherOrder;

            if (count > 0) {
                // 用户已经购买过了
                return Result.fail("用户已经购买一次了!");
            }

            // 扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")
                    .eq("voucher_id", voucherId).gt("stock", 0).update();
            if (!success) {
                return Result.fail("库存不足");
            }
            // 创建订单
            voucherOrder = new VoucherOrder();
            // 订单id
            long orderId = redisWorker.nextId("order");
            voucherOrder.setId(orderId);
            //用户id
            voucherOrder.setUserId(userId);
            //代金券id
            voucherOrder.setVoucherId(voucherId);
            // 返回订单id
            save(voucherOrder);
            return Result.ok(voucherOrder);
    }

    @Transactional
    @Override
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        // 查询订单
        Long userId = voucherOrder.getUserId();
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();


        if (count > 0) {
            // 用户已经购买过了
           // return Result.fail("用户已经购买一次了!");
            log.error("用户已经购买一次了!");
            return ;
        }

        // 扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherOrder.getVoucherId()).gt("stock", 0).update();
        if (!success) {
            log.error("库存不足!");
            return ;
            // return Result.fail("库存不足");
        }
        // 返回订单id
        save(voucherOrder);

    }
}
