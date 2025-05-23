package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result sendCode(String phone, HttpSession session) {

        // 1. 校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 2. 如果不符合，返回错误信息
            return Result.fail("手机号格式错误");
        }
        // 3.符合，生成验证码
        String code = RandomUtil.randomNumbers(6);

        // 4.保存验证码到redis 加业务前缀
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY +phone,code,RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES);

        // 5.模拟发送验证码
        log.debug("发送短信验证码成功,验证码 {}",code);

        //返回 ok
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1. 校验手机号
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误");
        }
        // 2. 校验验证码
        Object cacheCode = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (cacheCode == null || !cacheCode.toString().equals(code)) {
            // 3.不一样，报错
            return Result.fail("验证码错误");
        }

        // 4 根据手机号查询用户 select * form tb_user where phone = ?
        User user = query().eq("phone", phone).one();

        // 5. 判断用户是否存在
        if (user == null){
            // 6.不存在，创建新用户并保存
            user = createUserWithPhone(phone);
        }

        // 7. 保存用户信息到redis中
        // 7.1 生成token
        String token = UUID.randomUUID().toString(true);
        //7.2 将User 对象转化为Map储存
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create().setIgnoreNullValue(true)
                        .setFieldValueEditor((fieldName,fieldValue)->fieldValue.toString()));
        //7.3 储存
        stringRedisTemplate.opsForHash().putAll(RedisConstants.LOGIN_USER_KEY+token,userMap);
         // 7.4 设置token有效期
        stringRedisTemplate.expire(RedisConstants.LOGIN_USER_KEY+token,RedisConstants.LOGIN_USER_TTL,TimeUnit.MINUTES);
        //8 返回token
        return Result.ok(token);
    }


    @Override
    public Result sign() {

        // 获取当前用户
        Long userId = UserHolder.getUser().getId();

        // 获取日期
        LocalDateTime now = LocalDateTime.now();


        // 拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));

        String key ="sign:"+userId+keySuffix;

        // 存value
        int dayOfMonth = now.getDayOfMonth();

        // 写入redis
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth-1,true);

        return Result.ok();

    }

    @Override
    public Result signCount() {
        // 获取当前用户
        Long userId = UserHolder.getUser().getId();

        // 获取日期
        LocalDateTime now = LocalDateTime.now();


        // 拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));

        String key ="sign:"+userId+keySuffix;

        // 存value
        int dayOfMonth = now.getDayOfMonth();

        // 获取当前记录
        List<Long> res = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0)
        );
        // 循环遍历
       if(res == null || res.isEmpty()){
           return Result.ok(0);
       }

       Long num= res.get(0);
       if(num == null || num == 0){
           return Result.ok(0);
       }
        // 进行运算获取最长连续1
        int cnt=0;
        while(true){

            if ((num & 1) == 0) {
                break;
            }else {
                cnt++;
            }
            num>>>=1;
        }


        return Result.ok(cnt);
    }


    private User createUserWithPhone(String phone) {
        // 创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX +RandomUtil.randomString(10));
        save(user); // 保存用户到 tb_user
        return user;
    }
}
