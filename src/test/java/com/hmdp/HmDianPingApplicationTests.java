package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisWorker;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.info.ProjectInfoAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@SpringBootTest
class HmDianPingApplicationTests {
     @Resource
    private RedisWorker redisWorker;

     @Resource
     private IShopService shopService;

     @Autowired
     private StringRedisTemplate  stringRedisTemplate;
     private ExecutorService es = Executors.newFixedThreadPool(500);
    @Autowired
    private ProjectInfoAutoConfiguration projectInfoAutoConfiguration;

    @Test
    void testIdWorker() throws InterruptedException {
         CountDownLatch latch = new CountDownLatch(300);


         Runnable task = () -> {
             for(int i=0;i<100;i++){
                 long id = redisWorker.nextId("order");
                 System.out.println("id = "+id);
             }
             latch.countDown();
         };

         long begin = System.currentTimeMillis();
         for(int i=0;i<300;i++){
             es.submit(task);
         }
         latch.await();
         long end = System.currentTimeMillis();
         System.out.println("time = "+(end-begin));


     }

     @Test
    void loadShopData() {
         List<Shop> list = shopService.list();

         Map<Long,List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));

         for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {

             Long typeId = entry.getKey();
             String key = "shop:geo:"+typeId;
             List<Shop> value = entry.getValue();

             List<RedisGeoCommands.GeoLocation<String>> locations = new ArrayList<>(value.size());

             int cnt = 0;
             // 写入redis geoAdd key 经度,纬度
             for (Shop shop : value) {
                 cnt++;
               //  stringRedisTemplate.opsForGeo().add(key,new Point(shop.getX(),shop.getY()),shop.getId().toString());
                 locations.add(new RedisGeoCommands.GeoLocation<>(shop.getId().toString(),new Point(shop.getX(),shop.getY())));
             }
             stringRedisTemplate.opsForGeo().add(key,locations);
             //System.out.println("typeId = "+typeId + cnt);
         }
     }

}
