package com.hmdp.service.impl;

import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryList() {
        // 1.查询redis是否存在
        String typeJson = stringRedisTemplate.opsForValue().get("cache:shopType");
        // 2. 存在直接返回
        if(StrUtil.isNotBlank(typeJson)){
            List<ShopType> shopTypes = JSONUtil.toList(typeJson, ShopType.class);
            return Result.ok(shopTypes);
        }

        //3. 不存在向数据库中查询
        List<ShopType> shopTypes = query().orderByAsc("sort").list();
        //4.不存在 ，返回错误
        if (shopTypes == null){
            return Result.fail("商铺列表不存在");
        }
        //5.存在 写入redis
        stringRedisTemplate.opsForValue().set("cache:shopType", JSONUtil.toJsonStr(shopTypes));
        //6.返回
        return Result.ok(shopTypes);
    }
}
