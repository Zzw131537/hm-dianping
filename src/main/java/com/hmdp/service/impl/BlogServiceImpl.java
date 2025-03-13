package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.Result;
import com.hmdp.dto.ScrollResult;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
   @Resource
    private IBlogService blogService;

   @Resource
   private IUserService userService;

   @Resource
   private IFollowService followService;
   @Autowired
   private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryHotBlog(Integer current) {
                // 根据用户查询
        Page<Blog> page = blogService.query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        records.forEach(blog -> {
            this.queryBlogUser(blog);
            this.isBlockLiked(blog);
        });
        return Result.ok(records);
    }

    @Override
    public Result queryBlogById(Long id) {
        
        // 查询blog
        Blog blog = blogService.getById(id);
        if(blog==null){
            return Result.fail("笔记不存在");
        }
        // 查询blog用户
        queryBlogUser(blog);

        // 查询blog 是否被点赞 l了
        isBlockLiked(blog);
        return Result.ok(blog);
    }

    // 修改 Blog.isLiked
    private void isBlockLiked(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        if(user == null){
            // 未登录
            return ;
        }
        Long userId = user.getId();
        // 判断当前用户是否点赞
        String key ="blog:liked:"+blog.getId();
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
       blog.setIsLike(BooleanUtil.isTrue(score != null));
    }

    // 点赞功能
    @Override
    public Result likeBlog(Long id) {
        // 获取登录用户
        Long userId = UserHolder.getUser().getId();
        // 判断当前用户是否点赞
        String key ="blog:liked:"+id;
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());

        // 如果没有
        if(score == null){
            // 数据库点赞数加1
            boolean isSuccess = update().setSql("liked = liked + 1").eq("id", id).update();
            // 保存到redis
            if(isSuccess){
                stringRedisTemplate.opsForZSet().add(key, userId.toString(),System.currentTimeMillis());
            }
        }else {
            // 已点赞 -> 取消点赞
            boolean isSuccess = update().setSql("liked = liked - 1").eq("id", id).update();

            // 数据库点赞数减 1
            if(isSuccess){
                // 从redis 的set 集合中移除
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }
        return Result.ok();
    }

    @Override
    public Result queryBlogLikes(Long id) {
        //实现查询 Top5的点赞用户 zrange key 0 4
        String key ="blog:liked:"+id;
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if(top5==null || top5.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        // 解析出用户Id
        List<Long> ids = top5.stream().map(Long::valueOf).collect(Collectors.toList());
        String idStr = StrUtil.join(",", ids);
        // 根据用户Id 查询用户

        List<UserDTO> dtoList = userService.query().in("id",ids)
                .last("order by field(id,"+idStr+")").list()
                .stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        return Result.ok(dtoList);
    }

    @Override
    public Result saveBlog(Blog blog) {
        // 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 保存探店博文
        boolean isSuccess = save(blog);

        if(!isSuccess){
            return Result.fail("新增笔记失败!");
        }
        //3. 查询笔记作者的所有粉丝 select * from tb_follow   where follow_user_id = ?
        List<Follow> follows = followService.query()
                .eq("follow_user_id", user.getId()).list();
        //4. 推送笔记id给粉丝
        for (Follow follow : follows) {
            // 获取分数Id
            Long userId = follow.getUserId();
            // 推送收件箱 sorted_set
            String key = "feed:"+userId;

            stringRedisTemplate.opsForZSet().add(key,blog.getId().toString(),System.currentTimeMillis());



        }
        // 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {

        // 1. 找到收件箱
        Long userId = UserHolder.getUser().getId();

        // 查询收件箱
        String key = "feed:"+userId;

        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 3);


        // 判断
        if(typedTuples==null || typedTuples.isEmpty()){
            return Result.ok(Collections.emptyList());
        }

        // 解析数据 BlogId minTime offset
        List<Long> ids = new ArrayList<>(typedTuples.size());
        Long minTime = 0L;
        int os = 1;
        for (ZSetOperations.TypedTuple<String> typedTuple : typedTuples) {
            // 获取 Id
            ids.add(Long.valueOf(typedTuple.getValue()));

            long time = typedTuple.getScore().longValue();
            if(time == minTime){
                os+=1;
            }else if(time < minTime){
                minTime = time;
                os=1;
            }

        }

        // 根据Id 返回Blog集合
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id",ids).last("order by field(id,"+idStr+")").list();

        for (Blog blog : blogs) {
            queryBlogUser(blog);
            isBlockLiked(blog);
        }
        // 封装并返回
        ScrollResult r = new ScrollResult();
        r.setList(blogs);
        r.setMinTime(minTime);
        r.setOffset(os);

        return Result.ok(r);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }
}
