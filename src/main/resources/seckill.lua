local voucherId = ARGV[1]
local userId = ARGV[2]

-- 1.3 订单 id
local orderId = ARGV[3]

local stockKey = 'seckill:stock:' .. voucherId  -- 注意冒号分隔符
local orderKey = 'seckill:order:' .. voucherId

-- 处理键不存在的情况（返回0）
if (tonumber(redis.call('get',stockKey)) <= 0)then
    return 1
end

if redis.call('sismember', orderKey, userId) == 1 then
    return 2
end

redis.call('incrby', stockKey, -1)
redis.call('sadd', orderKey, userId)
--3.6 发送消息到队列中 xadd stream.orders * k1 v1 k2 v2
redis.call('xadd','stream.orders','*','userId',userId,'voucherId',voucherId,'id',orderId)

return 0  -- 成功