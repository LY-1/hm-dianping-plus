-- 优惠券ID
local voucherId = ARGV[1]
-- 用户ID
local userId = ARGV[2]
-- 订单ID
local orderID = ARGV[3]
-- 库存key
local stockKey = "seckill:stock:" .. voucherId
-- 订单key
local orderKey = "seckill:order:" .. voucherId

-- 判断库存是否充足
if (tonumber(redis.call('get', stockKey)) <= 0) then
    return 1
end
-- 判断用户是否下单
if (redis.call('sismember', orderKey, userId) == 1) then
    return 2
end
-- 扣减库存
redis.call('incrby', stockKey, -1)
-- 将userID存入当前的券set
redis.call('sadd', orderKey, userId)
-- 发送消息到队列中， XADD strea.orders * k1 v1 ...
redis.call('xadd', 'stream.orders', '*', 'voucherId', voucherId, 'userId', userId, 'id', orderID)
return 0