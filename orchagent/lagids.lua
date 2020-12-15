-- KEYS - lagid_start, lagid_end, lagids, lagid_set
-- ARGV[1] - operation (add/del/get)
-- ARGV[2] - lag name
-- ARGV[3] - current lag id (for "add" operation only)

-- return lagid if success for "add"/"del"
-- return 0 if lag not exists for "del"
-- return -1 if lag table full for "add"
-- return -2 if log does exist for "get"
-- return -3 if invalid operation

local op = ARGV[1]
local pcname = ARGV[2]

local lagid_start = tonumber(redis.call("get", KEYS[1]))
local lagid_end = tonumber(redis.call("get", KEYS[2]))

if op == "add" then

    local plagid = tonumber(ARGV[3])

    local dblagid = redis.call("hget", KEYS[3], pcname)

    if dblagid then
        dblagid = tonumber(dblagid)
        if plagid == 0 then
            -- no lagid propsed. Return the existing lagid
            redis.call("sadd", KEYS[4], tostring(dblagid))
            return dblagid
        end
    end

    -- lagid allocation request with a lagid proposal
    if plagid >= lagid_start and plagid <= lagid_end then
        if plagid == dblagid then
            -- proposed lagid is same as the lagid in database
            redis.call("sadd", KEYS[4], tostring(plagid))
            return plagid
        end
        -- proposed lag id is different than that in database OR
        -- the portchannel does not exist in the database
        -- If proposed lagid is available, return the same proposed lag id
        if redis.call("sismember", KEYS[4], tostring(plagid)) == 0 then
            redis.call("sadd", KEYS[4], tostring(plagid))
            redis.call("srem", KEYS[4], tostring(dblagid))
            redis.call("hset", KEYS[3], pcname, tostring(plagid))
            return plagid
        end
    end

    local lagid = lagid_start
    while lagid <= lagid_end do
        if redis.call("sismember", KEYS[4], tostring(lagid)) == 0 then
            redis.call("sadd", KEYS[4], tostring(lagid))
            redis.call("srem", KEYS[4], tostring(dblagid))
            redis.call("hset", KEYS[3], pcname, tostring(lagid))
            return lagid
        end
        lagid = lagid + 1
    end

    return -1

end

if op == "del" then

    if redis.call("hexists", KEYS[3], pcname) == 1 then
        local lagid = redis.call("hget", KEYS[3], pcname)
        redis.call("srem", KEYS[4], lagid)
        redis.call("hdel", KEYS[3], pcname)
        return tonumber(lagid)
    end

    return 0

end

if op == "get" then

    if redis.call("hexists", KEYS[3], pcname) == 1 then
        local lagid = redis.call("hget", KEYS[3], pcname)
        return tonumber(lagid)
    end

    return -2

end

return -3
