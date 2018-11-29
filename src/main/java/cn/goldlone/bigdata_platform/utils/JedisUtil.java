package cn.goldlone.bigdata_platform.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Transaction;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @author Created by CN on 2018/08/12/0012 12:21 .
 */
@Component
public class JedisUtil implements InitializingBean {

    private static final Logger logger = LoggerFactory.getLogger(JedisUtil.class);

    @Value("${redisHost}")
    private String redisHost;

    private JedisPool jedisPool;

    @Override
    public void afterPropertiesSet() throws Exception {
        jedisPool = new JedisPool(redisHost);
    }

    public long sadd(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();

            return jedis.sadd(key, value);

        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        } finally {
            if(jedis != null)
                jedis.close();
        }

        return 0;
    }

    public long scard(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();

            return jedis.scard(key);

        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        } finally {
            if(jedis != null)
                jedis.close();
        }

        return 0;
    }

    public long srem(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();

            return jedis.srem(key, value);

        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        } finally {
            if(jedis != null)
                jedis.close();
        }

        return 0;
    }

    public boolean sismember(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();

            return jedis.sismember(key, value);

        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        } finally {
            if(jedis != null)
                jedis.close();
        }

        return false;
    }

    public long lpush(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();

            return jedis.lpush(key, value);

        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        } finally {
            if(jedis != null)
                jedis.close();
        }

        return 0;
    }

    public List<String> brpop(int timeout, String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();

            return jedis.brpop(timeout, key);

        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        } finally {
            if(jedis != null)
                jedis.close();
        }

        return null;
    }

    public List<String> lrange(String key, int start, int end) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();

            return jedis.lrange(key, start, end);
        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        } finally {
            if(jedis != null)
                jedis.close();
        }

        return null;
    }

    public long lrem(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();

            return jedis.lrem(key, 0, value);
        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        } finally {
            if(jedis != null)
                jedis.close();
        }

        return 0;
    }

    public long llen(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();

            return jedis.llen(key);
        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        } finally {
            if(jedis != null)
                jedis.close();
        }

        return 0;
    }


    public Jedis getJedis() {
        try {
            return jedisPool.getResource();
        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        }

        return null;
    }

    public Transaction multi(Jedis jedis) {
        try {
            return jedis.multi();
        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        }

        return null;
    }

    public List<Object> exec(Jedis jedis, Transaction tran) {
        try {

            return tran.exec();
        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        } finally {
            if(tran != null) {
                try {
                    tran.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(jedis != null)
                jedis.close();
        }

        return null;
    }

    public long zadd(String key, double score, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();

            return jedis.zadd(key, score, value);

        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        } finally {
            if(jedis != null)
                jedis.close();
        }

        return 0;
    }

    public long zrem(String key, String value) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();

            return jedis.zrem(key, value);

        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        } finally {
            if(jedis != null)
                jedis.close();
        }

        return 0;
    }

    public Set<String> zrange(String key, int start, int end) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();

            return jedis.zrange(key, start, end);

        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        } finally {
            if(jedis != null)
                jedis.close();
        }

        return null;
    }

    public Set<String> zrevrange(String key, int start, int end) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();

            return jedis.zrevrange(key, start, end);

        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        } finally {
            if(jedis != null)
                jedis.close();
        }

        return null;
    }

    public long zcard(String key) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();

            return jedis.zcard(key);

        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        } finally {
            if(jedis != null)
                jedis.close();
        }

        return 0;
    }

    public Double zscore(String key, String member) {
        Jedis jedis = null;
        try {
            jedis = jedisPool.getResource();

            return jedis.zscore(key, member);

        } catch (Exception e) {
            logger.error("出现异常" + e.getMessage());
        } finally {
            if(jedis != null)
                jedis.close();
        }

        return null;
    }
}
