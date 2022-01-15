package redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import redis.clients.jedis.Jedis;

import java.util.Map;

/**
 * Created by lili on 2017/6/21.
 */
public class RedisOperations {

    private String redisIP;

    private int port;

    private static final long serialVersionUID = 1L;

    Jedis jedis = null;

    public RedisOperations(String redisIP, int port) {
        jedis = new Jedis(redisIP, port);
    }

    public void insert(Map<String, Object> record, String id) {
        try {
            jedis.set(id, new ObjectMapper().writeValueAsString(record));
        } catch (JsonProcessingException e) {
            System.out.println("Record not persist into datastore");
            e.printStackTrace();
        }
    }
}
