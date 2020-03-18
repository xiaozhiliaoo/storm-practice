package redis;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Created by lili on 2017/6/21.
 */
public class StormRedisBolt implements IBasicBolt {

    private static final long servialVersionUID = 1L;
    private RedisOperations redisOperations = null;
    private String redisIP = null;
    private int port ;

    public StormRedisBolt(String redisIP, int port) {
        this.redisIP = redisIP;
        this.port = port;
    }



    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        redisOperations = new RedisOperations(this.redisIP,this.port);
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        Map<String,Object> record = new HashMap();
        record.put("firstName",input.getValueByField("firstName"));
        record.put("lastName",input.getValueByField("lastName"));
        record.put("companyName",input.getValueByField("companyName"));
        redisOperations.insert(record, UUID.randomUUID().toString());
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
