package pv.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lili on 2017/6/18.
 */
public class LogStat extends BaseRichBolt {

    private OutputCollector collector;
    private Map<String, Integer> pvMap = new HashMap<>();

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String user = tuple.getStringByField("user");
        if (pvMap.containsKey(user)) {
            pvMap.put(user, pvMap.get(user) + 1);
        } else {
            pvMap.put(user, 1);
        }
        collector.emit(new Values(user, pvMap.get(user)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("user", "pv"));
    }
}
