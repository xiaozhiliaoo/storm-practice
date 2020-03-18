package pv2.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by lili on 2017/6/18.
 */
public class LogStat extends BaseRichBolt {

    private OutputCollector collector;
    private Map<String,Integer> pvMap = new HashMap<>();
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        if(streamId.equals("log")) {
            String user = tuple.getStringByField("user");
            if (pvMap.containsKey(user)) {
                pvMap.put(user, pvMap.get(user) + 1);
            } else {
                pvMap.put(user, 1);
            }
        }
        if(streamId.equals("stop")){
            Iterator<Map.Entry<String, Integer>> iterator = pvMap.entrySet().iterator();
            while (iterator.hasNext()){
                Map.Entry<String, Integer> entry = iterator.next();
                collector.emit(new Values(entry.getKey(),entry.getValue()));
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("user","pv"));
    }
}
