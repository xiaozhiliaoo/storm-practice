package wordcount.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.*;

/**
 * Created by lili on 2017/6/17.
 */
public class WordReportBolt implements IRichBolt {

    private HashMap<String,Long> counts = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counts = new HashMap<>();
    }

    @Override
    public void execute(Tuple tuple) {
        String word = tuple.getStringByField("word");
        Long count = tuple.getLongByField("count");
        this.counts.put(word,count);
    }

    @Override
    public void cleanup() {
        /**
         * 拓扑停止前的方法
         */
        System.out.println("------------Final Count Start----------------");
        List<String> keys = new ArrayList<>();
        keys.addAll(this.counts.keySet());
        Collections.sort(keys);
        for(String key:keys){
            System.out.println(key+" : " +this.counts.get(key));
        }
        System.out.println("----------------Final Count End-------------");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //reportBolt不提交任何

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
