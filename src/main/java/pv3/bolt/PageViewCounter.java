package pv3.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;

import java.util.*;

/**
 * Created by lili on 2017/6/18.
 */
public class PageViewCounter implements IRichBolt {

    private OutputCollector collector;
    private HashMap<String,Long> counts = null;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        this.counts = new HashMap<String,Long>();
    }

    @Override
    public void execute(Tuple tuple) {
        String url = tuple.getStringByField("page");
        Long count = this.counts.get(url);
        if(count==null){
            count = 0L;
        }
        count++;
        this.counts.put(url,count);
    }

    @Override
    public void cleanup() {
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

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
