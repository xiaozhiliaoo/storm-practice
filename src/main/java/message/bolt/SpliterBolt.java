package message.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by lili on 2017/6/17.
 */
public class SpliterBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;

    private OutputCollector collector;

    private boolean flag = false;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        try {
            String subjects = tuple.getStringByField("subjects");
//            if(!flag && subjects.equals("flume,activiti")){
//                flag = true;
//                int a = 1/0;
//            }
            String[] words = subjects.split(",");
            List<String> list =  new ArrayList<>();
            int index = 0;
            for(String word:words){
                collector.emit(tuple,new Values(word));
//                list.add(word);
//                index++;
            }
//            collector.emit(tuple,new Values(list));
            collector.ack(tuple);
        } catch (Exception e) {
            e.printStackTrace();
            //失败后重新发送  把tuple传回去
            collector.fail(tuple);
        }
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
