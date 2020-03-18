package bussiness.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lili on 2017/6/18.
 */
public class LogMergeBolt extends BaseRichBolt {
    private transient OutputCollector collector;
    private HashMap<String,String> srcMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        if(srcMap==null){
            srcMap = new HashMap<>();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String streamId = tuple.getSourceStreamId();
        if(streamId.equals("visit")){
            String user = tuple.getStringByField("user");
            String srcId = tuple.getStringByField("srcid");
            //所有用户
            srcMap.put(user,srcId);
        }else if(streamId.equals("business")){
            String user = tuple.getStringByField("user");
            String pay = tuple.getStringByField("pay");
            String srcId = srcMap.get(user);
            if(srcId!=null){
                collector.emit(new Values(user,pay,srcId));
                //没统计一个删除一个用户
                srcMap.remove(user);
            }else{
                //成交日志快于流量日志时才会发生
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("user","pay","srcid"));
    }
}
