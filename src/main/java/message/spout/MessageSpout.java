package message.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by lili on 2017/6/17.
 */
public class MessageSpout implements IRichSpout {


    private static final long serialVersionUID = 1L;

    private int index = 0;

    private String[] subjects = new String[]{
            "groovy,oeacnbase",
            "openfire,restful",
            "flume,activiti",
            "hadoop,hbase",
            "spark,sqoop"
    };

    private SpoutOutputCollector collector ;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        if(index<subjects.length){
            String sub = subjects[index];
            //emit(List<Object> tuple, Object messageId)
            collector.emit(new Values(sub),index);
            index++;
        }
    }

    @Override
    public void ack(Object o) {
        //发送成功回调此函数
        System.out.println("[消息发送成功！！！](messageId="+ o+ ")");

    }

    @Override
    public void fail(Object o) {
        System.out.println("[消息发送失败！！！](messageId="+ o+ ")");
        System.out.println("[消息重新发送中......]");
        //发送失败会返回
        collector.emit(new Values(subjects[(Integer) o]),o);
        System.out.println("[消息重发成功！！！]");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("subjects"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
