package wordcount.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import wordcount.utils.Utils;

import java.util.Map;

/**
 * Created by lili on 2017/6/17.
 */
public class WordSpout implements IRichSpout {

    public static final long serialVersionUID = 1L;

    private SpoutOutputCollector collector;

    private int index = 0;

    private String[] sentences = {
            "my dog has fleas",
            "i like cold beverages",
            "the dog ate my homework",
            "don't have a cow man",
            "i dont't think i like fleas"
    };

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {
        System.out.println("..........word spout stop........");

    }

    @Override
    public void deactivate() {

    }

    @Override
    public void nextTuple() {
        //永不停止 一直会发送  循环发送句子
        this.collector.emit(new Values(sentences[index]));
        index++;
        if (index >= sentences.length) {
            index = 0;
        }
        //去掉之后停不下来程序  why？？？
        Utils.waitForSeconds(1);
    }

    @Override
    public void ack(Object o) {

    }

    @Override
    public void fail(Object o) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
