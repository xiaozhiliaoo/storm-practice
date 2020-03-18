package pv3.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by lili on 2017/6/18.
 */
public class LogAnalysis implements IRichBolt{
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        //192.168.111.943 1497775091321 requestMethod943 www.baidu.com/943.html
        String logLine = tuple.getString(0);
        String[] input_fileds = logLine.toString().split(" ");
        collector.emit(new Values(input_fileds[3])); //request url
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("page"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
