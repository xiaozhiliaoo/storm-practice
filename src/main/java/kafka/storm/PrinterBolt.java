package kafka.storm;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

/**
 * Created by lili on 2017/6/18.
 */
public class PrinterBolt extends BaseBasicBolt{
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String sentence = input.getString(0);
        System.out.println("Receive Sentence："+sentence);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
