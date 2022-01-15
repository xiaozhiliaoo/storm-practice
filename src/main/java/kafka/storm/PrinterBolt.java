package kafka.storm;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * Created by lili on 2017/6/18.
 */
public class PrinterBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        String sentence = input.getString(0);
        System.out.println("Receive Sentence：" + sentence);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
