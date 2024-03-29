package bussiness.bolt;


import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lili on 2017/6/18.
 */
public class LogStatBolt extends BaseRichBolt {
    private transient OutputCollector collector;
    // 来自某个渠道的成交量
    private HashMap<String, Long> scrpay;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        if (scrpay == null) {
            scrpay = new HashMap<String, Long>();
        }
    }

    @Override
    public void execute(Tuple tuple) {
        String pay = tuple.getStringByField("pay");
        String srcId = tuple.getStringByField("srcid");

        if (scrpay.containsKey(srcId)) {
            scrpay.put(srcId, Long.parseLong(pay) + scrpay.get(srcId));
        } else {
            scrpay.put(srcId, Long.parseLong(pay));
        }

        /*Iterator<Map.Entry<String, Long>> iterator = scrpay.entrySet().iterator();

        while (iterator.hasNext()){
            Map.Entry<String, Long> entry = iterator.next();
            collector.emit(new Values(entry.getKey(),entry.getValue()));
        }*/
        collector.emit(new Values(srcId, scrpay.get(srcId)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("srcId", "paySum"));

    }
}
