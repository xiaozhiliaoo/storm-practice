package message.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * Created by lili on 2017/6/17.
 */
public class WriterBolt implements IRichBolt {

    private static final long serialVersionUID = 1L;

    private OutputCollector collector;

    private FileWriter writer;

    private boolean flag = false;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        try {
            writer = new FileWriter("d://m.txt");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple) {

        String word = tuple.getStringByField("word");
        try {
//        List<String> list = (List<String>) tuple.getValueByField("word");
//        System.out.println("------"+list);
            //hadoop失败后  会导致hbase重发  建议批量发送
//            if(!flag && word.equals("hadoop")){
//                flag = true;
//                int a = 1/0;
//            }
            writer.write(word);
            writer.write("\r\n");
            writer.flush();
        } catch (IOException e) {
            e.printStackTrace();
            collector.fail(tuple);
        }
        collector.emit(tuple, new Values(word));
        collector.ack(tuple);

    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
