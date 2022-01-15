package redis;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * Created by lili on 2017/6/21.
 */
public class SampleSpout extends BaseRichSpout {

    private static final long serialVersionUID = 1L;

    private SpoutOutputCollector spoutOutputCollector;

    private static final Map<Integer, String> FIRSTNAMEMAP = new HashMap<>();

    static {
        FIRSTNAMEMAP.put(0, "lili");
        FIRSTNAMEMAP.put(1, "nick");
        FIRSTNAMEMAP.put(2, "ccc");
        FIRSTNAMEMAP.put(3, "tom");
        FIRSTNAMEMAP.put(4, "jetty");

    }

    private static final Map<Integer, String> LASTNAMEMAP = new HashMap<>();

    static {
        LASTNAMEMAP.put(0, "444");
        LASTNAMEMAP.put(1, "5555");
        LASTNAMEMAP.put(2, "6666");
        LASTNAMEMAP.put(3, "7777");
        LASTNAMEMAP.put(4, "je88888tty");
    }

    private static final Map<Integer, String> COMPANYNAMEMAP = new HashMap<>();

    static {
        COMPANYNAMEMAP.put(0, "aaa");
        COMPANYNAMEMAP.put(1, "bbb");
        COMPANYNAMEMAP.put(2, "ccc");
        COMPANYNAMEMAP.put(3, "ddd");
        COMPANYNAMEMAP.put(4, "eee");
    }


    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.spoutOutputCollector = collector;
    }

    @Override
    public void nextTuple() {
        final Random r = new Random();
        int randomNumber = r.nextInt(5);
        spoutOutputCollector.emit(new Values(
                FIRSTNAMEMAP.get(randomNumber),
                LASTNAMEMAP.get(randomNumber),
                COMPANYNAMEMAP.get(randomNumber)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("firstName", "lastName", "companyName"));
    }
}
