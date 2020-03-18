package bussiness.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by lili on 2017/6/18.
 */
public class BSpout extends BaseRichSpout{
    private SpoutOutputCollector collector;
    private String[] users = {"userA","userB","userC","userD","userE"};
    private String[] pays = {"100","234","465","1111","3455"};
    private int count = 5;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        for (int i = 0; i < count; i++) {
            try {
                Thread.sleep(1500);
                collector.emit("business",new Values(System.currentTimeMillis(),
                        users[i],
                        pays[i]));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("business",new Fields("time","user","pay"));
    }
}
