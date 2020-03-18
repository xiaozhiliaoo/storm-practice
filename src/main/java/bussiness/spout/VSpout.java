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
public class VSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private String[] users = {"userA","userB","userC","userD","userE"};
    private String[] srcid = {"srcA","srcB","srcC","srcD","srcE"};
    private int count = 5;
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        for (int i = 0; i < count; i++) {
            try {
                Thread.sleep(1000);
                collector.emit("visit",new Values(System.currentTimeMillis(),
                        users[i],
                        srcid[i]));
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream("visit",new Fields("time","user","srcid"));
    }
}
