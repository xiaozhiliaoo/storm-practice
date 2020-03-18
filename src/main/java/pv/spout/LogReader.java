package pv.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;

/**
 * Created by lili on 2017/6/18.
 */
public class LogReader extends BaseRichSpout {

    private SpoutOutputCollector _collector;
    private Random _rand = new Random();
    private int _count = 100;
    private String[] _users = {"userA","userB","userC","userD","userE"};
    private String[] _urls = {"urlA","urlB","urlC","urlD","urlE"};
    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this._collector = spoutOutputCollector;
    }

    @Override
    public void nextTuple() {
        try {
            Thread.sleep(1000);
            while(_count-- > 0){
                _collector.emit(new Values(
                        System.currentTimeMillis(),
                        _users[_rand.nextInt(5)],
                        _urls[_rand.nextInt(5)]));
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("time","user","url"));
    }
}