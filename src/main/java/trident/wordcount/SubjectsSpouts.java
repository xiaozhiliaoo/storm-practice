package trident.wordcount;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.spout.IBatchSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by lili on 2017/6/18.
 */
public class SubjectsSpouts implements IBatchSpout {
    public static final long serialVersionUID = 1L;
    private int batchSize;
    private HashMap<Long, List<List<Object>>> batchMap = new HashMap<>();
    private static final Map<Integer, String> DATA_MAP = new HashMap<>();

    public SubjectsSpouts(int batchSize) {
        this.batchSize = batchSize;
    }

    static {
        DATA_MAP.put(0, "java java php ruby c++");
        DATA_MAP.put(1, "python python python java c++");
        DATA_MAP.put(2, "php php php java ruby");
        DATA_MAP.put(3, "ruby python python python php");
    }


    @Override
    public void open(Map map, TopologyContext topologyContext) {

    }

    @Override
    public void emitBatch(long batchId, TridentCollector tridentCollector) {
        List<List<Object>> batches = new ArrayList<>();
        for (int i = 0; i < this.batchSize; i++) {
            batches.add(new Values(DATA_MAP.get(i)));
        }
        System.out.println("batchId: " + batchId);
        this.batchMap.put(batchId, batches);
        for (List<Object> list : batches) {
            tridentCollector.emit(list);
        }
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {

            e.printStackTrace();
        }
    }

    @Override
    public void ack(long batchId) {
        System.out.println("remove batchId: " + batchId);
        this.batchMap.remove(batchId);
    }

    @Override
    public void close() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("subjects");
    }


}
