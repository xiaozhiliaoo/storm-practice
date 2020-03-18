package trident.wordcount;

import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

/**
 * Created by lili on 2017/6/17.
 */
public class ResultFunction extends BaseFunction {
    private static final long serialVersionUID = 1L;

    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String sub = tridentTuple.getStringByField("sub");
        Long count = tridentTuple.getLongByField("count");
        System.out.println(sub+" 出现了: ["+count+"]次");
    }
}
