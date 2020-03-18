package drpc.drpc1;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;
import org.apache.thrift7.TException;

/**
 * Created by lili on 2017/6/17.
 */
public class DRPCExclam {
    public static void main(String[] args) throws TException, DRPCExecutionException {
        DRPCClient client = new DRPCClient("192.168.31.121",3772);
        for(String word:new String[]{"hello","goodbye"}){
            System.out.println(client.execute("exclamation",word));
        }
    }
}
