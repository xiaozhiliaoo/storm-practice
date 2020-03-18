package trident.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.io.FileWriter;

/**
 * Created by lili on 2017/6/17.
 */
public class WriteFunction extends BaseFunction{

    private static final long serialVersionUID = 1L;

    private FileWriter writer;

    public static final Logger log = LoggerFactory.getLogger(WriteFunction.class);
    @Override
    public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
        String text = tridentTuple.getStringByField("sub");

        try {
            if(writer == null){
                if(System.getProperty("os.name").equals("Windows 10")){
                    writer = new FileWriter("D:\\099_test\\" + this);
                } else if(System.getProperty("os.name").equals("Windows 8.1")){
                    writer = new FileWriter("D:\\099_test\\" + this);
                } else if(System.getProperty("os.name").equals("Windows 7")){
                    writer = new FileWriter("D:\\099_test\\" + this);
                } else if(System.getProperty("os.name").equals("Linux")){
                    System.out.println("----:" + System.getProperty("os.name"));
                    writer = new FileWriter("/usr/local/temp/" + this);
                }else{
                    System.out.println("----:" + System.getProperty("os.name"));
                    writer = new FileWriter("/usr/local/temp/" + this);
                }
            }
            log.info("【write】： 写入文件");
            log.debug(text);
            writer.write(text);
            writer.write("\n");
            writer.flush();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
