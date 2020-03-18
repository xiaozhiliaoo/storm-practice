package kafka.example;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by lili on 2017/6/18.
 */
public class KafkaConsumer {
    private static final String topic = "test1";

    public static void main(String[] args) {
        Properties props = new Properties();
        // 设置zookeeper的链接地址
        props.put("zookeeper.connect","192.168.31.121:2181,192.168.31.122:2181,192.168.31.123:2181");
        // 设置group id
        props.put("group.id", "group1");

        // kafka的group 消费记录是保存在zookeeper上的, 但这个信息在zookeeper上不是实时更新的, 需要有个间隔时间更新
        props.put("auto.commit.interval.ms", "1000");
        props.put("zookeeper.session.timeout.ms","10000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.offset.reset", "smallest");
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ConsumerConfig config = new ConsumerConfig(props);

        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);

        Map<String,Integer> topicCountMap = new HashMap<>();

        topicCountMap.put(topic,new Integer(1));

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap =
                consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get(topic).get(0);
        ConsumerIterator<String, String> it = stream.iterator();
        while (it.hasNext()) {
            System.out.println(it.next().message());
        }
    }
}
