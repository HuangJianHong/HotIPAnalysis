package storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import scala.actors.threadpool.Arrays;

/**
 * 	pom文件中，Storm的版本为1.1.0
 * 	在此pom文件中，已经集成了Storm与Kafka、Redis、JDBC、MySQL的依赖
 */
public class HotIPMain {
    public static void main(String[] args) {
        //创建一个任务：Topology = spout + bolt;

        //spout 从kafka找那个接收数据
        TopologyBuilder builder = new TopologyBuilder();

        //指定任务的spout的组件，接口kafka的数据
        //指定ZK的地址
        String zks = "192.168.18.21:2181";

        //topic的名字
        String topic = "mytopic";
        //Storm在ZK的根目录
        String zkRoot = "/storm";
        String id = "mytopic";  //自己定名字
        //指定Broker地址信息
        BrokerHosts hosts = new ZkHosts(zks);

        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, id);
        /**
         * 指定从kafka中接收的是字符串
         */
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        //zk地址 用数组装载，这边只有一个
        spoutConfig.zkServers = Arrays.asList(new String[]{"192.168.18.21"});
        spoutConfig.zkPort = 2181;

        builder.setSpout("kafka_reader", new KafkaSpout(spoutConfig));
        //指定任务的第一个bolt组件，分词
        //分组策略，随机分组
        builder.setBolt("split_bolt", new HotIPSplitBolt()).shuffleGrouping("kafka_reader");
        //指定任务的第二个bolt组件，计数;
        //分组策略， 按照字段分组
        builder.setBolt("hotip_bolt", new HotIPTotalBolt()).fieldsGrouping("split_bolt", new Fields("ip"));

        //本地运行程序
        Config config = new Config();
        LocalCluster localCluster = new LocalCluster(config);

        localCluster.submitTopology("MyHotIP", config, builder.createTopology());
    }
}
