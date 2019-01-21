package storm;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class HotIPTotalBolt extends BaseRichBolt {

    //定义一个Map集合，保存结果
    private OutputCollector outCollector;
    HashMap<String, Integer> result = new HashMap<>();

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
        String ip = tuple.getStringByField("ip");
        Integer count = tuple.getIntegerByField("count");

        if (result.containsKey(ip)) {
            //如果存在，累加
            int sum = result.get("ip") + count;
            result.put("ip", sum);
        } else {
            //第一次出现
            result.put("ip", count);
        }

        //输出
        System.out.println("Hot IP的结果:" + result);

        this.outCollector.emit(new Values(ip, result.get(ip)));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("ip", "total"));

    }
}
