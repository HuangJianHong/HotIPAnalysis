import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.kafka.KafkaUtils


//使用Spark SQL分析流式数据
//日志数据: 1,201.105.101.102,http://mystore.jsp/?productid=1,2017020029,2,1
case class LogInfo(user_id: String, user_ip: String, url: String, click_time: String, action_type: String, area_id: String)

object HotIPScala {

  def main(args: Array[String]): Unit = {
    //Fixme 去除掉resources目录下的 log4j.properties文件，不然日志过多影响Spark信息查看
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    //创建一个 SteamingContext
    val conf: SparkConf = new SparkConf().setAppName("HotIP").setMaster("local[2]")
    val ssc = new StreamingContext(conf, Seconds(10))

    //由于使用Spark SQL分析数据， 创建SQL Context对象
    val sqlContext = new SQLContext(ssc.sparkContext)
    //导入隐式 转换包
    import sqlContext.implicits._

    //指定kafka的Topic,表示 从这个topic中， 每次接受一条数据
    val topic = Map("mytopic" -> 1)

    //创建DStream接受数据: 低版本kafka不能直接读取，用Receiver方式
    //一个组用户，只有收到一次消息
    val kafkaStream: ReceiverInputDStream[(String, String)] = KafkaUtils
      .createStream(ssc, "192.168.18.21:2181", "mygroup", topic)
    //从kafka中，接收到的数据是<key value> key ---> null空值
    val logRDD: DStream[String] = kafkaStream.map(_._2)

    //日志：1,201.105.101.102,http://mystore.jsp/?productid=1,2017020029,2,1
    logRDD.foreachRDD((rdd: RDD[String]) => {
      //生成DataFrame
      val result: DataFrame = rdd.map(_.split(",")).map(x => new LogInfo(x(0), x(1), x(2), x(3), x(4), x(5))).toDF()

      //生成视图
      result.createOrReplaceTempView("clicklog")

      //执行sql
      sqlContext.sql("select user_ip as IP, count(user_ip) as PV from clicklog group by user_ip").show()

      ssc.start()
      ssc.awaitTermination()
    })


  }


}
