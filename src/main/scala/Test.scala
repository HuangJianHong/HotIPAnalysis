import org.apache.log4j.{Level, Logger}

object Test {
  def main(args: Array[String]): Unit = {
    //Fixme 去除掉resources目录下的 log4j.properties文件，不然日志过多影响Spark信息查看

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.OFF)

    
    System.out.println("----SparkStream-----")
  }

}
