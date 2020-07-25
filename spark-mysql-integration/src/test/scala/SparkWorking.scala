import org.apache.spark.{SparkConf, SparkContext}

object SparkWorking {
  def main(args : Array[String]):Unit={
      // Creating Spark Config
       val sparkConfig = new SparkConf()
         .setAppName("SparkWorking")
         .setMaster("local")
      // Creating Spark Context with spark config
     val sparkContext = new SparkContext(sparkConfig)
      // Checking AppName
     println(sparkContext.appName)

  }
}
