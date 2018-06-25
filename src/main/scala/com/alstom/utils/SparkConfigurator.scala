package com.alstom.utils
import com.alstom.Launcher.Config.ConfigConstants._
import com.alstom.Launcher.GTFSCleanerApp.config
import com.alstom.utils.ConfigUtils.SparkAppName
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

object SparkConfigurator extends LazyLogging with ConfigUtils{
  
  /**
   * Return spark session object
   * 
   * NOTE Add .master("local") to enable debug via IntelliJ or add as a VM option at runtime
   * -Dspark.master="local[*]"
   */

    private lazy val spark = SparkSession.builder
      .appName("SparkTest")
      .master("local[3]")
      //.enableHiveSupport()
      .getOrCreate()

  type EnvironmentName = String
  def getSparkSessionForEnv(name: EnvironmentName): SparkSession = {
    val spark = SparkSession.builder.appName(config.getString(SparkAppName))
      .master("local[3]")
      //.enableHiveSupport()
      .getOrCreate()
    //spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
      spark.sparkContext.setCheckpointDir("./tmp/")
    spark
  }

  def configureSpark: SparkSession = {
    spark.conf.set("hadoop.security.authentication", HadoopSecurityAuthentication)
    spark.conf.set("hadoop.security.authorization", HadoopSecurityAuthorization)
    spark.conf.set("dfs.permissions", DfsPermissions)
    spark.conf.set("spark.sql.shuffle.partitions", SparkSqlShufflePartitions)
    spark.conf.set("spark.executor.memory", SparkExecutorMemory)
    spark
 }

  lazy val sparkSession = configureSpark


  def configureSpark(spark: SparkSession): SparkSession = {
    spark.conf.set("hadoop.security.authentication", HadoopSecurityAuthentication)
    spark.conf.set("hadoop.security.authorization", HadoopSecurityAuthorization)
    spark.conf.set("dfs.permissions", DfsPermissions)
    spark.conf.set("spark.sql.shuffle.partitions", SparkSqlShufflePartitions)
    //spark.conf.set("spark.executor.memory", SparkExecutorMemory)
    spark
  }


  /**
  * Return some information on the environment we are running in.
  */
  def versionInfo: Seq[String] = {
    val sc = sparkSession.sparkContext
    val scalaVersion = scala.util.Properties.scalaPropOrElse("version.number", "unknown")
    val sparkVersion = sc.version
    val sparkMaster  = sc.master
    val local        = sc.isLocal
    val defaultPar   = sc.defaultParallelism

    val versionInfo = s"""
        |---------------------------------------------------------------------------------
        | Spark version: $sparkVersion
        | Scala version: $scalaVersion
        | Spark master : $sparkMaster
        | Spark running locally? $local
        | Default parallelism: $defaultPar
        |---------------------------------------------------------------------------------
        |""".stripMargin

    versionInfo.split("\n")
  }

  /*
	* Dump spark configuration for the current spark session.
	*/
  def getAllConf: String = {
      sparkSession.conf.getAll.map { case(k,v) => "Key: [%s] Value: [%s]" format (k,v)} mkString("","\n","\n")
  }
      
}
