// Databricks notebook source
import org.apache.hadoop.fs._
import scala.concurrent.{Await, ExecutionContext, Future}
import java.util.concurrent.ForkJoinPool

val numThreads: Int = 10
val readRestTime: Long = 5000L
val sources: Seq[String] = Seq("/somewhere/")
val sink: String = "/nowhere/"
val partitionBy: Seq[String] = Seq()
val writeMode: String = "append"
val totalTime = scala.concurrent.duration.Duration.Inf

// COMMAND ----------

// implicit in "global" scope, bad idea for prod env
implicit val ec: ExecutionContext = ExecutionContext.fromExecutor{
  new ForkJoinPool(
    numThreads,
    ForkJoinPool.defaultForkJoinWorkerThreadFactory,
    new Thread.UncaughtExceptionHandler(){
      override def uncaughtException(th: Thread, t: Throwable): Unit = {
        // TODO something meaningful
        println(th)
        println(t)
        t.printStackTrace()
      }
    },
    true)
}

// COMMAND ----------

def startFileCollection(location: String, partitionBy: Seq[String], writeMode: String): Future[Long] = {
  val path = new Path(location)
  val fs = path.getFileSystem(spark.sessionState.newHadoopConf)
  
  /*
  Get list of file metadata in location.
  For especially large jobs, this can operate over Datasets rather than Collections.
  */
  def fileStatus(in: Seq[Path], acc: Set[FileStatus]): Set[FileStatus] = {
    in match{
      case a +: tail =>
        val (dirs: Seq[FileStatus], files: Seq[FileStatus]) =
          fs.listStatus(a).toSeq.partition{_.isDirectory()}
        fileStatus(tail ++ dirs.map{_.getPath}, acc ++ files)
      case _ => acc
    }
  }
  
  /*
  Helper for the rest period between directory polls
  */
  def readRest[A](a: A): Future[A] = Future{
    Thread.sleep(readRestTime)
    a
  }
  /*
  Does the actual work. This function uses Future as a trampoline, so recursion does not blow the stack.
  */
  def perform(minTs: Long)(implicit __ec__: ExecutionContext): Future[Long] = Future{
    println(location + ": " + minTs)
    val time = System.currentTimeMillis
    val files = fileStatus(Seq(path), Set()).collect{
      case a if a.getModificationTime > minTs =>
        (time, a.getModificationTime, a.getPath.toString, a.getLen)
    }.toList
    
    // if new files found, max new modification time; otherwise maintain
    if(files.nonEmpty){
      val writer = {
        val w = spark.sparkContext.parallelize(files)
          .toDF("ingestTime", "modificationTime", "path", "length")
          .write.format("delta")
        if(partitionBy.nonEmpty) w.partitionBy(partitionBy: _*)
        else w
      }
      
      writer.mode(writeMode)
        .save(sink)
      
      files.map(_._2).max
    } else minTs
  }.flatMap(readRest).flatMap(perform)
  
  //find the intial value of modification times to skip
  val init = // on first execution, no data exists so NPE; wrap in Try to overcome
    scala.util.Try{
      spark.read.format("delta").load(sink)
        //should probably be filtered by location somehow since that is the quantum of execution
        //.filter("something = '" + something + "'")
        .select(org.apache.spark.sql.functions.max($"modificationTime"))
        .collect()(0).getLong(0)
    }.toOption.getOrElse(0L)
  
  perform(init)
}

// COMMAND ----------

// For especially large jobs, this can operate over a dataset rather than a Seq[Future] 
val th = Future.sequence(
  sources.map(startFileCollection(_, partitionBy, writeMode))
)

// COMMAND ----------

Await.result(th, totalTime)
