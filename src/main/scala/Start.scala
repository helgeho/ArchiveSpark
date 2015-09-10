import de.l3s.archivespark.ArchiveSpark
import de.l3s.archivespark.EnrichableRDD._
import de.l3s.archivespark.enrich.Response
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by holzmann on 04.08.2015.
 */
object Start {
  def main(args: Array[String]): Unit = {
    val appName = "ArchiveSpark"
    val master = "yarn-client"

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    implicit val sc = new SparkContext(conf)

    val rdd = ArchiveSpark.hdfs("/data/ia/derivatives/de/cdx/*/*.cdx", "/data/ia/w/de")
    val filteredRdd = rdd.filter(r => r.get.originalUrl == "...")
    filteredRdd.enrich(Response)

    filteredRdd.saveAsTextFile("out.json")
  }
}
