import de.l3s.archivespark.enrich.functions.Response
import de.l3s.archivespark.{Implicits, ArchiveSpark}
import de.l3s.archivespark.EnrichableRDD._
import Implicits._
import org.apache.spark.{SparkConf, SparkContext}
import de.l3s.archivespark.JsonConvertibleRDD._

/**
 * Created by holzmann on 04.08.2015.
 */
object Start {
  def main(args: Array[String]): Unit = {
    val appName = "ArchiveSpark"
    val master = "yarn-client"

    val conf = new SparkConf().setAppName(appName).setMaster(master)
    implicit val sc = new SparkContext(conf)

    val rdd = ArchiveSpark.hdfs("/data/ia/derivatives/de/cdx_orig_WILL_BE_REMOVED/TA/TA-100000-000000.arc.cdx", "/data/ia/w/de")
    val filteredRdd = rdd.filter(r => r.surtUrl.startsWith("de,entspannungs-shop"))
    val enriched = filteredRdd.enrich(Response)

    enriched.saveAsJson("out.json")
  }
}
