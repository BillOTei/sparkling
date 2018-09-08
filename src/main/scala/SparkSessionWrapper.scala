import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {

  lazy val conf: Config = ConfigFactory.load()
  lazy val spark: SparkSession = {
    SparkSession
      .builder()
      .master(conf.getString("spark.master"))
      .appName(conf.getString("spark.app-name"))
      .getOrCreate()
  }

}