package analysis

import analysis.Preparation._
import org.apache.spark.sql.DataFrame

object Analysis {
  def analyse(src: DataFrame): Unit = {
    val model = stringIndexerData(src)
    model.transform(src).show()
  }
}
