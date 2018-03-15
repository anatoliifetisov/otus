import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

object HtmlCleanerTransformer extends DefaultParamsReadable[HtmlCleanerTransformer] {
  override def load(path: String): HtmlCleanerTransformer = super.load(path)
}

class HtmlCleanerTransformer(override val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with DefaultParamsWritable {

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def this() = this(Identifiable.randomUID("htmlCleanerTransformer"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val cleanTags = udf { s: String => TagSoupXmlLoader.get().loadString(s).text }
    dataset.select(col("*"), cleanTags(dataset.col($(inputCol))).as($(outputCol)))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val idx = schema.fieldIndex($(inputCol))
    val field = schema.fields(idx)
    if (field.dataType != StringType) {
      throw new Exception(s"Input type ${field.dataType} did not match input type StringType")
    }
    schema.add(StructField($(outputCol), StringType, nullable = false))
  }
}
