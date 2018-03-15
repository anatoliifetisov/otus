import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.shared.{HasInputCol, HasOutputCol}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

object StringConcatenationTransformer extends DefaultParamsReadable[StringConcatenationTransformer] {
  override def load(path: String): StringConcatenationTransformer = super.load(path)
}

class StringConcatenationTransformer(override val uid: String) extends Transformer
  with HasInputCol with HasOutputCol with DefaultParamsWritable {

  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def this() = this(Identifiable.randomUID("stringConcatenationTransformer"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val concat = udf { ar: Seq[String] => ar.mkString(" ") }
    dataset.select(col("*"), concat(dataset.col($(inputCol))).as($(outputCol)))
  }

  override def copy(extra: ParamMap): Transformer = defaultCopy(extra)

  override def transformSchema(schema: StructType): StructType = {
    val idx = schema.fieldIndex($(inputCol))
    val field = schema.fields(idx)
    if (field.dataType != ArrayType(StringType)) {
      throw new Exception(s"Input type ${field.dataType} did not match input type ArrayType")
    }
    schema.add(StructField($(outputCol), StringType, nullable = false))
  }
}
