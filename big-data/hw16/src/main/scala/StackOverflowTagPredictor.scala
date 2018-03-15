import com.johnsnowlabs.nlp.annotators.{Normalizer, Stemmer, Tokenizer}
import com.johnsnowlabs.nlp.{DocumentAssembler, Finisher}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.runtime.universe._

object StackOverflowTagPredictor {

  case class RawRow(id: Long, title: String, body: String, tags: String)
  case class SplittedTagsRow(id: Long, title: String, body: String, tags: Set[String])
  case class EncodedTagRow(id: Long, label: Int)
  case class EncodedTitleRow(id: Long, titleFeatures: Vector)
  case class EncodedBodyRow(id: Long, bodyFeatures: Vector)
  case class EncodedRow(id: Long, features: Vector, label: Int)

  def main(args: Array[String]): Unit = {

    val inputFile = args(0)

    val spark = SparkSession.builder
      .appName("StackOverflowTagPredictor")
      .getOrCreate()

    import spark.sqlContext.implicits._

    val languages = Set("javascript", "java", "python", "ruby", "php", "c++", "c#", "go", "scala", "swift")
    val languagesSet = spark.sparkContext.broadcast(languages)
    val languagesMap = spark.sparkContext.broadcast(languages.zipWithIndex.toMap)

    val posts = spark.read
      .format("csv")
      .option("header", "true")
      .option("escape", "\"")
      .option("multiLine", "true")
      .option("inferSchema", "true")
      .load(inputFile)
      .toDF("id", "title", "body", "tags")
      .as[RawRow]

    val postsWithSplittedTags = posts.map(x => SplittedTagsRow(x.id, x.title, x.body, x.tags.split(' ').toSet))
    val postsWithLanguagesInTags = postsWithSplittedTags.map(x => x.copy(tags = languagesSet.value.intersect(x.tags)))
    val postsWithSingleTag = postsWithLanguagesInTags.filter(_.tags.size == 1)
      .cache()
    val transformedTags = dropRedundantColumns[EncodedTagRow](postsWithSingleTag.map(x =>
        EncodedTagRow(x.id, languagesMap.value(x.tags.head)))
      .toDF)
      .as[EncodedTagRow]
      .alias("tags") // to prevent any ambiguity during join
      .cache()

    val titlePipeline = createPipeline("title", "titleFeatures")
    val bodyPipeline = createPipeline("body", "bodyFeatures", useW2v = false) // w2v for the body takes forever

    val assembler = new VectorAssembler()
      .setInputCols(Array("titleFeatures", "bodyFeatures"))
      .setOutputCol("features")

    val titleModel = titlePipeline.fit(postsWithSingleTag)
    val bodyModel = bodyPipeline.fit(postsWithSingleTag)

    val transformedTitle = dropRedundantColumns[EncodedTitleRow](titleModel.transform(postsWithSingleTag))
      .as[EncodedTitleRow]
      .cache()
      .alias("title")
    val transformedBody = dropRedundantColumns[EncodedBodyRow](bodyModel.transform(postsWithSingleTag))
      .as[EncodedBodyRow]
      .cache()
      .alias("body")

    val transformedPosts = dropRedundantColumns[EncodedRow](assembler.transform(transformedTitle
          .join(transformedBody, "id")
          .join(transformedTags, "id")))
        .as[EncodedRow]
        .cache()

    val Array(trainData, testData) = transformedPosts.randomSplit(Array(0.7, 0.3), seed = 0)
    val (trainCached, testCached) = (trainData.cache(), testData.cache())

    val lr = new LogisticRegression()
    val evaluator = new MulticlassClassificationEvaluator().setMetricName("accuracy")

    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.maxIter, Array(100))
      .addGrid(lr.regParam, Array(0.0, 0.5, 1.0))
      .addGrid(lr.elasticNetParam, Array(0.0, 0.5, 1.0))
      .addGrid(lr.tol, Array(0.01))
      .build()

    val cv = new CrossValidator()
      .setEstimator(lr)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)
      .setParallelism(8)
      .setSeed(0)

    val cvModel = cv.fit(trainCached)

    println("Cross-validated params:")
    cvModel.bestModel.extractParamMap().toSeq.foreach(x => println(s"${x.param.name}: ${x.value} (${x.param.doc})"))

    val prediction = cvModel.transform(testCached)

    println(s"Test set accuracy = ${evaluator.evaluate(prediction)}")

    spark.stop()
  }

  private def createPipeline(inColName: String, outColName: String, useW2v: Boolean = true): Pipeline = {

    val htmlCleaner = new HtmlCleanerTransformer()
      .setInputCol(inColName)
      .setOutputCol("raw_" + inColName)

    val firstDocumentAssembler = new DocumentAssembler()
      .setInputCol(htmlCleaner.getOutputCol)
      .setOutputCol("document1_" + inColName)

    val firstTokenizer = new Tokenizer()
      .setInputCols(firstDocumentAssembler.getOutputCol)
      .setOutputCol("tokens1_" + inColName)

    val normalizer = new Normalizer()
      .setInputCols(Array(firstTokenizer.getOutputCol))
      .setOutputCol("normalized_" + inColName)

    val firstFinisher = new Finisher()
      .setInputCols(normalizer.getOutputCol)
      .setOutputCols("out1_" + inColName)
      .setOutputAsArray(true)

    val firstStopWordsRemover = new StopWordsRemover()
      .setInputCol(firstFinisher.getOutputCols(0))
      .setOutputCol("stopped1_" + inColName)

    val concatenator = new StringConcatenationTransformer()
      .setInputCol(firstStopWordsRemover.getOutputCol)
      .setOutputCol("restored_" + inColName)

    val secondDocumentAssembler = new DocumentAssembler()
      .setInputCol(concatenator.getOutputCol)
      .setOutputCol("document2_" + inColName)

    val secondTokenizer = new Tokenizer()
      .setInputCols(secondDocumentAssembler.getOutputCol)
      .setOutputCol("tokens2_" + inColName)

    val stemmer = new Stemmer()
      .setInputCols(secondTokenizer.getOutputCol)
      .setOutputCol("stem_" + inColName)

    val secondFinisher = new Finisher()
      .setInputCols(stemmer.getOutputCol)
      .setOutputCols("out2_" + inColName)
      .setOutputAsArray(true)

    val secondStopWordsRemover = new StopWordsRemover()
      .setInputCol(secondFinisher.getOutputCols(0))
      .setOutputCol("stopped2_" + inColName)

    val word2Vec = new Word2Vec()
      .setInputCol(secondStopWordsRemover.getOutputCol)
      .setOutputCol("w2v_" + inColName)
      .setVectorSize(500)
      .setMinCount(10)

    val countVectorizer = new CountVectorizer()
      .setInputCol(secondStopWordsRemover.getOutputCol)
      .setOutputCol("counted_" + inColName)
      .setVocabSize(10000)
      .setMinTF(0.05)
      .setMinDF(0.05)

    val idf = new IDF()
      .setInputCol(countVectorizer.getOutputCol)
      .setOutputCol(if (useW2v) "idf_" + inColName else outColName)

    val assembler = new VectorAssembler()
      .setInputCols(Array(word2Vec.getOutputCol, idf.getOutputCol))
      .setOutputCol(outColName)

    val stages = Array(htmlCleaner, firstDocumentAssembler, firstTokenizer, normalizer, firstFinisher,
      firstStopWordsRemover, concatenator, secondDocumentAssembler, secondTokenizer, stemmer, secondFinisher,
      secondStopWordsRemover, countVectorizer, idf) ++ (if (useW2v) Array(word2Vec, assembler) else Nil)

    val pipeline = new Pipeline().setStages(stages)

    pipeline
  }

  private def dropRedundantColumns[T: TypeTag](df: DataFrame): DataFrame = {

    val caseClassFieldsNames = typeOf[T].members.collect {
      case m: MethodSymbol if m.isCaseAccessor => m.name.toString
    }.toSet

    val columns = df.columns.toSet
    val columnsToDrop = columns &~ caseClassFieldsNames // columns except case class' fields

    def dropColumns(df: DataFrame, columns: Traversable[String]): DataFrame = columns match {
      case Nil => df
      case x :: xs => dropColumns(df.drop(x), xs)
    }

    dropColumns(df, columnsToDrop.toList)
  }
}
