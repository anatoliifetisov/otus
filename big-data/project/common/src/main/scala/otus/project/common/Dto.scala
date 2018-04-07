package otus.project.common

object Dto {

  case class PostsRow(id: Long, text: String, lat: Option[Double], lng: Option[Double], tag: String)
  case class SentimentRow(id: Long, text: String, lat: Option[Double], lng: Option[Double], sentiment: Int, tag: String)
  case class SummaryRow(tag: String, positive: Long, neutral: Long, negative: Long, total: Long, cumulative: Long, mean: Double) {

    def +(other: SummaryRow): SummaryRow = {
      assert(tag == other.tag)
      SummaryRow(tag,
        positive + other.positive,
        neutral + other.neutral,
        negative + other.negative,
        total + other.total,
        cumulative + other.cumulative,
        (cumulative + other.cumulative) / (total + other.total).toDouble)
    }
  }
}
