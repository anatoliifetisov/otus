package otus.project

import common.Dto.{PostsRow, SentimentRow, SummaryRow}
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

package object common {

  object constants {
    val NEW_POSTS = "newPosts"
    val ANALYZED_POSTS_UPDATE = "postsUpdate"
    val SUMMARY_UPDATE = "summaryUpdate"
    val SETTINGS_UPDATE = "settingsUpdate"
  }

  object implicits {

    // implicit conversions to JsonValue
    implicit val postsRow2Json: RootJsonFormat[PostsRow] = jsonFormat5(PostsRow)
    implicit val sentimentRow2Json: RootJsonFormat[SentimentRow] = jsonFormat6(SentimentRow)
    implicit val summaryRow2Json: RootJsonFormat[SummaryRow] = jsonFormat7(SummaryRow)

    // implicit conversion from Bool to Long
    implicit def bool2Long(b: Boolean): Long = if (b) 1L else 0L

    // implicit conversion from SentimentRow to SummaryRow
    implicit def sentimentRow2summaryRow(s: SentimentRow): SummaryRow = {
      SummaryRow(s.tag, s.sentiment > 2, s.sentiment == 2, s.sentiment < 2, 1, s.sentiment, s.sentiment)
    }
  }
}
