import org.ccil.cowan.tagsoup.jaxp.SAXFactoryImpl

import scala.xml.factory.XMLLoader
import scala.xml.{Elem, XML}

object TagSoupXmlLoader {

  private val factory = new SAXFactoryImpl()

  def get(): XMLLoader[Elem] = {
    XML.withSAXParser(factory.newSAXParser())
  }
}