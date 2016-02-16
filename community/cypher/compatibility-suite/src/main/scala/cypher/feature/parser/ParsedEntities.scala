package cypher.feature.parser

import java.lang.Iterable
import java.util
import java.util.Collections.emptyList
import java.util.function.Consumer

import org.neo4j.graphdb._

trait ParsedEntities {

  def parsedNode(labelNames: Iterable[String] = emptyList(), properties: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()): Node = {
    new ParsedNode() {
      override def getLabels: Iterable[Label] = {
        val list = new util.ArrayList[Label]()
        labelNames.forEach(new Consumer[String] {
          override def accept(t: String): Unit = list.add(Label.label(t))
        })
        list
      }

      override def getAllProperties: util.Map[String, AnyRef] = properties
    }
  }

}
