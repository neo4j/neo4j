package cypher.feature.parser

import java.lang.Iterable
import java.util
import org.neo4j.graphdb._

import scala.collection.JavaConverters._

trait ParsedEntities {

  def parsedNode(labels: Seq[String] = Seq.empty, properties: Map[String, AnyRef] = Map.empty): Node = {
    new EmptyNode() {
      override def getLabels: Iterable[Label] = {
        val seq: Seq[Label] = labels.map(labelName => new Label {
          override def name(): String = labelName
        })
        seq.toIterable.asJava
      }

      override def getAllProperties: util.Map[String, AnyRef] = properties.asJava
    }
  }

}
