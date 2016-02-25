package cypher.feature.parser

import cypher.feature.parser.matchers.ValueMatcher
import org.mockito.Mockito._
import org.neo4j.graphdb.{Label, Node}
import org.scalatest.{Matchers, FunSuite}
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.convert.DecorateAsJava

class ParsingTestSupport extends FunSuite with Matchers with DecorateAsJava {

  def node(labels: Seq[String] = Seq.empty, properties: Map[String, AnyRef] = Map.empty): Node = {
    val node = mock(classOf[Node])
    when(node.getLabels).thenReturn(labels.map(Label.label).toIterable.asJava)
    when(node.getAllProperties).thenReturn(properties.asJava)
    node
  }
  case class accept(value: Any) extends Matcher[ValueMatcher] {

    override def apply(matcher: ValueMatcher): MatchResult = {
      MatchResult(matches = matcher.matches(value),
                  s"$matcher did not match $value",
                  s"$matcher unexpectedly matched $value")
    }
  }
}
