/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.feature.parser

import cypher.feature.parser.matchers.{QueryStatisticsMatcher, ResultMatcher, RowMatcher, ValueMatcher}
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.graphdb._
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.convert.DecorateAsJava

@RunWith(classOf[JUnitRunner])
abstract class ParsingTestSupport extends FunSuite with Matchers with DecorateAsJava with Accepters {

  def node(labels: Seq[String] = Seq.empty, properties: Map[String, AnyRef] = Map.empty): Node = {
    val node = mock(classOf[Node])
    when(node.getLabels).thenReturn(labels.map(Label.label).toIterable.asJava)
    when(node.getAllProperties).thenReturn(properties.asJava)
    when(node.toString).thenReturn(s"(${labels.mkString(":", ":", "")} $properties)")
    node
  }

  def relationship(typ: String, properties: Map[String, AnyRef] = Map.empty): Relationship = {
    val rel = mock(classOf[Relationship])
    when(rel.getType).thenReturn(RelationshipType.withName(typ))
    when(rel.getAllProperties).thenReturn(properties.asJava)
    when(rel.toString).thenReturn(s"[:$typ $properties]")
    rel
  }

  def singleNodePath(node: Node): Path = {
    val path = mock(classOf[Path])
    when(path.startNode()).thenReturn(node)
    when(path.endNode()).thenReturn(node)
    when(path.length()).thenReturn(0)
    // Mockito bug makes mocks unable to refer to other mocks in stubs; can't inline this
    val pathString = s"<$node>"
    when(path.toString).thenReturn(pathString)
    path
  }

  def path(relationships: Relationship*): Path = {
    val path = mock(classOf[Path])
    when(path.length()).thenReturn(relationships.length)
    when(path.relationships()).thenReturn(relationships.toIterable.asJava)
    // Mockito bug makes mocks unable to refer to other mocks in stubs; can't inline this
    val pathString = s"<${relationships.mkString(",")}>"
    when(path.toString).thenReturn(pathString)
    path
  }

  def pathLink(startNode: Node, relationship: Relationship, endNode: Node): Relationship = {
    when(relationship.getStartNode).thenReturn(startNode)
    when(relationship.getEndNode).thenReturn(endNode)
    // Mockito bug makes mocks unable to refer to other mocks in stubs; can't inline this
    val pathString = s"$startNode-$relationship->$endNode"
    when(relationship.toString).thenReturn(pathString)
    relationship
  }

  def result(maps: Map[String, AnyRef]*): Result = {
    val result = mock(classOf[Result])
    val itr = maps.map(_.asJava).iterator
    when(result.hasNext).thenAnswer(new Answer[Boolean] {
      override def answer(invocation: InvocationOnMock): Boolean = itr.hasNext
    })
    when(result.next).thenAnswer(new Answer[java.util.Map[String, AnyRef]] {
      override def answer(invocation: InvocationOnMock) = itr.next
    })
    when(result.toString).thenReturn(s"Result:\n${maps.mkString("\n")}")
    result
  }

}

trait Accepters extends DecorateAsJava {

  case class accept(value: Any) extends Matcher[ValueMatcher] {

    override def apply(matcher: ValueMatcher): MatchResult = {
      MatchResult(matches = matcher.matches(value),
                  s"$matcher did not match $value",
                  s"$matcher unexpectedly matched $value")
    }
  }

  case class acceptStatistics(value: QueryStatistics) extends Matcher[QueryStatisticsMatcher] {

    override def apply(matcher: QueryStatisticsMatcher): MatchResult = {
      MatchResult(matches = matcher.matches(value),
                  s"$matcher did not match $value",
                  s"$matcher unexpectedly matched $value")
    }
  }

  case class acceptRow(value: Map[String, AnyRef]) extends Matcher[RowMatcher] {

    override def apply(matcher: RowMatcher): MatchResult = {
      MatchResult(matches = matcher.matches(value.asJava),
                  s"$matcher did not match $value",
                  s"$matcher unexpectedly matched $value")
    }
  }

  case class acceptResult(value: Result) extends Matcher[ResultMatcher] {

    override def apply(matcher: ResultMatcher): MatchResult = {
      MatchResult(matches = matcher.matches(value),
                  s"$matcher did not match $value",
                  s"$matcher unexpectedly matched $value")
    }
  }

  case class acceptOrderedResult(value: Result) extends Matcher[ResultMatcher] {

    override def apply(matcher: ResultMatcher): MatchResult = {
      MatchResult(matches = matcher.matchesOrdered(value),
                  s"$matcher did not match $value",
                  s"$matcher unexpectedly matched $value")
    }
  }
}
