/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import cypher.feature.parser.matchers.{ResultMatcher, RowMatcher, ValueMatcher}
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.graphdb._
import org.scalatest.{Matchers, FunSuite}
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.convert.DecorateAsJava

class ParsingTestSupport extends FunSuite with Matchers with DecorateAsJava {

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

  case class accept(value: Any) extends Matcher[ValueMatcher] {

    override def apply(matcher: ValueMatcher): MatchResult = {
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
}
