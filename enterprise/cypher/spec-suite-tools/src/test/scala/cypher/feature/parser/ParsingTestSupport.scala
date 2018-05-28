/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package cypher.feature.parser

import java.util.Collections

import cypher.feature.parser.matchers.ResultMatcher
import org.junit.runner.RunWith
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.javacompat.MapRow
import org.neo4j.graphdb.Result.ResultVisitor
import org.neo4j.graphdb._
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{FunSuite, Matchers}

import scala.collection.convert.DecorateAsJava
import scala.runtime.ScalaNumberProxy

@RunWith(classOf[JUnitRunner])
abstract class ParsingTestSupport extends FunSuite with Matchers with DecorateAsJava with MatcherMatchingSupport {

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
    when(path.relationships).thenReturn(Collections.emptyList[Relationship]())
    path
  }

  def path(relationships: Relationship*): Path = {
    val path = mock(classOf[Path])
    when(path.length()).thenReturn(relationships.length)
    when(path.relationships()).thenReturn(relationships.toIterable.asJava)
    val startNode: Node = relationships.head.getStartNode
    when(path.startNode()).thenReturn(startNode)
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
    when(relationship.getOtherNode(startNode)).thenReturn(endNode)
    when(relationship.getOtherNode(endNode)).thenReturn(startNode)
    relationship
  }

  def result(maps: Map[String, AnyRef]*): Result = {
    val result = mock(classOf[Result])
    val cols = if (maps.isEmpty) Seq.empty[String] else maps.head.keys
    maps.foreach(_.keys should equal(cols))

    val itr = maps.map(_.asJava).iterator
    when(result.hasNext).thenAnswer(new Answer[Boolean] {
      override def answer(invocation: InvocationOnMock): Boolean = itr.hasNext
    })
    when(result.next).thenAnswer(new Answer[java.util.Map[String, AnyRef]] {
      override def answer(invocation: InvocationOnMock) = itr.next
    })
    when(result.toString).thenReturn(s"Result:\n${maps.mkString("\n")}")

    when(result.columns()).thenReturn(cols.toList.asJava)

    when(result.accept(org.mockito.Matchers.any())).thenAnswer(new Answer[Unit] {
      override def answer(invocationOnMock: InvocationOnMock): Unit = {
        val visitor = invocationOnMock.getArgumentAt(0, classOf[ResultVisitor[_]])
        var continue = true
        while (continue && itr.hasNext) {
          val row = new MapRow(itr.next())
          continue = visitor.visit(row)
        }
      }
    })
    result
  }

}

trait MatcherMatchingSupport {

  class Acceptor[T <: AnyRef](value: T) extends Matcher[cypher.feature.parser.matchers.Matcher[_ >: T]] {
    override def apply(matcher: cypher.feature.parser.matchers.Matcher[_ >: T]): MatchResult = {
      MatchResult(matches = matcher.matches(value),
        s"$matcher did not match $value",
        s"$matcher unexpectedly matched $value")
    }
  }

  def accept[T <: AnyRef](value: T) = new Acceptor[T](value)
  def accept[T <: AnyVal](value: ScalaNumberProxy[T]) = new Acceptor(value.underlying())
  def accept(value: Boolean) = new Acceptor[java.lang.Boolean](value)
  def accept(value: Null) = new Acceptor[Null](value)

  case class acceptOrdered(value: Result) extends Matcher[ResultMatcher] {
    override def apply(matcher: ResultMatcher): MatchResult = {
      MatchResult(matches = matcher.matchesOrdered(value),
                  s"$matcher did not match ordered $value",
                  s"$matcher unexpectedly matched ordered $value")
    }
  }
}
