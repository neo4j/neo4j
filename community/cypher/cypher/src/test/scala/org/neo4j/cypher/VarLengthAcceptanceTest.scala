/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher

import org.neo4j.cypher.internal.compiler.v2_3.commands.NoneInCollection
import org.neo4j.cypher.internal.compiler.v2_3.executionplan.InternalExecutionResult
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.LegacyExpression
import org.neo4j.graphdb.{Direction, Node}
import org.neo4j.graphdb.Direction._
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.mutable

class VarLengthAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with NewPlannerTestSupport {

  test("should handle LIKES*") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(generation(2) ++ generation(3) ++ generation(4))
  }

  test("should handle LIKES*0") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*0]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(Set(nodes("n0")))
  }

  test("should handle LIKES*0..2") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*0..2]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(generation(1) ++ generation(2) ++ generation(3))
  }

  test("should handle LIKES*..") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*..]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(generation(2) ++ generation(3) ++ generation(4))
  }

  test("should not accept LIKES..") {
    //Given
    makeTreeModel(maxNodeDepth = 4)
    //Then
    a[SyntaxException] should be thrownBy {
      executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES..]->(c) RETURN c")
    }
  }

  test("should handle LIKES*1") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*1]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(generation(2))
  }

  test("should handle LIKES*1..2") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*1..2]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(generation(2) ++ generation(3))
  }

  test("should handle LIKES*0..0") {
    //Given
    val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*0..0]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(Set(nodes("n0")))
  }

  test("should handle LIKES*1..1") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*1..1]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(generation(2))
  }

  test("should handle LIKES*2..2") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*2..2]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(generation(3))
  }

  test("should not return results for LIKES*2..1") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*2..1]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(empty)
  }

  test("should handle LIKES*2") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners( "MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*2]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(generation(3))
  }

  test("should not accept LIKES*-2") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //Then
    a [SyntaxException] should be thrownBy {
      executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*-2]->(c) RETURN c")
    }
  }

  test("should handle LIKES*..0") {
    //Given
    makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners( "MATCH (p { id:'A' }) MATCH (p)-[:LIKES*..0]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(empty)
  }

  test("should handle LIKES*1..0") {
    //Given
    makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners( "MATCH (p { id:'A' }) MATCH (p)-[:LIKES*1..0]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(empty)
  }

  test("should handle LIKES*..1") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners( "MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*..1]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(generation(2))
  }

  test("should handle LIKES*..2") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners( "MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*..2]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(generation(2) ++ generation(3))
  }

  test("should handle LIKES*0..") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners( "MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*0..]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(generation(1) ++ generation(2) ++ generation(3) ++ generation(4))
  }

  test("should handle LIKES*1..") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners( "MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*1..]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(generation(2) ++ generation(3) ++ generation(4))

  }

  test("should handle LIKES*2..") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners( "MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*2..]->(c) RETURN c")
    //Then
    result.columnAs("c").toSet should be(generation(3) ++ generation(4))

  }

  test("should handle LIKES*0.LIKES") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*0]->()-[r:LIKES]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
    result.columnAs("c").toSet should be(generation(2))
  }

  test("should handle LIKES.LIKES*0") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES]->()-[r:LIKES*0]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
    result.columnAs("c").toSet should be(generation(2))
  }

  test("should handle LIKES*1.LIKES") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*1]->()-[r:LIKES]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
    result.columnAs("c").toSet should be(generation(3))
  }

  test("should handle LIKES.LIKES*1") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES]->()-[r:LIKES*1]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
    result.columnAs("c").toSet should be(generation(3))
  }

  test("should handle LIKES*2.LIKES") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*2]->()-[r:LIKES]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
    result.columnAs("c").toSet should be(generation(4))
  }

  test("should handle LIKES.LIKES*2") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES]->()-[r:LIKES*2]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
    result.columnAs("c").toSet should be(generation(4))
  }

  test("should handle LIKES.LIKES*3") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 5)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES]->()-[r:LIKES*3]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
    result.columnAs("c").toSet should be(generation(5))
  }

  test("should handle <-[:LIKES]-()-[r:LIKES*3]->") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 5, directions = Seq(INCOMING))
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)<-[:LIKES]-()-[r:LIKES*3]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
    result.columnAs("c").toSet should be(generation(5))
  }

  test("should handle -[:LIKES]->()<-[r:LIKES*3]-") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 5, directions = Seq(OUTGOING, INCOMING, INCOMING, INCOMING))
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES]->()<-[r:LIKES*3]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
    result.columnAs("c").toSet should be(generation(5))
  }

  test("should handle LIKES*1.LIKES.LIKES*2") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 5)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*1]->()-[:LIKES]->()-[r:LIKES*2]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
    result.columnAs("c").toSet should be(generation(5))
  }

  test("should handle LIKES.LIKES*2.LIKES") {
    //Given
    implicit val nodes = makeTreeModel(maxNodeDepth = 5)
    //When
    val result = executeWithAllPlanners("MATCH (p { id:'n0' }) MATCH (p)-[:LIKES]->()-[:LIKES*2]->()-[r:LIKES]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
    result.columnAs("c").toSet should be(generation(5))
  }

  test("should use variable of already matched rel in a varlenght path") {
    eengine.execute("""create
                      |(_0:`Node` ),
                      |(_1:`Node` ),
                      |(_2:`Node` ),
                      |(_3:`Node` ),
                      |_0-[:EDGE]->_1,
                      |_1-[:EDGE]->_2,
                      |_2-[:EDGE]->_3""".stripMargin)

    val result = executeWithAllPlanners("""MATCH ()-[r:`EDGE`]-()
                                          |WITH r
                                          |MATCH p=(n)-[*0..1]-()-[r]-()-[*0..1]-(m)
                                          |RETURN count(p) as c""".stripMargin)

    result.columnAs[Long]("c").toList should equal(List(32))
    result.close()
  }


  def haveNoneRelFilter: Matcher[InternalExecutionResult] = new Matcher[InternalExecutionResult] {
    override def apply(result: InternalExecutionResult): MatchResult = {
      val plan: InternalPlanDescription = result.executionPlanDescription()
      val res = plan.find("Filter").exists { p =>
        p.arguments.exists {
          case LegacyExpression(NoneInCollection(_, _, _)) => true
          case _ => false
        }
      }
      MatchResult(
        matches = res,
        rawFailureMessage = s"Plan should have Filter with NONE comprehension:\n$plan",
        rawNegatedFailureMessage = s"Plan should not have Filter with NONE comprehension:\n$plan")
    }
  }

  /*
  This tree model generator will generate a binary tree, starting with a single root(named "n0").
  'maxNodeDepth' refers to the node depth, so a value of 4 will generate a root, 2 children, 4 grandchildren
   and 8 grandchildren (so the depth of the tree is 3)
  */
  private def makeTreeModel(maxNodeDepth: Int, directions: Seq[Direction] = Seq()) = {

    val nodes = mutable.Map[String, Node]()
    for {
      depth <- 0 until maxNodeDepth
      width = math.pow(2, depth).toInt
      index <- 0 until width
    } {

      val inum = "0" * width + index.toBinaryString
      val name = "n" + inum.substring(inum.length - (depth + 1), inum.length)
      val parentName = name.substring(0, name.length - 1)
      nodes(name) = createNode(Map("id" -> name))
      if (nodes.isDefinedAt(parentName)) {
        val dir = if (directions.length >= depth) directions(depth - 1) else OUTGOING
        dir match {
          case OUTGOING =>
            relate(nodes(parentName), nodes(name), "LIKES")
          case INCOMING =>
            relate(nodes(name), nodes(parentName), "LIKES")
          case _ =>
            throw new IllegalArgumentException("Only accept INCOMING and OUTGOING")
        }
      }
    }
    nodes.toMap
  }

  //The root will be generation 1
  private def generation(gen: Int)(implicit nodes: Map[String, Node]): Set[Node] =
    nodes.filter { elem =>
      elem._1.length == gen + 1
    }.values.toSet match {
      case s: Set[Node] if s.nonEmpty => s
      case _ => throw new UnsupportedOperationException("Unexpectedly empty test set")
    }

}
