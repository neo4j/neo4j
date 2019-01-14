/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.runtime.InternalExecutionResult
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.v3_4.expressions.NoneIterablePredicate
import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.graphdb.Direction._
import org.neo4j.graphdb.{Direction, Node}
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.mutable

class VarLengthPlanningTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  private val expectedToSucceed = Configs.Interpreted

  test("should handle LIKES*0.LIKES") {
    //Given
    makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWith(expectedToSucceed, "MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*0]->()-[r:LIKES]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
  }

  test("should handle LIKES.LIKES*0") {
    //Given
    makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWith(expectedToSucceed, "MATCH (p { id:'n0' }) MATCH (p)-[:LIKES]->()-[r:LIKES*0]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
  }

  test("should handle LIKES*1.LIKES") {
    //Given
    makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWith(expectedToSucceed, "MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*1]->()-[r:LIKES]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
  }

  test("should handle LIKES.LIKES*1") {
    //Given
    makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWith(expectedToSucceed, "MATCH (p { id:'n0' }) MATCH (p)-[:LIKES]->()-[r:LIKES*1]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
  }

  test("should handle LIKES*2.LIKES") {
    //Given
    makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWith(expectedToSucceed, "MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*2]->()-[r:LIKES]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
  }

  test("should handle LIKES.LIKES*2") {
    //Given
    makeTreeModel(maxNodeDepth = 4)
    //When
    val result = executeWith(expectedToSucceed, "MATCH (p { id:'n0' }) MATCH (p)-[:LIKES]->()-[r:LIKES*2]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
  }

  test("should handle LIKES.LIKES*3") {
    //Given
    makeTreeModel(maxNodeDepth = 5)
    //When
    val result = executeWith(expectedToSucceed, "MATCH (p { id:'n0' }) MATCH (p)-[:LIKES]->()-[r:LIKES*3]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
  }

  test("should handle <-[:LIKES]-()-[r:LIKES*3]->") {
    //Given
    makeTreeModel(maxNodeDepth = 5, directions = Seq(INCOMING))
    //When
    val result = executeWith(expectedToSucceed, "MATCH (p { id:'n0' }) MATCH (p)<-[:LIKES]-()-[r:LIKES*3]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
  }

  test("should handle -[:LIKES]->()<-[r:LIKES*3]-") {
    //Given
    makeTreeModel(maxNodeDepth = 5, directions = Seq(OUTGOING, INCOMING, INCOMING, INCOMING))
    //When
    val result = executeWith(expectedToSucceed, "MATCH (p { id:'n0' }) MATCH (p)-[:LIKES]->()<-[r:LIKES*3]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
  }

  test("should handle LIKES*1.LIKES.LIKES*2") {
    //Given
    makeTreeModel(maxNodeDepth = 5)
    //When
    val result = executeWith(expectedToSucceed, "MATCH (p { id:'n0' }) MATCH (p)-[:LIKES*1]->()-[:LIKES]->()-[r:LIKES*2]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
  }

  test("should handle LIKES.LIKES*2.LIKES") {
    //Given
    makeTreeModel(maxNodeDepth = 5)
    //When
    val result = executeWith(expectedToSucceed, "MATCH (p { id:'n0' }) MATCH (p)-[:LIKES]->()-[:LIKES*2]->()-[r:LIKES]->(c) RETURN c")
    //Then
    result should haveNoneRelFilter
  }

  def haveNoneRelFilter: Matcher[InternalExecutionResult] = new Matcher[InternalExecutionResult] {
    override def apply(result: InternalExecutionResult): MatchResult = {
      val plan: InternalPlanDescription = result.executionPlanDescription()
      val res = plan.find("Filter").exists { p =>
        p.arguments.exists {
          case Arguments.Expression(NoneIterablePredicate(_, _)) => true
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

}
