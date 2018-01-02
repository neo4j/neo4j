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

import java.io.PrintWriter

import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{ExecutionPlanDescription, Result}

import scala.collection.Map

class Cypher1_9BinaryCompatibilityTest extends CypherFunSuite with GraphDatabaseTestSupport {

  override def databaseConfig(): Map[String, String] = super.databaseConfig() + ("cypher_parser_version" -> "1.9")

  test("Can call all methods on returned result when running via graph.execute") {
    def withResult(f: Result => Unit) = {
      graph.inTx {
        val n0 = createNode()
        val n1 = createNode()
        val result = graph.execute(s"START a=node(${n0.getId}), b=node(${n1.getId}) MATCH (a)-[r1:INTRODUCED]->(b)<-[r2:INTRODUCED]-(c) WHERE r1.year = r2.year RETURN b")
        f(result)
      }
    }

    verifyJavaResult(withResult)
  }

  test("Can call all methods on returned result when running via javacompat engine.execute") {
    val engine = new javacompat.ExecutionEngine(graph)

    def withResult(f: Result => Unit) = {
      graph.inTx {
        val n0 = createNode()
        val n1 = createNode()
        val result = engine.execute(s"START a=node(${n0.getId}), b=node(${n1.getId}) MATCH (a)-[r1:INTRODUCED]->(b)<-[r2:INTRODUCED]-(c) WHERE r1.year = r2.year RETURN b")
        f(result)
      }
    }

    verifyJavaResult(withResult)
  }

  test("Can call all methods on returned result when running via scala engine.execute") {
    val engine = new ExecutionEngine(graph)

    def withResult(f: ExtendedExecutionResult => Unit) = {
      graph.inTx {
        val n0 = createNode()
        val n1 = createNode()
        val result = engine.execute(s"START a=node(${n0.getId}), b=node(${n1.getId}) MATCH (a)-[r1:INTRODUCED]->(b)<-[r2:INTRODUCED]-(c) WHERE r1.year = r2.year RETURN b")
        f(result)
      }
    }

    withResult { result => result.accept(mock[Result.ResultVisitor[RuntimeException]]) }
    withResult { result => result.columns }
    withResult { result => result.columnAs("b") }
    withResult { result => result.executionType }
    withResult { result => result.notifications should be(empty) }
    withResult { result => result.planDescriptionRequested }
    withResult { result => result.dumpToString() }
    withResult { result => result.dumpToString(mock[PrintWriter]) }
    withResult { result => result.javaColumns }
    withResult { result => result.javaColumnAs[Any]("b").hasNext }
    withResult { result => result.javaIterator.hasNext }
    withResult { result => result.javaIterator.hasNext }
    withResult { result => result.close() }
  }


  test("Can call all methods on returned PlanDescription when using graph.execute together with PROFILE") {
    val plan = graph.inTx {
      val n0 = createNode()
      val n1 = createNode()
      val result = graph.execute(s"PROFILE START a=node(${n0.getId}), b=node(${n1.getId}) MATCH (a)-[r1:INTRODUCED]->(b)<-[r2:INTRODUCED]-(c) WHERE r1.year = r2.year RETURN b")
      result.getExecutionPlanDescription
    }

    verifyJavaPlan(plan)
  }

  test("Can call all methods on returned PlanDescription when using javacompat engine.execute together with PROFILE") {
    val engine = new javacompat.ExecutionEngine(graph)

    val plan = graph.inTx {
      val n0 = createNode()
      val n1 = createNode()
      val result = engine.execute(s"PROFILE START a=node(${n0.getId}), b=node(${n1.getId}) MATCH (a)-[r1:INTRODUCED]->(b)<-[r2:INTRODUCED]-(c) WHERE r1.year = r2.year RETURN b")
      result.getExecutionPlanDescription
    }

    verifyJavaPlan(plan)
  }

  test("Can call all methods on returned PlanDescription when using scala engine.profile") {
    val plan = graph.inTx {
      val n0 = createNode()
      val n1 = createNode()
      val engine = new ExecutionEngine(graph)
      val result = engine.profile(s"START a=node(${n0.getId}), b=node(${n1.getId}) MATCH (a)-[r1:INTRODUCED]->(b)<-[r2:INTRODUCED]-(c) WHERE r1.year = r2.year RETURN b")
      result.executionPlanDescription()
    }

    verifyScalaPlan(plan)
  }

  private def verifyJavaResult(withResult: ((Result => Unit) => Unit)): Unit = {
    withResult { result => result.columns() }
    withResult { result => result.columnAs("b") }
    withResult { result => result.getNotifications.iterator().hasNext should be(right = false) }
    withResult { result => result.getQueryStatistics.getConstraintsRemoved }
    withResult { result => result.accept(mock[Result.ResultVisitor[RuntimeException]]) }

  }
  private def verifyJavaPlan(plan: ExecutionPlanDescription): Unit = {
    plan.getName
    val args = plan.getArguments
    val argsIter = args.entrySet().iterator()
    while(argsIter.hasNext) {
      val elem = argsIter.next()
      elem.getKey.toString
      elem.getValue.toString
    }
    plan.getIdentifiers.toArray.map(_.toString)
    if (plan.hasProfilerStatistics) {
      val stats = plan.getProfilerStatistics
      stats.getDbHits
      stats.getRows
    }
    val iter = plan.getChildren.iterator()
    while (iter.hasNext) {
      val next = iter.next()
      verifyJavaPlan(next)
    }
  }

  private def verifyScalaPlan(plan: PlanDescription): Unit = {
    plan.name
    plan.arguments.map(_.toString())
    plan.hasProfilerStatistics
    plan.children.foreach(verifyScalaPlan)
    verifyLegacyJavaPlan(plan.asJava)
  }

  private def verifyLegacyJavaPlan(plan: javacompat.PlanDescription): Unit = {
    plan.getName
    plan.getArguments.values().toArray.map(_.toString)
    if (plan.hasProfilerStatistics) {
      val stats = plan.getProfilerStatistics
      stats.getDbHits
      stats.getRows
    }
    val iter = plan.getChildren.iterator()
    while (iter.hasNext) verifyLegacyJavaPlan(iter.next())
  }
}
