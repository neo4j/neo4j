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
package org.neo4j.internal.cypher.acceptance

import java.time.LocalDate

import org.neo4j.cypher._
import org.neo4j.cypher.internal.ir.v3_5.ProvidedOrder
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport.Configs

class IndexWithProvidedOrderAcceptanceTest extends ExecutionEngineFunSuite with QueryStatisticsTestSupport with CypherComparisonSupport {

  override def beforeEach(): Unit = {
    super.beforeEach()
    createSomeNodes()
    graph.createIndex("Awesome", "prop1")
    graph.createIndex("Awesome", "prop2")
    graph.createIndex("Awesome", "prop1", "prop2")
    graph.createIndex("Awesome", "prop3")
    graph.createIndex("Awesome", "prop4")
    graph.createIndex("DateString", "ds")
    graph.createIndex("DateDate", "d")
  }

  // Invoked once before the Tx and once in the same Tx
  def createSomeNodes(): Unit = {
    graph.execute(
      """CREATE (:Awesome {prop1: 40, prop2: 5})-[:R]->()
        |CREATE (:Awesome {prop1: 41, prop2: 2})-[:R]->()
        |CREATE (:Awesome {prop1: 42, prop2: 3})-[:R]->()
        |CREATE (:Awesome {prop1: 43, prop2: 1})-[:R]->()
        |CREATE (:Awesome {prop1: 44, prop2: 3})-[:R]->()
        |CREATE (:Awesome {prop2: 7})-[:R]->()
        |CREATE (:Awesome {prop2: 9})-[:R]->()
        |CREATE (:Awesome {prop2: 8})-[:R]->()
        |CREATE (:Awesome {prop2: 7})-[:R]->()
        |CREATE (:Awesome {prop3: 'footurama', prop4:'bar'})-[:R]->()
        |CREATE (:Awesome {prop3: 'fooism', prop4:'rab'})-[:R]->()
        |CREATE (:Awesome {prop3: 'ismfama', prop4:'rab'})-[:R]->()
        |
        |CREATE (:DateString {ds: '2018-01-01'})
        |CREATE (:DateString {ds: '2018-02-01'})
        |CREATE (:DateString {ds: '2018-04-01'})
        |CREATE (:DateString {ds: '2017-03-01'})
        |
        |CREATE (:DateDate {d: date('2018-02-10')})
        |CREATE (:DateDate {d: date('2018-01-10')})
      """.stripMargin)
  }

  test("should use index order for range predicate when returning that property") {
    val result = executeWith(Configs.Interpreted, "MATCH (n:Awesome) WHERE n.prop2 > 1 RETURN n.prop2 ORDER BY n.prop2", executeBefore = createSomeNodes)

    result.executionPlanDescription() should not (includeSomewhere.aPlan("Sort"))
    result.toList should equal(List(
      Map("n.prop2" -> 2), Map("n.prop2" -> 2),
      Map("n.prop2" -> 3), Map("n.prop2" -> 3),
      Map("n.prop2" -> 3), Map("n.prop2" -> 3),
      Map("n.prop2" -> 5), Map("n.prop2" -> 5)))
  }

  test("Order by index backed property renamed in an earlier WITH") {
    val result = executeWith(Configs.Interpreted,
      """MATCH (n:Awesome) WHERE n.prop3 STARTS WITH 'foo'
        |WITH n AS nnn
        |MATCH (m)<-[r]-(nnn)
        |RETURN nnn.prop3 ORDER BY nnn.prop3""".stripMargin, executeBefore = createSomeNodes)

    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Sort")) and
        includeSomewhere.aPlan("Projection")
          .withOrder(ProvidedOrder.asc("nnn.prop3"))
            .onTopOf(aPlan("NodeIndexSeekByRange")
              .withOrder(ProvidedOrder.asc("n.prop3"))
            )
      )

    result.toList should equal(List(
      Map("nnn.prop3" -> "fooism"), Map("nnn.prop3" -> "fooism"),
      Map("nnn.prop3" -> "footurama"), Map("nnn.prop3" -> "footurama")
      ))
  }

  test("Order by index backed property in a plan with an Apply") {
    val result = executeWith(Configs.Interpreted - Configs.Version3_1 - Configs.Version2_3 - Configs.AllRulePlanners,
      "MATCH (a:DateString), (b:DateDate) WHERE a.ds STARTS WITH '2018' AND b.d > date(a.ds) RETURN a.ds ORDER BY a.ds", executeBefore = createSomeNodes)

    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Sort")) and
        includeSomewhere.aPlan("Apply")
          .withOrder(ProvidedOrder.asc("a.ds"))
          .withLHS(
            aPlan("NodeIndexSeekByRange")
            .withOrder(ProvidedOrder.asc("a.ds"))
          )
          .withRHS(
            aPlan("NodeIndexSeekByRange")
            .withOrder(ProvidedOrder.asc("b.d"))
          )
      )

    result.toList should equal(List(
      Map("a.ds" -> "2018-01-01"), Map("a.ds" -> "2018-01-01"),
      Map("a.ds" -> "2018-01-01"), Map("a.ds" -> "2018-01-01"),
      Map("a.ds" -> "2018-01-01"), Map("a.ds" -> "2018-01-01"),
      Map("a.ds" -> "2018-01-01"), Map("a.ds" -> "2018-01-01"),
      Map("a.ds" -> "2018-02-01"), Map("a.ds" -> "2018-02-01"),
      Map("a.ds" -> "2018-02-01"), Map("a.ds" -> "2018-02-01")
    ))
  }

  test("Order by index backed property in a plan with an aggregation and an expand") {
    val result = executeWith(Configs.Interpreted,
      "MATCH (a:Awesome)-[r]->(b) WHERE a.prop2 > 1 RETURN a.prop2, count(b) ORDER BY a.prop2", executeBefore = createSomeNodes)

    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Sort")) and
        includeSomewhere.aPlan("EagerAggregation")
          .onTopOf(
            aPlan("Expand(All)")
              .withOrder(ProvidedOrder.asc("a.prop2"))
              .onTopOf(
                aPlan("NodeIndexSeekByRange")
                  .withOrder(ProvidedOrder.asc("a.prop2"))))
      )

    result.toList should equal(List(
      Map("a.prop2" -> 2, "count(b)" -> 2),
      Map("a.prop2" -> 3, "count(b)" -> 4),
      Map("a.prop2" -> 5, "count(b)" -> 2),
      Map("a.prop2" -> 7, "count(b)" -> 4),
      Map("a.prop2" -> 8, "count(b)" -> 2),
      Map("a.prop2" -> 9, "count(b)" -> 2)
    ))
  }

  test("Order by index backed property in a plan with a distinct") {
    val result = executeWith(Configs.Interpreted,
      "MATCH (a:Awesome)-[r]->(b) WHERE a.prop2 > 1 RETURN DISTINCT a.prop2 ORDER BY a.prop2", executeBefore = createSomeNodes)

    result.executionPlanDescription() should (
      not(includeSomewhere.aPlan("Sort")) and
        includeSomewhere.aPlan("Distinct")
          .onTopOf(
            aPlan("Expand(All)")
              .withOrder(ProvidedOrder.asc("a.prop2"))
              .onTopOf(
                aPlan("NodeIndexSeekByRange")
                  .withOrder(ProvidedOrder.asc("a.prop2"))))
      )

    result.toList should equal(List(
      Map("a.prop2" -> 2),
      Map("a.prop2" -> 3),
      Map("a.prop2" -> 5),
      Map("a.prop2" -> 7),
      Map("a.prop2" -> 8),
      Map("a.prop2" -> 9)
    ))
  }
}
