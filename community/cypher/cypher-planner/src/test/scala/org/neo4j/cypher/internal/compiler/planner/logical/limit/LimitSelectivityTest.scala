/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.limit

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.VariableStringInterpolator
import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.ProcedureCallProjection
import org.neo4j.cypher.internal.compiler.planner.logical.PlannerDefaults
import org.neo4j.cypher.internal.compiler.test_helpers.TestGraphStatistics
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadOnlyAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureReadWriteAccess
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.ir.AggregatingQueryProjection
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.QueryPagination
import org.neo4j.cypher.internal.ir.RegularQueryProjection
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.util.Cardinality
import org.neo4j.cypher.internal.util.Selectivity
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class LimitSelectivityTest extends CypherFunSuite with LogicalPlanningTestSupport2 {

  test("forLastPart: no LIMIT") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val query = RegularSinglePlannerQuery()

      // WHEN
      val result = LimitSelectivity.forLastPart(query, context, Selectivity.ONE)

      // THEN
      result shouldBe Selectivity.ONE
    }
  }

  test("forLastPart: LIMIT") {
    val limit = 10
    val nodes = 100

    // MATCH (n) RETURN n LIMIT 10
    new givenConfig {
      statistics = new TestGraphStatistics() {
        override def nodesAllCardinality(): Cardinality = Cardinality(nodes)
      }
    }.withLogicalPlanningContext { (_, context) =>
      val query = RegularSinglePlannerQuery(
        queryGraph = QueryGraph(patternNodes = Set(v"n")),
        horizon = RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(limit))))
      )

      // WHEN
      val result = LimitSelectivity.forLastPart(query, context, Selectivity.ONE)

      // THEN
      result shouldBe Selectivity(limit / nodes.toDouble)
    }
  }

  test("forLastPart: no LIMIT, parentLimitSelectivity") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val query = RegularSinglePlannerQuery()
      val p = Selectivity(0.5)

      // WHEN
      val result = LimitSelectivity.forLastPart(query, context, p)

      // THEN
      result shouldBe p
    }
  }

  test("forLastPart: LIMIT, parentLimitSelectivity") {
    val limit = 10
    val nodes = 100

    // MATCH (n) RETURN n LIMIT 10
    new givenConfig {
      statistics = new TestGraphStatistics() {
        override def nodesAllCardinality(): Cardinality = Cardinality(nodes)
      }
    }.withLogicalPlanningContext { (_, context) =>
      val query = RegularSinglePlannerQuery(
        queryGraph = QueryGraph(patternNodes = Set(v"n")),
        horizon = RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(limit))))
      )
      val p = Selectivity(0.5)

      // WHEN
      val result = LimitSelectivity.forLastPart(query, context, p)

      // THEN
      result shouldBe Selectivity(limit / nodes.toDouble) * p
    }
  }

  test("forAllParts: no LIMIT") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val query = RegularSinglePlannerQuery()

      // WHEN
      val result = LimitSelectivity.forAllParts(query, context)

      // THEN
      result shouldBe List(Selectivity.ONE)
    }
  }

  test("forAllParts: no LIMIT, tail") {
    new givenConfig().withLogicalPlanningContext { (_, context) =>
      val query = RegularSinglePlannerQuery(
        tail = Some(RegularSinglePlannerQuery())
      )

      // WHEN
      val result = LimitSelectivity.forAllParts(query, context)

      // THEN
      result shouldBe List(Selectivity.ONE, Selectivity.ONE)
    }
  }

  test("forAllParts: LIMIT in first part, no tail") {
    val limit = 10
    val nodes = 100

    // MATCH (n) RETURN n LIMIT 10
    new givenConfig {
      statistics = new TestGraphStatistics() {
        override def nodesAllCardinality(): Cardinality = Cardinality(nodes)
      }
    }.withLogicalPlanningContext { (_, context) =>
      val query = RegularSinglePlannerQuery(
        queryGraph = QueryGraph(patternNodes = Set(v"n")),
        horizon = RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(limit))))
      )

      // WHEN
      val result = LimitSelectivity.forAllParts(query, context)

      // THEN
      result shouldBe List(Selectivity(limit / nodes.toDouble))
    }
  }

  test("forAllParts: LIMIT in first part, tail") {
    val limit = 10
    val nodes = 100

    // MATCH (n) WITH n LIMIT 10 MATCH (m)
    new givenConfig {
      statistics = new TestGraphStatistics() {
        override def nodesAllCardinality(): Cardinality = Cardinality(nodes)
      }
    }.withLogicalPlanningContext { (_, context) =>
      val query = RegularSinglePlannerQuery(
        queryGraph = QueryGraph(patternNodes = Set(v"n")),
        horizon = RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(limit)))),
        tail = Some(RegularSinglePlannerQuery(
          queryGraph = QueryGraph(argumentIds = Set(v"n"), patternNodes = Set(v"m"))
        ))
      )

      // WHEN
      val result = LimitSelectivity.forAllParts(query, context)

      // THEN
      result shouldBe List(
        Selectivity(limit / nodes.toDouble),
        Selectivity.ONE
      )
    }
  }

  test("forAllParts: no LIMIT in first part, tail with LIMIT") {
    val limit = 10
    val nodes = 100

    // MATCH (n) WITH n MATCH (m) RETURN * LIMIT 10
    new givenConfig {
      statistics = new TestGraphStatistics() {
        override def nodesAllCardinality(): Cardinality = Cardinality(nodes)
      }
    }.withLogicalPlanningContext { (_, context) =>
      val query = RegularSinglePlannerQuery(
        queryGraph = QueryGraph(patternNodes = Set(v"n")),
        tail = Some(RegularSinglePlannerQuery(
          queryGraph = QueryGraph(argumentIds = Set(v"n"), patternNodes = Set(v"m")),
          horizon = RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(limit))))
        ))
      )

      // WHEN
      val result = LimitSelectivity.forAllParts(query, context)

      // THEN
      val expectedSelectivity = Selectivity(limit / (nodes.toDouble * nodes.toDouble))
      result shouldBe List(expectedSelectivity, expectedSelectivity)
    }
  }

  test("forAllParts: with LIMIT in first part and tail with LIMIT") {
    val lowLimit = 75 // Higher than 5000 / 100
    val highLimit = 5000
    val nodes = 100

    // MATCH (n) WITH n LIMIT <lowLimit> MATCH (m) RETURN * LIMIT <highLimit>
    new givenConfig {
      statistics = new TestGraphStatistics() {
        override def nodesAllCardinality(): Cardinality = Cardinality(nodes)
      }
    }.withLogicalPlanningContext { (_, context) =>
      val query = RegularSinglePlannerQuery(
        queryGraph = QueryGraph(patternNodes = Set(v"n")),
        horizon = RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(lowLimit)))),
        tail = Some(RegularSinglePlannerQuery(
          queryGraph = QueryGraph(argumentIds = Set(v"n"), patternNodes = Set(v"m")),
          horizon = RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(highLimit))))
        ))
      )

      // WHEN
      val result = LimitSelectivity.forAllParts(query, context)

      // THEN
      result shouldBe List(
        Selectivity(highLimit / (nodes.toDouble * nodes.toDouble)),
        Selectivity(highLimit / (lowLimit.toDouble * nodes.toDouble))
      )
    }
  }

  test("forAllParts: with LIMIT in first part and tail with LIMIT 2") {
    val lowLimit = 25 // Lower than 5000 / 100
    val highLimit = 5000
    val nodes = 100

    // MATCH (n) WITH n LIMIT <lowLimit> MATCH (m) RETURN * LIMIT <highLimit>
    new givenConfig {
      statistics = new TestGraphStatistics() {
        override def nodesAllCardinality(): Cardinality = Cardinality(nodes)
      }
    }.withLogicalPlanningContext { (_, context) =>
      val query = RegularSinglePlannerQuery(
        queryGraph = QueryGraph(patternNodes = Set(v"n")),
        horizon = RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(lowLimit)))),
        tail = Some(RegularSinglePlannerQuery(
          queryGraph = QueryGraph(argumentIds = Set(v"n"), patternNodes = Set(v"m")),
          horizon = RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(highLimit))))
        ))
      )

      // WHEN
      val result = LimitSelectivity.forAllParts(query, context)

      // THEN
      result shouldBe List(
        Selectivity(lowLimit / nodes.toDouble),
        Selectivity.ONE
      )
    }
  }

  test("forAllParts: multiple tails") {
    val lowLimit = 25
    val midLimit = 75
    val highLimit = 5000
    val nodes = 100

    Seq(lowLimit, midLimit, highLimit).permutations.foreach {
      case Seq(firstLimit, secondLimit, thirdLimit) =>
        // MATCH (n) WITH * LIMIT <firstLimit> WITH * LIMIT <secondLimit> RETURN * LIMIT <thirdLimit>
        new givenConfig {
          statistics = new TestGraphStatistics() {
            override def nodesAllCardinality(): Cardinality = Cardinality(nodes)
          }
        }.withLogicalPlanningContext { (_, context) =>
          val query = RegularSinglePlannerQuery(
            queryGraph = QueryGraph(patternNodes = Set(v"n")),
            horizon = RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(firstLimit)))),
            tail = Some(RegularSinglePlannerQuery(
              queryGraph = QueryGraph(argumentIds = Set(v"n")),
              horizon =
                RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(secondLimit)))),
              tail = Some(RegularSinglePlannerQuery(
                queryGraph = QueryGraph(argumentIds = Set(v"n")),
                horizon =
                  RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(thirdLimit))))
              ))
            ))
          )

          // WHEN
          val result = LimitSelectivity.forAllParts(query, context)

          // THEN
          result shouldBe List(
            Selectivity(lowLimit / nodes.toDouble),
            Selectivity
              .of(math.min(secondLimit, thirdLimit) / math.min(nodes.toDouble, firstLimit))
              .getOrElse(Selectivity.ONE),
            Selectivity
              .of(thirdLimit.toDouble / math.min(firstLimit, secondLimit))
              .getOrElse(Selectivity.ONE)
          )
        }
      case _ => sys.error("the impossible happened")
    }
  }

  test("forAllParts: updating statement in first part, horizon with LIMIT") {
    val limit = 10
    val nodes = 100

    // MATCH (n), (m) SET n.foo = 1 RETURN * LIMIT 10
    new givenConfig {
      statistics = new TestGraphStatistics() {
        override def nodesAllCardinality(): Cardinality = Cardinality(nodes)
      }
    }.withLogicalPlanningContext { (_, context) =>
      val query = RegularSinglePlannerQuery(
        queryGraph = QueryGraph(
          patternNodes = Set(v"n", v"m"),
          mutatingPatterns = IndexedSeq(SetNodePropertyPattern(varFor("n"), PropertyKeyName("foo")(pos), literalInt(1)))
        ),
        horizon = RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(limit))))
      )

      // WHEN
      val result = LimitSelectivity.forAllParts(query, context)

      // THEN
      result shouldBe List(Selectivity.ONE)
    }
  }

  test("forAllParts: updating procedure call in first horizon, tail with LIMIT") {
    val limit = 10
    val nodes = 100

    val ns = Namespace(List("my", "proc"))(pos)
    val name = ProcedureName("foo")(pos)
    val qualifiedName = QualifiedName(ns.parts, name.name)
    val signature = ProcedureSignature(qualifiedName, IndexedSeq.empty, None, None, ProcedureReadWriteAccess, id = 42)

    val resolvedCall = ResolvedCall(signature, Seq.empty, IndexedSeq.empty)(pos)

    // MATCH (n), (m) CALL my.proc.foo() RETURN * LIMIT 10
    new givenConfig {
      statistics = new TestGraphStatistics() {
        override def nodesAllCardinality(): Cardinality = Cardinality(nodes)
      }
    }.withLogicalPlanningContext { (_, context) =>
      val query = RegularSinglePlannerQuery(
        queryGraph = QueryGraph(patternNodes = Set(v"n", v"m")),
        horizon = ProcedureCallProjection(resolvedCall),
        tail = Some(RegularSinglePlannerQuery(
          queryGraph = QueryGraph(argumentIds = Set(v"n", v"m")),
          horizon = RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(limit))))
        ))
      )

      // WHEN
      val result = LimitSelectivity.forAllParts(query, context)

      // THEN
      result shouldBe List(
        Selectivity.ONE,
        Selectivity.ONE
      )
    }
  }

  test("forAllParts: eager procedure call in first horizon, tail with LIMIT") {
    val limit = 10
    val nodes = 100

    val ns = Namespace(List("my", "proc"))(pos)
    val name = ProcedureName("foo")(pos)
    val qualifiedName = QualifiedName(ns.parts, name.name)
    val signature =
      ProcedureSignature(qualifiedName, IndexedSeq.empty, None, None, ProcedureReadOnlyAccess, id = 42, eager = true)

    val resolvedCall = ResolvedCall(signature, Seq.empty, IndexedSeq.empty)(pos)

    // MATCH (n), (m) CALL my.proc.foo() RETURN * LIMIT 10
    new givenConfig {
      statistics = new TestGraphStatistics() {
        override def nodesAllCardinality(): Cardinality = Cardinality(nodes)
      }
    }.withLogicalPlanningContext { (_, context) =>
      val query = RegularSinglePlannerQuery(
        queryGraph = QueryGraph(patternNodes = Set(v"n", v"m")),
        horizon = ProcedureCallProjection(resolvedCall),
        tail = Some(RegularSinglePlannerQuery(
          queryGraph = QueryGraph(argumentIds = Set(v"n", v"m")),
          horizon = RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(limit))))
        ))
      )

      // WHEN
      val result = LimitSelectivity.forAllParts(query, context)

      // THEN
      result shouldBe List(
        Selectivity.ONE,
        Selectivity(limit / (nodes * nodes * PlannerDefaults.DEFAULT_MULTIPLIER.coefficient))
      )
    }
  }

  test("forAllParts: reading procedure call in first horizon, tail with LIMIT") {
    val limit = 10
    val nodes = 100

    val ns = Namespace(List("my", "proc"))(pos)
    val name = ProcedureName("foo")(pos)
    val qualifiedName = QualifiedName(ns.parts, name.name)
    val signature = ProcedureSignature(qualifiedName, IndexedSeq.empty, None, None, ProcedureReadOnlyAccess, id = 42)

    val resolvedCall = ResolvedCall(signature, Seq.empty, IndexedSeq.empty)(pos)

    // MATCH (n), (m) CALL my.proc.foo() RETURN * LIMIT 10
    new givenConfig {
      statistics = new TestGraphStatistics() {
        override def nodesAllCardinality(): Cardinality = Cardinality(nodes)
      }
    }.withLogicalPlanningContext { (_, context) =>
      val query = RegularSinglePlannerQuery(
        queryGraph = QueryGraph(patternNodes = Set(v"n", v"m")),
        horizon = ProcedureCallProjection(resolvedCall),
        tail = Some(RegularSinglePlannerQuery(
          queryGraph = QueryGraph(argumentIds = Set(v"n", v"m")),
          horizon = RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(limit))))
        ))
      )

      // WHEN
      val result = LimitSelectivity.forAllParts(query, context)

      // THEN
      val expectedSelectivity =
        Selectivity(limit / (nodes.toDouble * nodes.toDouble * PlannerDefaults.DEFAULT_MULTIPLIER.coefficient))
      result shouldBe List(expectedSelectivity, expectedSelectivity)
    }
  }

  test("forAllParts: limit in first part, updating statement in tail") {
    val limit = 10
    val nodes = 100

    // MATCH (n), (m) WITH * LIMIT 10 SET n.foo = 1
    new givenConfig {
      statistics = new TestGraphStatistics() {
        override def nodesAllCardinality(): Cardinality = Cardinality(nodes)
      }
    }.withLogicalPlanningContext { (_, context) =>
      val query = RegularSinglePlannerQuery(
        queryGraph = QueryGraph(patternNodes = Set(v"n", v"m")),
        horizon = RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(limit)))),
        tail = Some(RegularSinglePlannerQuery(
          queryGraph = QueryGraph(
            argumentIds = Set(v"n", v"m"),
            mutatingPatterns =
              IndexedSeq(SetNodePropertyPattern(varFor("n"), PropertyKeyName("foo")(pos), literalInt(1)))
          )
        ))
      )

      // WHEN
      val result = LimitSelectivity.forAllParts(query, context)

      // THEN
      result shouldBe List(
        Selectivity(limit / (nodes.toDouble * nodes.toDouble)),
        Selectivity.ONE
      )
    }
  }

  test("forAllParts: aggregation in first horizon, tail with LIMIT") {
    val limit = 10
    val nodes = 100

    // MATCH (n) WITH count(*) AS c MATCH (m) RETURN * LIMIT 10
    new givenConfig {
      statistics = new TestGraphStatistics() {
        override def nodesAllCardinality(): Cardinality = Cardinality(nodes)
      }
    }.withLogicalPlanningContext { (_, context) =>
      val query = RegularSinglePlannerQuery(
        queryGraph = QueryGraph(patternNodes = Set(v"n")),
        horizon = AggregatingQueryProjection(Map.empty, Map(v"c" -> countStar())),
        tail = Some(RegularSinglePlannerQuery(
          queryGraph = QueryGraph(patternNodes = Set(v"m"), argumentIds = Set(v"c")),
          horizon = RegularQueryProjection(queryPagination = QueryPagination(limit = Some(literalInt(limit))))
        ))
      )

      // WHEN
      val result = LimitSelectivity.forAllParts(query, context)

      // THEN
      result shouldBe List(
        Selectivity.ONE,
        Selectivity(limit / nodes.toDouble)
      )
    }
  }
}
