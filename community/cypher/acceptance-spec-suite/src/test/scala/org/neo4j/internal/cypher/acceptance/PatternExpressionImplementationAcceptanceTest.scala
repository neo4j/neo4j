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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v3_2.commands.expressions.PathImpl
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription.Arguments.{EstimatedRows, ExpandExpression}
import org.neo4j.cypher.internal.frontend.v3_2.SemanticDirection
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.graphdb.Node
import org.scalatest.Matchers

// TCK'd
// All test cases in this class have TCK representations
class PatternExpressionImplementationAcceptanceTest extends ExecutionEngineFunSuite with Matchers with NewPlannerTestSupport {

  // TESTS WITH CASE EXPRESSION

  test("match (n) return case when id(n) >= 0 then (n)-->() otherwise 42 as p") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode("match (n) return case when id(n) >= 0 then (n)-->() else 42 end as p")

    result.toList.head("p").asInstanceOf[Seq[_]] should have size 2
    result shouldNot use("Expand(All)")
  }

  test("match (n) return case when id(n) < 0 then (n)-->() otherwise 42 as p") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode("match (n) return case when id(n) < 0 then (n)-->() else 42 end as p")

    result.toList.head("p").asInstanceOf[Long] should equal(42)
    result shouldNot use("Expand(All)")
  }

  test("match (n) return case when n:A then (n)-->(:C) when n:B then (n)-->(:D) else 42 end as p") {
    val start = createLabeledNode("A")
    val c = createLabeledNode("C")
    val rel1 = relate(start, c)
    val rel2 = relate(start, c)
    val start2 = createLabeledNode("B")
    val d = createLabeledNode("D")
    val rel3 = relate(start2, d)
    val rel4 = relate(start2, d)

    graph.inTx {
      val result = executeWithAllPlannersAndCompatibilityMode("match (n) return case when n:A then (n)-->(:C) when n:B then (n)-->(:D) else 42 end as p")
        .map(_.mapValues {
          case l: Seq[Any] => l.toSet
          case x => x
        }).toList

      result should equal(List(
        Map("p" -> Set(new PathImpl(start, rel2, c), new PathImpl(start, rel1, c))),
        Map("p" -> 42),
        Map("p" -> Set(new PathImpl(start2, rel4, d), new PathImpl(start2, rel3, d))),
        Map("p" -> 42)
      ))
    }
  }

  test("match (n) with case when id(n) >= 0 then (n)-->() else 42 end as p, count(n) as c return p, c") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode(
      """match (n)
        |with case
        |       when id(n) >= 0 then (n)-->()
        |       else 42
        |     end as p, count(n) as c
        |return p, c order by c""".stripMargin)
      .toList.head("p").asInstanceOf[Seq[_]]

    result should have size 2
  }

  test("match (n) with case when id(n) < 0 then (n)-->() else 42 end as p, count(n) as c return p, c") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode("match (n) with case when id(n) < 0 then (n)-->() else 42 end as p, count(n) as c return p, c")
      .toList.head("p").asInstanceOf[Long]

    result should equal(42)
  }

  test("match (n) with case when n:A then (n)-->(:C) when n:B then (n)-->(:D) else 42 end as p, count(n) as c return p, c") {
    val start = createLabeledNode("A")
    val c = createLabeledNode("C")
    val rel1 = relate(start, c)
    val rel2 = relate(start, c)
    val start2 = createLabeledNode("B")
    val d = createLabeledNode("D")
    val rel3 = relate(start2, d)
    val rel4 = relate(start2, d)

    graph.inTx {
      val result = executeWithAllPlannersAndCompatibilityMode("match (n) with case when n:A then (n)-->(:C) when n:B then (n)-->(:D) else 42 end as p, count(n) as c return p, c")
        .map(_.mapValues {
          case l: Seq[Any] => l.toSet
          case x => x
        }).toSet

      result should equal(Set(
        Map("c" -> 1, "p" -> Set(new PathImpl(start, rel2, c), new PathImpl(start, rel1, c))),
        Map("c" -> 1, "p" -> Set(new PathImpl(start2, rel4, d), new PathImpl(start2, rel3, d))),
        Map("c" -> 2, "p" -> 42)
      ))
    }
  }

  test("match (n) where (case when id(n) >= 0 then length((n)-->()) else 42 end) > 0 return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode("match (n) where (case when id(n) >= 0 then length((n)-->()) else 42 end) > 0 return n")

    result.toList should equal(List(
      Map("n" -> start)
    ))
    result shouldNot use("RollUpApply")
  }

  test("match (n) where (case when id(n) < 0 then length((n)-->()) else 42 end) > 0 return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode("match (n) where (case when id(n) < 0 then length((n)-->()) else 42 end) > 0 return n")

    result should have size 3
  }

  test("match (n) where (case when id(n) < 0 then length((n)-[:X]->()) else 42 end) > 0 return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlanners("match (n) where (case when id(n) < 0 then length((n)-[:X]->()) else 42 end) > 0 return n")

    result should have size 3
  }

  test("match (n) where (case when id(n) < 0 then length((n)-->(:X)) else 42 end) > 0 return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlanners("match (n) where (case when id(n) < 0 then length((n)-->(:X)) else 42 end) > 0 return n")

    result should have size 3
    result shouldNot use("RollUpApply")
  }

  test("match (n) where (case when n:A then length((n)-->(:C)) when n:B then length((n)-->(:D)) else 42 end) > 1 return n") {
    val start = createLabeledNode("A")
    relate(start, createLabeledNode("C"))
    relate(start, createLabeledNode("C"))
    val start2 = createLabeledNode("B")
    relate(start2, createLabeledNode("D"))
    val start3 = createNode()
    relate(start3, createNode())

    graph.inTx {
      val result = executeWithAllPlannersAndCompatibilityMode("match (n) where (n)-->() AND (case when n:A then length((n)-->(:C)) when n:B then length((n)-->(:D)) else 42 end) > 1 return n")

      result.toList should equal(List(
        Map("n" -> start),
        Map("n" -> start3)
      ))
      result shouldNot use("RollUpApply")
    }
  }

  test("case expressions and pattern expressions") {
    val n1 = createLabeledNode(Map("prop" -> 42), "A")

    relate(n1, createNode())
    relate(n1, createNode())
    relate(n1, createNode())

    val result = executeWithAllPlanners(
      """match (a:A)
        |return case
        |         WHEN a.prop = 42 THEN []
        |         ELSE (a)-->()
        |       END as X
        |         """.stripMargin)

    result shouldNot use("RollUpApply")

    result.toList should equal(List(Map("X" -> Seq())))
  }

  test("should not use full expand 1") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode("match (n) return case when id(n) >= 0 then (n)-->() else 42 end as p")

    result shouldNot use("Expand(All)")
  }

  test("should not use full expand 2") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode("match (n) return case when id(n) < 0 then (n)-->() else 42 end as p")

    result shouldNot use("Expand(All)")
  }

  // TESTS WITH EXTRACT

  test("match (n) return extract(x IN (n)-->() | head(nodes(x)) )  as p") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode("match (n) return extract(x IN (n)-->() | head(nodes(x)) )  as p")

    result.toList.head("p").asInstanceOf[Seq[_]] should equal(List(start, start))
  }

  test("match (n:A) with extract(x IN (n)-->() | head(nodes(x)) ) as p, count(n) as c return p, c") {
    val start = createLabeledNode("A")
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode("match (n:A) with extract(x IN (n)-->() | head(nodes(x)) ) as p, count(n) as c return p, c")
      .toList.head("p").asInstanceOf[Seq[_]]

    result should equal(List(start, start))
  }

  test("match (n) where n IN extract(x IN (n)-->() | head(nodes(x)) ) return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode("match (n) where n IN extract(x IN (n)-->() | head(nodes(x)) ) return n")
      .toList

    result should equal(List(
      Map("n" -> start)
    ))
  }

  // TESTS WITH PLANNING ASSERTIONS

  test("should use full expand") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode("match (n)-->(b) with (n)-->() as p, count(b) as c return p, c")

    result should use("Expand(All)")
  }

  test("should use varlength expand into when variables are bound") {
    val a = createLabeledNode("Start")
    val b = createLabeledNode("End")
    relate(a, b)

    val result = executeWithAllPlannersAndCompatibilityMode("match (a:Start), (b:End) with (a)-[*]->(b) as path, count(a) as c return path, c")

    result should use("VarLengthExpand(Into)")
  }

  test("should not use a label scan as starting point when statistics are bad") {
    graph.inTx {
      (1 to 10000).foreach { i =>
        createLabeledNode("A")
        createNode()
      }
    }
    relate(createNode(), createLabeledNode("A"), "T")

    val result = executeWithAllPlannersAndCompatibilityMode("PROFILE MATCH ()-[r]->() WHERE ()-[r]-(:A) RETURN r")

    result shouldNot use("NodeByLabelScan")
  }

  test("should consider cardinality input when planning pattern expression in where clause") {
    // given
    val node = createLabeledNode("A")
    createLabeledNode("A")
    createLabeledNode("A")
    relate(node, createNode(), "HAS")

    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:A) WHERE (n)-[:HAS]->() RETURN n")

    val argumentPLan = result.executionPlanDescription().cd("Argument")
    val estimatedRows = argumentPLan.arguments.collect { case n: EstimatedRows => n }.head
    estimatedRows should equal(EstimatedRows(3.0))
  }

  test("should consider cardinality input when planning in return") {
    // given
    val node = createLabeledNode("A")
    createLabeledNode("A")
    createLabeledNode("A")
    val endNode = createNode()
    val rel = relate(node, endNode, "HAS")

    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:A) RETURN (n)-[:HAS]->() as p")

    val executionPlanDescription = result.executionPlanDescription()

    executionPlanDescription.cd("Argument").arguments should equal(List(EstimatedRows(1)))
    executionPlanDescription.cd("Expand(All)").arguments.toSet should equal(Set(
      ExpandExpression("n", "  UNNAMED23", Seq("HAS"), "  UNNAMED32", SemanticDirection.OUTGOING, 1, Some(1)),
      EstimatedRows(0.25)
    ))
  }

  test("should be able to execute aggregating-functions on pattern expressions") {
    // given
    val node = createLabeledNode("A")
    createLabeledNode("A")
    createLabeledNode("A")
    relate(node, createNode(), "HAS")

    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:A) RETURN count((n)-[:HAS]->()) as c")

    result should use("Expand(All)")
  }

  test("use getDegree for simple pattern expression with length clause, outgoing") {
    setup()

    val result = executeWithAllPlanners("MATCH (n:X) WHERE LENGTH((n)-->()) > 2 RETURN n")

    result shouldNot use("RollUpApply")
  }

  test("use getDegree for simple pattern expression with length clause, incoming") {
    setup()

    val result = executeWithAllPlanners("MATCH (n:X) WHERE LENGTH((n)<--()) > 2 RETURN n")

    result shouldNot use("RollUpApply")
  }

  test("use getDegree for simple pattern expression with length clause, both") {
    setup()

    val result = executeWithAllPlanners("MATCH (n:X) WHERE LENGTH((n)--()) > 2 RETURN n")

    result shouldNot use("RollUpApply")
  }

  test("use getDegree for simple pattern expression with rel-type ORs") {
    setup()

    val result = executeWithAllPlanners("MATCH (n) WHERE length((n)-[:X|Y]->()) > 2 RETURN n")

    result shouldNot use("RollUpApply")
  }

  private def setup(): (Node, Node) = {
    val n1 = createLabeledNode("X")
    val n2 = createLabeledNode("X")

    relate(n1, createNode())
    relate(n1, createNode())
    relate(n1, createNode())
    relate(createNode(), n2)
    relate(createNode(), n2)
    relate(createNode(), n2)
    (n1, n2)
  }
}
