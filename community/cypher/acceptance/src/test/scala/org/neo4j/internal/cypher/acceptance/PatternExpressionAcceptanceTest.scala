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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v3_0.commands.expressions.PathImpl
import org.neo4j.cypher.internal.compiler.v3_0.planDescription.InternalPlanDescription.Arguments.{EstimatedRows, ExpandExpression}
import org.neo4j.cypher.internal.frontend.v3_0.SemanticDirection
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.graphdb.{Node, Relationship}
import org.scalatest.Matchers

class PatternExpressionAcceptanceTest extends ExecutionEngineFunSuite with Matchers with NewPlannerTestSupport {

  test("match (n) return (n)-->()") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode("match (n) return (n)-->() as p").toList.head("p").asInstanceOf[Seq[_]]

    result should have size 2
  }

  test("should handle path predicates with labels") {
    // GIVEN
    val a = createLabeledNode("Start")

    val b1 = createLabeledNode("A")
    val b2 = createLabeledNode("B")
    val b3 = createLabeledNode("C")

    relate(a, b1)
    relate(a, b2)
    relate(a, b3)

    // WHEN
    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:Start) RETURN (n)-->(:A)")
      .toList.head("(n)-->(:A)").asInstanceOf[Seq[_]]

    result should have size 1
  }

  test("match (a:Start), (b:End) RETURN (a)-[*]->(b) as path") {
    val a = createLabeledNode("Start")
    val b = createLabeledNode("End")
    relate(a, b)

    val resultPath = executeWithAllPlannersAndCompatibilityMode("match (a:Start), (b:End) RETURN (a)-[*]->(b) as path")
      .toList.head("path").asInstanceOf[Seq[_]]

    resultPath should have size 1
  }

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

  test("match (n) return extract(x IN (n)-->() | head(nodes(x)) )  as p") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode("match (n) return extract(x IN (n)-->() | head(nodes(x)) )  as p")

    result.toList.head("p").asInstanceOf[Seq[_]] should equal(List(start, start))
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
          case l: List[Any] => l.toSet
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

  test("match (n)-->(b) with (n)-->() as p, count(b) as c return p, c") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode("match (n)-->(b) with (n)-->() as p, count(b) as c return p, c")

    result.toList.head("p").asInstanceOf[Seq[_]] should have size 2
    result should use("Expand(All)")
  }

  test("match (a:Start), (b:End) with (a)-[*]->(b) as path, count(a) as c return path, c") {
    val a = createLabeledNode("Start")
    val b = createLabeledNode("End")
    relate(a, b)

    val result = executeWithAllPlannersAndCompatibilityMode("match (a:Start), (b:End) with (a)-[*]->(b) as path, count(a) as c return path, c")

    result.toList.head("path").asInstanceOf[Seq[_]] should have size 1
    result should use("VarLengthExpand(Into)")
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

  test("match (n:A) with extract(x IN (n)-->() | head(nodes(x)) ) as p, count(n) as c return p, c") {
    val start = createLabeledNode("A")
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode("match (n:A) with extract(x IN (n)-->() | head(nodes(x)) ) as p, count(n) as c return p, c")
      .toList.head("p").asInstanceOf[Seq[_]]

    result should equal(List(start, start))
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
          case l: List[Any] => l.toSet
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

  test("MATCH (owner) WITH owner, COUNT(*) > 0 AS collected WHERE (owner)--() RETURN *") {
    val a = createNode()
    relate(a, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode(
      """MATCH (owner)
        |WITH owner, COUNT(*) > 0 AS collected
        |WHERE (owner)-->()
        |RETURN owner""".stripMargin)
      .toList

    result should equal(List(
      Map("owner" -> a)
    ))
  }

  test("MATCH (owner) WITH owner, COUNT(*) AS collected WHERE (owner)--() RETURN *") {
    val a = createNode()
    relate(a, createNode())

    val result = executeWithAllPlannersAndCompatibilityMode(
      """MATCH (owner)
        |WITH owner, COUNT(*) AS collected
        |WHERE (owner)-->()
        |RETURN owner""".stripMargin)
      .toList

    result should equal(List(
      Map("owner" -> a)
    ))
  }

  test("MATCH ()-[r]->() WHERE ()-[r]-(:A) RETURN r") {
    graph.inTx {
      (1 to 10000).foreach { i =>
        createLabeledNode("A")
        createNode()
      }
    }

    val r = relate(createNode(), createLabeledNode("A"), "T")

    val result = executeWithAllPlannersAndCompatibilityMode("PROFILE MATCH ()-[r]->() WHERE ()-[r]-(:A) RETURN r")

    result.columnAs[Relationship]("r").toList should equal(List(r))
    result.executionPlanDescription().toString should not include "NodeByLabelScan"
  }

  test("pattern expression should ensure to plan for the unique relationship constraint") {
    val nodeA = createLabeledNode("Foo")
    val nodeB = createLabeledNode("Bar")
    val nodeC = createLabeledNode("Foo")
    val nodeD = createLabeledNode("Foo")
    val nodeE = createLabeledNode("Bar")
    createLabeledNode("Bar")
    createLabeledNode("Bar")
    relate(nodeA, nodeB, "HAS")
    relate(nodeC, nodeB, "HAS")
    relate(nodeD, nodeE, "HAS")

    val query = "PROFILE MATCH (a:Foo) OPTIONAL MATCH (a)--(b:Bar) WHERE (a)--(b:Bar)--() RETURN b"
    val results = executeWithAllPlannersAndCompatibilityMode(query).toList
    results should equal(List(Map("b" -> nodeB), Map("b" -> nodeB), Map("b" -> null)))

    val queryNot = "PROFILE MATCH (a:Foo) OPTIONAL MATCH (a)--(b:Bar) WHERE NOT((a)--(b:Bar)--()) RETURN b"
    val resultsNot = executeWithAllPlannersAndCompatibilityMode(queryNot).toList
    resultsNot should equal(List(Map("b" -> null), Map("b" -> null), Map("b" -> nodeE)))
  }

  test("should consider cardinality input when planning pattern expression in where clause") {
    // given
    val node = createLabeledNode("A")
    createLabeledNode("A")
    createLabeledNode("A")
    relate(node, createNode(), "HAS")

    val result = executeWithAllPlannersAndCompatibilityMode("MATCH (n:A) WHERE (n)-[:HAS]->() RETURN n")

    result.toList should equal(Seq(Map("n" -> node)))
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

    graph.inTx {
      result.toList should equal(Seq(
        Map("p" -> Seq(PathImpl(node, rel, endNode))),
        Map("p" -> Seq()),
        Map("p" -> Seq()))
      )
    }

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

    result.toList should equal(List(Map("c" -> 3)))
    result should use("Expand(All)")
  }

  test("use getDegree for simple pattern expression with length clause, outgoing") {
    val (n1, _) = setup()

    val result = executeWithAllPlanners("MATCH (n:X) WHERE LENGTH((n)-->()) > 2 RETURN n")
    result shouldNot use("RollUpApply")
    result.toList should equal(List(Map("n" -> n1)))
  }

  test("use getDegree for simple pattern expression with length clause, incoming") {
    val (_, n2) = setup()

    val result = executeWithAllPlanners("MATCH (n:X) WHERE LENGTH((n)<--()) > 2 RETURN n")
    result shouldNot use("RollUpApply")
    result.toList should equal(List(Map("n" -> n2)))
  }

  test("use getDegree for simple pattern expression with length clause, both") {
    val (n1, n2) = setup()

    val result = executeWithAllPlanners("MATCH (n:X) WHERE LENGTH((n)--()) > 2 RETURN n")
    result shouldNot use("RollUpApply")
    result.toList should equal(List(Map("n" -> n1), Map("n" -> n2)))
  }

  test("use getDegree for simple pattern expression with rel-type ORs") {
    setup()

    val result = executeWithAllPlanners("MATCH (n) WHERE length((n)-[:X|Y]->()) > 2 RETURN n")
    result shouldNot use("RollUpApply")
    result should be (empty)
  }

  test("match (n:X) return n, EXISTS( (n)--() ) AS b") {
    val n1 = createLabeledNode(Map("prop" -> 42), "X")
    val n2 = createLabeledNode(Map("prop" -> 42), "X")

    relate(n1, createNode())

    val result = executeWithAllPlanners("match (n:X) return n, EXISTS( (n)--() ) AS b")

    result.toList should equal(List(
      Map("n" -> n1, "b" -> true),
      Map("n" -> n2, "b" -> false)))
  }

  test("match (n:X) return n, exists( (n)--() ) AS b, not uppercase") {
    val n1 = createLabeledNode(Map("prop" -> 42), "X")
    val n2 = createLabeledNode(Map("prop" -> 42), "X")

    relate(n1, createNode())

    val result = executeWithAllPlanners("match (n:X) return n, exists( (n)--() ) AS b")

    result.toList should equal(List(
      Map("n" -> n1, "b" -> true),
      Map("n" -> n2, "b" -> false)))
  }

  test("pattern expression inside list comprehension") {
    val n1 = createLabeledNode("X")
    val m1 = createLabeledNode("Y")
    val i1 = createLabeledNode("Y")
    val i2 = createLabeledNode("Y")
    relate(n1, m1)
    relate(m1, i1)
    relate(m1, i2)

    val n2 = createLabeledNode("X")
    val m2 = createNode()
    val i3 = createLabeledNode()
    val i4 = createLabeledNode("Y")

    relate(n2, m2)
    relate(m2, i3)
    relate(m2, i4)

    val result = executeWithAllPlanners("match p = (n:X)-->(b) return n, [x in nodes(p) | length( (x)-->(:Y) ) ] as coll")

    result.toList should equal(List(
      Map("n" -> n1, "coll" -> Seq(1, 2)),
      Map("n" -> n2, "coll" -> Seq(0, 1))))
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
