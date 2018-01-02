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
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{NestedPipeExpression, PathImpl}
import org.neo4j.cypher.internal.compiler.v2_3.pipes.{ArgumentPipe, ExpandAllPipe, LazyTypes}
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription.Arguments.{EstimatedRows, LegacyExpression}
import org.neo4j.cypher.internal.compiler.v2_3.planner.BeLikeMatcher._
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.graphdb.Relationship
import org.scalatest.Matchers

class PatternExpressionAcceptanceTest extends ExecutionEngineFunSuite with Matchers with NewPlannerTestSupport {

  test("match (n) return (n)-->()") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlanners("match (n) return (n)-->() as p").toList
      .toList.head("p").asInstanceOf[Seq[_]]

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
    val result = executeWithAllPlanners("MATCH (n:Start) RETURN n-->(:A)")
      .toList.head("n-->(:A)").asInstanceOf[Seq[_]]

    result should have size 1
  }

  test("match (a:Start), (b:End) RETURN a-[*]->b as path") {
    val a = createLabeledNode("Start")
    val b = createLabeledNode("End")
    relate(a, b)

    val resultPath = executeWithAllPlanners("match (a:Start), (b:End) RETURN a-[*]->b as path")
      .toList.head("path").asInstanceOf[Seq[_]]

    resultPath should have size 1
  }

  test("match (n) return case when id(n) >= 0 then (n)-->() otherwise 42 as p") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlanners("match (n) return case when id(n) >= 0 then (n)-->() else 42 end as p")
      .toList.head("p").asInstanceOf[Seq[_]]

    result should have size 2
  }

  test("match (n) return case when id(n) < 0 then (n)-->() otherwise 42 as p") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlanners("match (n) return case when id(n) < 0 then (n)-->() else 42 end as p")
      .toList.head("p").asInstanceOf[Long]

    result should equal(42)
  }

  test("match (n) return extract(x IN (n)-->() | head(nodes(x)) )  as p") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlanners("match (n) return extract(x IN (n)-->() | head(nodes(x)) )  as p")
      .toList.head("p").asInstanceOf[Seq[_]]

    result should equal(List(start, start))
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
      val result = executeWithAllPlanners("match (n) return case when n:A then (n)-->(:C) when n:B then (n)-->(:D) else 42 end as p")
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

    val result = executeWithAllPlanners("match (n)-->(b) with (n)-->() as p, count(b) as c return p, c").toList
      .toList.head("p").asInstanceOf[Seq[_]]

    result should have size 2
  }

  test("match (a:Start), (b:End) with a-[*]->b as path, count(a) as c return path, c") {
    val a = createLabeledNode("Start")
    val b = createLabeledNode("End")
    relate(a, b)

    val resultPath = executeWithAllPlanners("match (a:Start), (b:End) with a-[*]->b as path, count(a) as c return path, c")
      .toList.head("path").asInstanceOf[Seq[_]]

    resultPath should have size 1
  }

  test("match (n) with case when id(n) >= 0 then (n)-->() else 42 end as p, count(n) as c return p, c") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlanners("match (n) with case when id(n) >= 0 then (n)-->() else 42 end as p, count(n) as c return p, c")
      .toList.head("p").asInstanceOf[Seq[_]]

    result should have size 2
  }

  test("match (n) with case when id(n) < 0 then (n)-->() else 42 end as p, count(n) as c return p, c") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlanners("match (n) with case when id(n) < 0 then (n)-->() else 42 end as p, count(n) as c return p, c")
      .toList.head("p").asInstanceOf[Long]

    result should equal(42)
  }

  test("match (n:A) with extract(x IN (n)-->() | head(nodes(x)) ) as p, count(n) as c return p, c") {
    val start = createLabeledNode("A")
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlanners("match (n:A) with extract(x IN (n)-->() | head(nodes(x)) ) as p, count(n) as c return p, c")
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
      val result = executeWithAllPlanners("match (n) with case when n:A then (n)-->(:C) when n:B then (n)-->(:D) else 42 end as p, count(n) as c return p, c")
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

    val result = executeWithAllPlanners("match (n) where (case when id(n) >= 0 then length((n)-->()) else 42 end) > 0 return n")
      .toList

    result should equal(List(
      Map("n" -> start)
    ))
  }


  test("match (n) where (case when id(n) < 0 then length((n)-->()) else 42 end) > 0 return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlanners("match (n) where (case when id(n) < 0 then length((n)-->()) else 42 end) > 0 return n")
      .toList

    result should have size 3
  }

  test("match (n) where n IN extract(x IN (n)-->() | head(nodes(x)) ) return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithAllPlanners("match (n) where n IN extract(x IN (n)-->() | head(nodes(x)) ) return n")
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
      val result = executeWithAllPlanners("match (n) where (n)-->() AND (case when n:A then length((n)-->(:C)) when n:B then length((n)-->(:D)) else 42 end) > 1 return n")
        .toList

      result should equal(List(
        Map("n" -> start),
        Map("n" -> start3)
      ))
    }
  }

  test("MATCH (owner) WITH owner, COUNT(*) > 0 AS collected WHERE (owner)--() RETURN *") {
    val a = createNode()
    relate(a, createNode())

    val result = executeWithAllPlanners(
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

    val result = executeWithAllPlanners(
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

    val result = executeWithAllPlanners("PROFILE MATCH ()-[r]->() WHERE ()-[r]-(:A) RETURN r")

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

    val query = "PROFILE MATCH (a:Foo) OPTIONAL MATCH a--(b:Bar) WHERE a--(b:Bar)--() RETURN b"
    val results = executeWithAllPlanners(query).toList
    results should equal(List(Map("b" -> nodeB), Map("b" -> nodeB), Map("b" -> null)))

    val queryNot = "PROFILE MATCH (a:Foo) OPTIONAL MATCH a--(b:Bar) WHERE NOT(a--(b:Bar)--()) RETURN b"
    val resultsNot = executeWithAllPlanners(queryNot).toList
    resultsNot should equal(List(Map("b" -> null), Map("b" -> null), Map("b" -> nodeE)))
  }

  test("should consider cardinality input when planning pattern expression in where clause") {
    // given
    val node = createLabeledNode("A")
    createLabeledNode("A")
    createLabeledNode("A")
    relate(node, createNode(), "HAS")

    val result = executeWithAllPlanners("MATCH (n:A) WHERE (n)-[:HAS]->() RETURN n")

    result.toList should equal(Seq(Map("n" -> node)))
    val argumentPLan = result.executionPlanDescription().cd("Argument")
    val estimatedRows = argumentPLan.arguments.collect {case n : EstimatedRows => n}.head
    estimatedRows should equal(EstimatedRows(3.0))
  }

  test("should consider cardinality input when planning in return") {
    // given
    val node = createLabeledNode("A")
    createLabeledNode("A")
    createLabeledNode("A")
    val endNode = createNode()
    val rel = relate(node, endNode, "HAS")

    val result = executeWithAllPlanners("MATCH (n:A) RETURN (n)-[:HAS]->() as p")

    graph.inTx {
      result.toList should equal(Seq(
        Map("p" -> Seq(PathImpl(node, rel, endNode))),
        Map("p" -> Seq()),
        Map("p" -> Seq()))
      )
    }

    val args = result.executionPlanDescription().children.head.arguments
    val legacyExpression = args.collect {case n: LegacyExpression => n}.head
    val pipe = legacyExpression.value.asInstanceOf[NestedPipeExpression].pipe

    pipe should beLike {
      case ExpandAllPipe(_: ArgumentPipe, "n", _, _, SemanticDirection.OUTGOING, LazyTypes(List("HAS"))) => ()
    }

    pipe.sources.head.asInstanceOf[ArgumentPipe].estimatedCardinality should equal(Some(3.0))
  }

  test("should be able to execute aggregating-functions on pattern expressions") {
    // given
    val node = createLabeledNode("A")
    createLabeledNode("A")
    createLabeledNode("A")
    relate(node, createNode(), "HAS")

    val result = executeWithAllPlanners("MATCH (n:A) RETURN count((n)-[:HAS]->()) as c").toList

    result should equal(List(Map("c" -> 3)))
  }
}
