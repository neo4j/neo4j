/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.v3_4.expressions.SemanticDirection
import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.cypher.internal.runtime.PathImpl
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription.Arguments.{EstimatedRows, ExpandExpression}
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._
import org.scalatest.Matchers

class PatternExpressionImplementationAcceptanceTest extends ExecutionEngineFunSuite with Matchers with CypherComparisonSupport {

  // TESTS WITH CASE EXPRESSION

  test("match (n) return case when id(n) >= 0 then (n)-->() otherwise 42 as p") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.Interpreted, "match (n) return case when id(n) >= 0 then (n)-->() else 42 end as p",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot useOperators("Expand(All)")))

    result.toList.head("p").asInstanceOf[Seq[_]] should have size 2
  }

  test("match (n) return case when id(n) < 0 then (n)-->() otherwise 42 as p") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.Interpreted, "match (n) return case when id(n) < 0 then (n)-->() else 42 end as p",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot useOperators("Expand(All)")))

    result.toList.head("p").asInstanceOf[Long] should equal(42)
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

    val result = executeWith(Configs.Interpreted, "match (n) return case when n:A then (n)-->(:C) when n:B then (n)-->(:D) else 42 end as p")
      .map(_.mapValues {
        case l: Seq[Any] => l.toSet
        case x => x
      }).toSet

    result should equal(Set(
      Map("p" -> Set(PathImpl(start, rel2, c), PathImpl(start, rel1, c))),
      Map("p" -> 42),
      Map("p" -> Set(PathImpl(start2, rel4, d), PathImpl(start2, rel3, d))),
      Map("p" -> 42)
    ))
  }

  test("match (n) with case when id(n) >= 0 then (n)-->() else 42 end as p, count(n) as c return p, c") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.Interpreted,
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

    val result = executeWith(Configs.Interpreted, "match (n) with case when id(n) < 0 then (n)-->() else 42 end as p, count(n) as c return p, c")
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

    val result = executeWith(Configs.Interpreted, "match (n) with case when n:A then (n)-->(:C) when n:B then (n)-->(:D) else 42 end as p, count(n) as c return p, c")
      .map(_.mapValues {
        case l: Seq[Any] => l.toSet
        case x => x
      }).toSet

    result should equal(Set(
      Map("c" -> 1, "p" -> Set(PathImpl(start, rel2, c), PathImpl(start, rel1, c))),
      Map("c" -> 1, "p" -> Set(PathImpl(start2, rel4, d), PathImpl(start2, rel3, d))),
      Map("c" -> 2, "p" -> 42)
    ))
  }

  test("match (n) where (case when id(n) >= 0 then length((n)-->()) else 42 end) > 0 return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.Interpreted, "match (n) where (case when id(n) >= 0 then length((n)-->()) else 42 end) > 0 return n",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot useOperators("RollUpApply")))

    result.toList should equal(List(
      Map("n" -> start)
    ))
  }

  test("match (n) where (case when id(n) < 0 then length((n)-->()) else 42 end) > 0 return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.Interpreted, "match (n) where (case when id(n) < 0 then length((n)-->()) else 42 end) > 0 return n")

    result should have size 3
  }

  test("match (n) where (case when id(n) < 0 then length((n)-[:X]->()) else 42 end) > 0 return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.Interpreted, "match (n) where (case when id(n) < 0 then length((n)-[:X]->()) else 42 end) > 0 return n")

    result should have size 3
  }

  test("match (n) where (case when id(n) < 0 then length((n)-->(:X)) else 42 end) > 0 return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.Interpreted, "match (n) where (case when id(n) < 0 then length((n)-->(:X)) else 42 end) > 0 return n",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot useOperators("RollUpApply")))

    result should have size 3
  }

  test("match (n) where (case when n:A then length((n)-->(:C)) when n:B then length((n)-->(:D)) else 42 end) > 1 return n") {
    val start = createLabeledNode("A")
    relate(start, createLabeledNode("C"))
    relate(start, createLabeledNode("C"))
    val start2 = createLabeledNode("B")
    relate(start2, createLabeledNode("D"))
    val start3 = createNode()
    relate(start3, createNode())

    val result = executeWith(Configs.Interpreted, "match (n) where (n)-->() AND (case when n:A then length((n)-->(:C)) when n:B then length((n)-->(:D)) else 42 end) > 1 return n",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot useOperators("RollUpApply")))

    result.toList should equal(List(
      Map("n" -> start),
      Map("n" -> start3)
    ))
  }

  test("MATCH (n:FOO) WITH n, COLLECT(DISTINCT { res:CASE WHEN EXISTS ((n)-[:BAR*]->()) THEN 42 END }) as x RETURN n, x") {
    val node1 = createLabeledNode("FOO")
    val node2 = createNode()
    relate(node1, node2, "BAR")
    val result = executeWith(Configs.Interpreted - Configs.Cost3_1,
      """
        |MATCH (n:FOO)
        |WITH n, COLLECT (DISTINCT{
        |res:CASE WHEN EXISTS((n)-[:BAR*]->()) THEN 42 END
        |}) as x RETURN n, x
      """.stripMargin
    )

    result.toList should equal(List(Map("n" -> node1, "x" -> List(Map("res" -> 42)))))
  }

  test("case expressions and pattern expressions") {
    val n1 = createLabeledNode(Map("prop" -> 42), "A")

    relate(n1, createNode())
    relate(n1, createNode())
    relate(n1, createNode())

    val result = executeWith(Configs.Interpreted,
      """match (a:A)
        |return case
        |         WHEN a.prop = 42 THEN []
        |         ELSE (a)-->()
        |       END as X
        |         """.stripMargin,
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot useOperators("RollUpApply")))

    result.toList should equal(List(Map("X" -> Seq())))
  }

  test("should not use full expand 1") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    executeWith(Configs.Interpreted, "match (n) return case when id(n) >= 0 then (n)-->() else 42 end as p",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot useOperators("Expand(All)")))
  }

  test("should not use full expand 2") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    executeWith(Configs.Interpreted, "match (n) return case when id(n) < 0 then (n)-->() else 42 end as p",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot useOperators("Expand(All)")))
  }

  // TESTS WITH EXTRACT

  test("match (n) return extract(x IN (n)-->() | head(nodes(x)) )  as p") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.Interpreted, "match (n) return extract(x IN (n)-->() | head(nodes(x)) )  as p")

    result.toList.head("p").asInstanceOf[Seq[_]] should equal(List(start, start))
  }

  test("match (n:A) with extract(x IN (n)-->() | head(nodes(x)) ) as p, count(n) as c return p, c") {
    val start = createLabeledNode("A")
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.Interpreted, "match (n:A) with extract(x IN (n)-->() | head(nodes(x)) ) as p, count(n) as c return p, c")
      .toList.head("p").asInstanceOf[Seq[_]]

    result should equal(List(start, start))
  }

  test("match (n) where n IN extract(x IN (n)-->() | head(nodes(x)) ) return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWith(Configs.Interpreted, "match (n) where n IN extract(x IN (n)-->() | head(nodes(x)) ) return n")
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

    executeWith(Configs.Interpreted, "match (n)-->(b) with (n)-->() as p, count(b) as c return p, c",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("Expand(All)"), expectPlansToFail = Configs.AllRulePlanners))
  }

  test("should use varlength expandInto when variables are bound") {
    val a = createLabeledNode("Start")
    val b = createLabeledNode("End")
    relate(a, b)

    executeWith(Configs.Interpreted, "match (a:Start), (b:End) with (a)-[*]->(b) as path, count(a) as c return path, c",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("VarLengthExpand(Into)"),
        expectPlansToFail = Configs.AllRulePlanners + Configs.Version2_3))
  }

  // FAIL: <default version> <default planner> runtime=slotted returned different results than <default version> <default planner> runtime=interpreted List() did not contain the same elements as List(Map("r" -> (20000)-[T,0]->(20001)))
  test("should not use a label scan as starting point when statistics are bad") {
    graph.inTx {
      (1 to 10000).foreach { i =>
        createLabeledNode("A")
        createNode()
      }
    }
    relate(createNode(), createLabeledNode("A"), "T")

    executeWith(Configs.Interpreted, "PROFILE MATCH ()-[r]->() WHERE ()-[r]-(:A) RETURN r",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot useOperators("NodeByLabelScan")))
  }

  test("should consider cardinality input when planning pattern expression in where clause") {
    // given
    val node = createLabeledNode("A")
    createLabeledNode("A")
    createLabeledNode("A")
    relate(node, createNode(), "HAS")

    val result = executeWith(Configs.Interpreted, "MATCH (n:A) WHERE (n)-[:HAS]->() RETURN n")

    val argumentPLan = result.executionPlanDescription().cd("NodeByLabelScan")
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

    executeWith(Configs.Interpreted, "MATCH (n:A) RETURN (n)-[:HAS]->() as p",
      planComparisonStrategy = ComparePlansWithAssertion((planDescription) => {
        planDescription.find("Argument") shouldNot be(empty)
        planDescription.cd("Argument").arguments should equal(List(EstimatedRows(1)))
        planDescription.find("Expand(All)") shouldNot be(empty)
        val expandArgs = planDescription.cd("Expand(All)").arguments.toSet
        expandArgs should contain(EstimatedRows(0.25))
        expandArgs collect {
          case ExpandExpression("n", _, Seq("HAS"), _, SemanticDirection.OUTGOING, 1, Some(1)) => true
        } should not be empty
      }, Configs.AllRulePlanners + Configs.Version2_3))
  }

  test("should be able to execute aggregating-functions on pattern expressions") {
    // given
    val node = createLabeledNode("A")
    createLabeledNode("A")
    createLabeledNode("A")
    relate(node, createNode(), "HAS")

    executeWith(Configs.Interpreted, "MATCH (n:A) RETURN count((n)-[:HAS]->()) as c",
      planComparisonStrategy = ComparePlansWithAssertion(_ should useOperators("Expand(All)"),
        expectPlansToFail = Configs.AllRulePlanners + Configs.Version2_3))
  }

  test("use getDegree for simple pattern expression with length clause, outgoing") {
    setup()

    executeWith(Configs.Interpreted, "MATCH (n:X) WHERE LENGTH((n)-->()) > 2 RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot useOperators("RollUpApply")))
  }

  test("use getDegree for simple pattern expression with length clause, incoming") {
    setup()

    executeWith(Configs.Interpreted, "MATCH (n:X) WHERE LENGTH((n)<--()) > 2 RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot useOperators("RollUpApply")))
  }

  test("use getDegree for simple pattern expression with length clause, both") {
    setup()

    executeWith(Configs.Interpreted, "MATCH (n:X) WHERE LENGTH((n)--()) > 2 RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot useOperators("RollUpApply")))
  }

  test("use getDegree for simple pattern expression with rel-type ORs") {
    setup()

    executeWith(Configs.Interpreted, "MATCH (n) WHERE length((n)-[:X|Y]->()) > 2 RETURN n",
      planComparisonStrategy = ComparePlansWithAssertion(_ shouldNot useOperators("RollUpApply")))
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
