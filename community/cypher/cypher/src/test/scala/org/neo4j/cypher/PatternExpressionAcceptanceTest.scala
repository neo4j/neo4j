/**
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
package org.neo4j.cypher

import org.scalatest.Matchers
import org.neo4j.cypher.internal.PathImpl
import org.neo4j.graphdb.Node

class PatternExpressionAcceptanceTest extends ExecutionEngineFunSuite with Matchers with NewPlannerTestSupport {

  test("match (n) return (n)-->()") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithNewPlanner("match (n) return (n)-->() as p").toList
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
    val result = executeWithNewPlanner("MATCH (n:Start) RETURN n-->(:A)")
      .toList.head("n-->(:A)").asInstanceOf[Seq[_]]

    result should have size 1
  }

  test("match (a:Start), (b:End) RETURN a-[*]->b as path") {
    val a = createLabeledNode("Start")
    val b = createLabeledNode("End")
    relate(a, b)

    val resultPath = executeWithNewPlanner("match (a:Start), (b:End) RETURN a-[*]->b as path")
      .toList.head("path").asInstanceOf[Seq[_]]

    resultPath should have size 1
  }

  test("match (n) return case when id(n) >= 0 then (n)-->() otherwise 42 as p") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithNewPlanner("match (n) return case when id(n) >= 0 then (n)-->() else 42 end as p")
      .toList.head("p").asInstanceOf[Seq[_]]

    result should have size 2
  }

  test("match (n) return case when id(n) < 0 then (n)-->() otherwise 42 as p") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithNewPlanner("match (n) return case when id(n) < 0 then (n)-->() else 42 end as p")
      .toList.head("p").asInstanceOf[Long]

    result should equal(42)
  }

  test("match (n) return extract(x IN (n)-->() | head(nodes(x)) )  as p") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithNewPlanner("match (n) return extract(x IN (n)-->() | head(nodes(x)) )  as p")
      .toList.head("p").asInstanceOf[Seq[_]]

    result should equal(List(start,start))
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
      val result = executeWithNewPlanner("match (n) return case when n:A then (n)-->(:C) when n:B then (n)-->(:D) else 42 end as p")
        .toList

      result should equal(List(
        Map("p" -> List(new PathImpl(start, rel1, c), new PathImpl(start, rel2, c))),
        Map("p" -> 42),
        Map("p" -> List(new PathImpl(start2, rel3, d), new PathImpl(start2, rel4, d))),
        Map("p" -> 42)
      ))
    }
  }

  test("match (n)-->(b) with (n)-->() as p, count(b) as c return p, c") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithNewPlanner("match (n)-->(b) with (n)-->() as p, count(b) as c return p, c").toList
      .toList.head("p").asInstanceOf[Seq[_]]

    result should have size 2
  }

  test("match (a:Start), (b:End) with a-[*]->b as path, count(a) as c return path, c") {
    val a = createLabeledNode("Start")
    val b = createLabeledNode("End")
    relate(a, b)

    val resultPath = executeWithNewPlanner("match (a:Start), (b:End) with a-[*]->b as path, count(a) as c return path, c")
      .toList.head("path").asInstanceOf[Seq[_]]

    resultPath should have size 1
  }

  test("match (n) with case when id(n) >= 0 then (n)-->() else 42 end as p, count(n) as c return p, c") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithNewPlanner("match (n) with case when id(n) >= 0 then (n)-->() else 42 end as p, count(n) as c return p, c")
      .toList.head("p").asInstanceOf[Seq[_]]

    result should have size 2
  }

  test("match (n) with case when id(n) < 0 then (n)-->() else 42 end as p, count(n) as c return p, c") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithNewPlanner("match (n) with case when id(n) < 0 then (n)-->() else 42 end as p, count(n) as c return p, c")
      .toList.head("p").asInstanceOf[Long]

    result should equal(42)
  }

  test("match (n:A) with extract(x IN (n)-->() | head(nodes(x)) ) as p, count(n) as c return p, c") {
    val start = createLabeledNode("A")
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithNewPlanner("match (n:A) with extract(x IN (n)-->() | head(nodes(x)) ) as p, count(n) as c return p, c")
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
      val result = executeWithNewPlanner("match (n) with case when n:A then (n)-->(:C) when n:B then (n)-->(:D) else 42 end as p, count(n) as c return p, c")
        .toList

      result should equal(List(
        Map("c" -> 1, "p" -> List(new PathImpl(start, rel1, c), new PathImpl(start, rel2, c))),
        Map("c" -> 1, "p" -> List(new PathImpl(start2, rel3, d), new PathImpl(start2, rel4, d))),
        Map("c" -> 2, "p" -> 42)
      ))
    }
  }

  test("match (n) where (case when id(n) >= 0 then length((n)-->()) else 42 end) > 0 return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithNewPlanner("match (n) where (case when id(n) >= 0 then length((n)-->()) else 42 end) > 0 return n")
      .toList

    result should equal(List(
      Map("n" -> start)
    ))
  }


  test("match (n) where (case when id(n) < 0 then length((n)-->()) else 42 end) > 0 return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithNewPlanner("match (n) where (case when id(n) < 0 then length((n)-->()) else 42 end) > 0 return n")
      .toList

    result should have size 3
  }

  test("match (n) where n IN extract(x IN (n)-->() | head(nodes(x)) ) return n") {
    val start = createNode()
    relate(start, createNode())
    relate(start, createNode())

    val result = executeWithNewPlanner("match (n) where n IN extract(x IN (n)-->() | head(nodes(x)) ) return n")
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
      val result = executeWithNewPlanner("match (n) where (n)-->() AND (case when n:A then length((n)-->(:C)) when n:B then length((n)-->(:D)) else 42 end) > 1 return n")
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

    val result = executeWithNewPlanner(
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

    val result = executeWithNewPlanner(
      """MATCH (owner)
        |WITH owner, COUNT(*) AS collected
        |WHERE (owner)-->()
        |RETURN owner""".stripMargin)
      .toList

    result should equal(List(
      Map("owner" -> a)
    ))
  }

}
