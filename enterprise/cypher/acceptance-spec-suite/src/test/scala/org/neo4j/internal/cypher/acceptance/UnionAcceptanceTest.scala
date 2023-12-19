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

import org.neo4j.cypher._
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._

class UnionAcceptanceTest extends ExecutionEngineFunSuite with CypherComparisonSupport {

  test("Should work when doing union with same return variables") {
    createLabeledNode(Map("a" -> "a", "b" -> "b"), "A")
    createLabeledNode(Map("a" -> "a", "b" -> "b"), "B")

    val query =
      """
        |MATCH (N:A)
        |RETURN
        |N.a as A,
        |N.b as B
        |UNION
        |MATCH (M:B) RETURN
        |M.b as A,
        |M.a as B
      """.stripMargin

    val result = executeWith(Configs.Interpreted, query)
    val expected = List(Map("A" -> "a", "B" -> "b"), Map("A" -> "b", "B" -> "a"))

    result.toList should equal(expected)
  }

  test("Should work when doing union while mixing nodes and strings") {
    val node = createLabeledNode("A")

    val query1 =
      """
        |MATCH (n)
        |RETURN n AS A
        |UNION
        |RETURN "foo" AS A
      """.stripMargin

    val result1 = executeWith(Configs.Interpreted, query1)
    val expected1 = List(Map("A" -> node), Map("A" -> "foo"))

    result1.toList should equal(expected1)

    val query2 =
      """
        |RETURN "foo" AS A
        |UNION
        |MATCH (n)
        |RETURN n AS A
      """.stripMargin

    val result2 = executeWith(Configs.Interpreted, query2)
    val expected2 = List(Map("A" -> "foo"), Map("A" -> node))

    result2.toList should equal(expected2)
  }

  test("Should work when doing union while mixing nodes and relationships") {
    val node = createLabeledNode("A")
    val rel = relate(node, node, "T")

    val query1 =
      """
        |MATCH (n)
        |RETURN n AS A
        |UNION
        |MATCH ()-[r:T]->()
        |RETURN r AS A
      """.stripMargin

    val result1 = executeWith(Configs.Interpreted, query1)
    val expected1 = List(Map("A" -> node), Map("A" -> rel))

    result1.toList should equal(expected1)

    val query2 =
      """
        |MATCH ()-[r:T]->()
        |RETURN r AS A
        |UNION
        |MATCH (n)
        |RETURN n AS A
      """.stripMargin

    val result2 = executeWith(Configs.Interpreted, query2)
    val expected2 = List(Map("A" -> rel), Map("A" -> node))

    result2.toList should equal(expected2)
  }

  test("Should work when doing union of nodes in permuted order") {
    val a = createLabeledNode("A")
    val b = createLabeledNode("B")

    val query =
      """
        |MATCH (N:A),(M:B)
        |RETURN
        |N, M
        |UNION
        |MATCH (N:B), (M:A) RETURN
        |M, N
      """.stripMargin

    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query)
    val expected = List(Map("M" -> b, "N" -> a), Map("M" -> a, "N" -> b))

    result.toList should equal(expected)
  }

  test("Should work when doing union with permutated return variables") {
    createLabeledNode(Map("a" -> "a", "b" -> "b"), "A")
    createLabeledNode(Map("a" -> "b", "b" -> "a"), "B")

    val query =
      """
        |MATCH (N:A)
        |RETURN
        |N.a as B,
        |N.b as A
        |UNION
        |MATCH (M:B) RETURN
        |M.b as A,
        |M.a as B
      """.stripMargin

    val expectedToWorkIn = Configs.Interpreted - Configs.Cost2_3
    val result = executeWith(expectedToWorkIn, query)
    val expected = List(Map("A" -> "b", "B" -> "a"), Map("A" -> "a", "B" -> "b"))

    result.toList should equal(expected)
  }
}
