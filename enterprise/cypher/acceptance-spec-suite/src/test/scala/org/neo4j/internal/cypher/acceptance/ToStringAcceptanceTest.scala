/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.scalatest.Matchers

class ToStringAcceptanceTest extends ExecutionEngineFunSuite with Matchers {

  test("Node should provide sensible toString") {
    val data = makeModel()
    val result = graph.execute("MATCH (a:A) RETURN a")
    result.columnAs("a").next().toString should be(s"Node[${data("a")}]")
  }

  test("Node should provide sensible toString within transactions") {
    val data = makeModel()
    graph.inTx {
      val result = graph.execute("MATCH (a:A) RETURN a")
      result.columnAs("a").next().toString should be(s"Node[${data("a")}]")
    }
  }

  test("Node collection should provide sensible toString") {
    makeModel()
    val result = graph.execute("MATCH (a) RETURN collect(a) as nodes")
    result.columnAs("nodes").next().toString should fullyMatch regex "\\[Node\\[\\d+\\], Node\\[\\d+\\], Node\\[\\d+\\]\\]"
  }

  test("Relationship should provide sensible toString") {
    val data = makeModel()
    val result = graph.execute("MATCH (:A)-[r]->(:B) RETURN r")
    result.columnAs("r").next().toString should be(s"(?)-[RELTYPE(0),${data("ab")}]->(?)")
  }

  test("Relationship should provide sensible toString within transactions") {
    val data = makeModel()
    graph.inTx {
      val result = graph.execute("MATCH (:A)-[r]->(:B) RETURN r")
      result.columnAs("r").next().toString should be(s"(${data("a")})-[KNOWS,${data("ab")}]->(${data("b")})")
    }
  }

  test("Path should provide sensible toString") {
    val data = makeModel()
    val result = graph.execute("MATCH p = (:A)-->(:B)-->(:C) RETURN p")
    result.columnAs("p").next().toString should (
      be(s"(?)-[?,${data("ab")}]-(?)-[?,${data("bc")}]-(?)") or
        be(s"(${data("a")})-[${data("ab")}:KNOWS]->(${data("b")})-[${data("bc")}:KNOWS]->(${data("c")})"))
  }

  test("Path should provide sensible toString within transactions") {
    val data = makeModel()
    graph.inTx {
      val result = graph.execute("MATCH p = (:A)-->(:B)-->(:C) RETURN p")
      result.columnAs("p").next().toString should (
        be(s"(${data("a")})-[KNOWS,${data("ab")}]->(${data("b")})-[KNOWS,${data("bc")}]->(${data("c")})") or
          be(s"(${data("a")})-[${data("ab")}:KNOWS]->(${data("b")})-[${data("bc")}:KNOWS]->(${data("c")})"))
    }
  }

  private def makeModel() = {
    val a = createLabeledNode("A")
    val b = createLabeledNode("B")
    val c = createLabeledNode("C")
    val ab = relate(a, b, "KNOWS")
    val bc = relate(b, c, "KNOWS")
    Map("a" -> a.getId, "b" -> b.getId, "c" -> c.getId, "ab" -> ab.getId, "bc" -> bc.getId)
  }
}
