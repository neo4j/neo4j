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

import org.neo4j.graphdb.Node
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.util.matching.Regex

class EagerizationAcceptanceTest extends ExecutionEngineFunSuite
  with TableDrivenPropertyChecks
  with QueryStatisticsTestSupport {

  val EagerRegEx: Regex = "Eager(?!A)".r

  test("should plan eagerness for delete on paths") {
    val node0 = createLabeledNode("L")
    val node1 = createLabeledNode("L")
    relate(node0, node1)

    val query = "MATCH p=(:L)-[*]-() DELETE p"

    assertNumberOfEagerness(query, 1)
  }

  test("should plan eagerness for detach delete on paths") {
    val node0 = createLabeledNode("L")
    val node1 = createLabeledNode("L")
    relate(node0, node1)

    val query = "MATCH p=(:L)-[*]-() DETACH DELETE p"

    assertNumberOfEagerness(query, 1)
  }

  test("github issue ##5653") {
    assertNumberOfEagerness(
      "MATCH (p1:Person {name:'Michal'})-[r:FRIEND_OF {since:2007}]->(p2:Person {name:'Daniela'}) DELETE r, p1, p2", 1)
  }

  test("should introduce eagerness between DELETE and MERGE for node") {
    val query =
      """
        |MATCH (b:B)
        |DELETE b
        |MERGE (b2:B { value: 1 })
        |RETURN b2
      """.stripMargin

    assertNumberOfEagerness(query, 2)
  }

  test("should introduce eagerness between DELETE and MERGE for relationship") {
    val query =
      """
        |MATCH (a)-[t:T]->(b)
        |DELETE t
        |MERGE (a)-[t2:T]->(b)
        |RETURN t2
      """.stripMargin

    assertNumberOfEagerness(query, 2)
  }

  test("should not introduce eagerness for MATCH nodes and CREATE relationships") {
    val query = "MATCH a, b CREATE (a)-[:KNOWS]->(b)"

    assertNumberOfEagerness(query, 0)
  }

  test("should introduce eagerness when doing first matching and then creating nodes") {
    val query = "MATCH a CREATE (b)"

    assertNumberOfEagerness(query, 1)
  }

  test("should not introduce eagerness for MATCH nodes and CREATE UNIQUE relationships") {
    val query = "MATCH a, b CREATE UNIQUE (a)-[r:KNOWS]->(b)"

    assertNumberOfEagerness(query, 0)
  }

  test("should not introduce eagerness for MATCH nodes and MERGE relationships") {
    val query = "MATCH a, b MERGE (a)-[r:KNOWS]->(b)"

    assertNumberOfEagerness(query, 0)
  }

  test("should not add eagerness when not writing to nodes") {
    val query = "MATCH a, b CREATE (a)-[r:KNOWS]->(b) SET r = { key: 42 }"

    assertNumberOfEagerness(query, 0)
  }

  test("should not introduce eagerness when the ON MATCH includes writing to a non-matched property") {
    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON MATCH SET a.prop = 42"

    assertNumberOfEagerness(query, 0)
  }

  test("should introduce eagerness when the ON MATCH includes writing to a matched label") {
    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON MATCH SET b:Foo"

    assertNumberOfEagerness(query, 1)
  }

  test("should understand symbols introduced by FOREACH") {
    val query =
      """MATCH (a:Label)
        |WITH collect(a) as nodes
        |MATCH (b:Label2)
        |FOREACH(n in nodes |
        |  CREATE UNIQUE (n)-[:SELF]->(b))""".stripMargin

    assertNumberOfEagerness(query, 0)
  }

  test("LOAD CSV FROM 'file:///something' AS line MERGE (b:B {p:line[0]}) RETURN b") {
    val query = "LOAD CSV FROM 'file:///something' AS line MERGE (b:B {p:line[0]}) RETURN b"

    assertNumberOfEagerness(query, 0)
  }

  test("MATCH (a:Person),(m:Movie) OPTIONAL MATCH (a)-[r1]-(), (m)-[r2]-() DELETE a,r1,m,r2") {
    val query = "MATCH (a:Person),(m:Movie) OPTIONAL MATCH (a)-[r1]-(), (m)-[r2]-() DELETE a,r1,m,r2"

    assertNumberOfEagerness(query, 1)
  }

  test("MATCH (a:Person),(m:Movie) CREATE (a)-[:T]->(m) WITH a OPTIONAL MATCH (a) RETURN *") {
    val query = "MATCH (a:Person),(m:Movie) CREATE (a)-[:T]->(m) WITH a OPTIONAL MATCH (a) RETURN *"

    assertNumberOfEagerness(query, 0)
  }

  test("should not add eagerness when reading and merging nodes and relationships when matching different label") {
    val query = "MATCH (a:A) MERGE (a)-[:BAR]->(b:B) WITH a MATCH (a) WHERE (a)-[:FOO]->() RETURN a"

    assertNumberOfEagerness(query, 0)
  }

  test("should add eagerness when reading and merging nodes and relationships on matching same label") {
    val query = "MATCH (a:A) MERGE (a)-[:BAR]->(b:A) WITH a MATCH (a) WHERE (a)-[:FOO]->() RETURN a"

    assertNumberOfEagerness(query, 1)
  }

  test("should not add eagerness when reading nodes and merging relationships") {
    val query = "MATCH (a:A), (b:B) MERGE (a)-[:BAR]->(b) WITH a MATCH (a) WHERE (a)-[:FOO]->() RETURN a"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property and writing different property should not be eager") {
    val query = "MATCH (n:Node {prop:5}) SET n.value = 10"

    assertNumberOfEagerness(query, 0)
  }

  test("matching label and writing different label should not be eager") {
    val query = "MATCH (n:Node) SET n:Lol"

    assertNumberOfEagerness(query, 0)
  }

  test("matching label and writing same label should be eager") {
    val query = "MATCH (n:Lol) SET n:Lol"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property and writing label should not be eager") {
    val query = "MATCH (n {name : 'thing'}) SET n:Lol"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property and also matching label, and then writing label should be eager") {
    val query = "MATCH (a:Lol) MATCH (n {name : 'thing'}) SET n:Lol"

    assertNumberOfEagerness(query, 1)
  }

  test("matching label and writing property should not be eager") {
    val query = "MATCH (n:Lol) SET n.name = 'thing'"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property and writing property should be eager") {
    val query = "MATCH (n:Node {prop:5}) SET n.prop = 10"

    assertNumberOfEagerness(query, 1)
  }

  test("writing property without matching should not be eager") {
    val query = "MATCH n SET n.prop = 5"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property via index and writing same property should be eager") {
    execute("CREATE CONSTRAINT ON (book:Book) ASSERT book.isbn IS UNIQUE")
    execute("CREATE (b:Book {isbn : '123'})")

    val query = "MATCH (b :Book {isbn : '123'}) SET b.isbn = '456'"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using AND and writing to same property should be eager") {
    val query = "MATCH n WHERE n.prop1 = 10 AND n.prop2 = 10 SET n.prop1 = 5"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using AND and writing to different property should not be eager") {
    val query = "MATCH n WHERE n.prop1 = 10 AND n.prop2 = 10 SET n.prop3 = 5"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using OR and writing to same property should be eager") {
    val query = "MATCH n WHERE n.prop1 = 10 OR n.prop2 = 10 SET n.prop1 = 5"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using OR and writing to different property should not be eager") {
    val query = "MATCH n WHERE n.prop1 = 10 OR n.prop2 = 10 SET n.prop3 = 5"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using XOR and writing to same property should be eager") {
    val query = "MATCH n WHERE n.prop1 XOR n.prop2 SET n.prop1 = 5"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using XOR and writing to different property should not be eager") {
    val query = "MATCH n WHERE n.prop1 XOR n.prop2 SET n.prop3 = 5"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using NOT and writing to same property should be eager") {
    val query = "MATCH n WHERE NOT(n.prop1 = 42) SET n.prop1 = 5"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using NOT and writing to different property should not be eager") {
    val query = "MATCH n WHERE NOT(n.prop1 = 42) SET n.prop3 = 5"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using COALESCE and writing should be eager") {
    val query = "MATCH n WHERE COALESCE(n.prop, 2) = 1 SET n.prop = 3"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using COALESCE and not writing should not be eager") {
    val query = "MATCH n WHERE COALESCE(n.prop, 2) = 1 RETURN n"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using IN and writing should be eager") {
    val query = "MATCH n WHERE n.prop IN [1] SET n.prop = 5"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using IN and not writing should not be eager") {
    val query = "MATCH n WHERE n.prop IN [1] RETURN n"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using Collection and writing should be eager") {
    val query = "MATCH n WHERE [n.prop] = [1] SET n.prop = 5"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using Collection and not writing should not be eager") {
    val query = "MATCH n WHERE [n.prop] = [1] RETURN n"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using CollectionIndex and writing should be eager") {
    val query = "MATCH n WHERE [n.prop][0] = 1 SET n.prop = 5"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using CollectionIndex and not writing should not be eager") {
    val query = "MATCH n WHERE [n.prop][0] = 1 RETURN n"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using CollectionSlice and writing should be eager") {
    val query = "MATCH n WHERE [n.prop1, n.prop2][0..1] = [1, 1] SET n.prop1 = 5"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using CollectionSlice and not writing should not be eager") {
    val query = "MATCH n WHERE [n.prop1, n.prop2][0..1] = [1, 1] RETURN n"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using EXTRACT and writing should be eager") {
    val query = "MATCH path=(n)-->(m) WHERE extract(x IN nodes(path) | x.prop) = [] SET n.prop = 5"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using EXTRACT and not writing should not be eager") {
    val query = "MATCH path=(n)-->(m) WHERE extract(x IN nodes(path) | x.prop) = [] RETURN n"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using REDUCE and writing should be eager") {
    val query = "MATCH path=(n)-->(m) WHERE reduce(s = 0, x IN nodes(path) | s + x.prop) = 99 SET n.prop = 5"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using REDUCE and not writing should not be eager") {
    val query = "MATCH path=(n)-->(m) WHERE reduce(s = 0, x IN nodes(path) | s + x.prop) = 99 RETURN n"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using FILTER and writing should be eager") {
    val query = "MATCH path=(n)-->(m) WHERE filter(x IN nodes(path) WHERE x.prop = 4) = [] SET n.prop = 10"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using FILTER and not writing should not be eager") {
    val query = "MATCH path=(n)-->(m) WHERE filter(x IN nodes(path) WHERE x.prop = 4) = [] RETURN n"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using KEYS and writing should be eager") {
    val query = "MATCH n WHERE keys(n) = [] SET n.prop = 5"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using KEYS and not writing should not be eager") {
    val query = "MATCH n WHERE keys(n) = [] RETURN n"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using HAS and writing should be eager") {
    val query = "MATCH n WHERE has(n.prop) SET n.prop = 5"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using HAS and not writing should not be eager") {
    val query = "MATCH n WHERE has(n.prop) RETURN n"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using RegEx and writing should be eager") {
    val query = "MATCH n WHERE n.prop =~ 'Foo*' SET n.prop = 'bar'"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using RegEx and not writing should not be eager") {
    val query = "MATCH n WHERE n.prop =~ 'Foo*' RETURN n"

    assertNumberOfEagerness(query, 0)
  }

  test("matching all nodes using LABELS and writing should not be eager") {
    val query = "MATCH n WHERE labels(n) = [] SET n:Lol"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using LABELS and not writing should not be eager") {
    val query = "MATCH n WHERE labels(n) = [] RETURN n"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using REPLACE and writing should be eager") {
    val query = "MATCH n WHERE replace(n.prop, 'foo', 'bar') = 'baz' SET n.prop = 'qux'"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using REPLACE and not writing should not be eager") {
    val query = "MATCH n WHERE replace(n.prop, 'foo', 'bar') = 'baz' RETURN n"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using SUBSTRING and writing should be eager") {
    val query = "MATCH n WHERE substring(n.prop, 3, 5) = 'foo' SET n.prop = 'bar'"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using SUBSTRING and not writing should not be eager") {
    val query = "MATCH n WHERE substring(n.prop, 3, 5) = 'foo' RETURN n"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using LEFT and writing should be eager") {
    val query = "MATCH n WHERE left(n.prop, 5) = 'foo' SET n.prop = 'bar'"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using LEFT and not writing should not be eager") {
    val query = "MATCH n WHERE left(n.prop, 5) = 'foo' RETURN n"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using RIGHT and writing should be eager") {
    val query = "MATCH n WHERE right(n.prop, 5) = 'foo' SET n.prop = 'bar'"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using RIGHT and not writing should not be eager") {
    val query = "MATCH n WHERE right(n.prop, 5) = 'foo' RETURN n"

    assertNumberOfEagerness(query, 0)
  }

  test("matching property using SPLIT and writing should be eager") {
    val query = "MATCH n WHERE split(n.prop, ',') = ['foo', 'bar'] SET n.prop = 'baz,qux'"

    assertNumberOfEagerness(query, 1)
  }

  test("matching property using SPLIT and not writing should not be eager") {
    val query = "MATCH n WHERE split(n.prop, ',') = ['foo', 'bar'] RETURN n"

    assertNumberOfEagerness(query, 0)
  }

  test("matching using a pattern predicate and creating relationship should be eager") {
    val query = "MATCH n WHERE n-->() CREATE n-[:T]->()"

    assertNumberOfEagerness(query, 1)
  }

  private val MathFunctions = Table(
    "function name",
    "abs",
    "sqrt",
    "round",
    "sign",
    "sin",
    "cos",
    "cot",
    "tan",
    "atan",
    "acos",
    "asin",
    "haversin",
    "ceil",
    "floor",
    "log",
    "log10",
    "exp"
  )

  forAll(MathFunctions) {
    function =>
      test(s"matching property using ${function.toUpperCase} and writing should be eager") {
        assertNumberOfEagerness(s"MATCH n WHERE $function(n.prop) = 0 SET n.prop = 42", 1)
      }

      test(s"matching property using ${function.toUpperCase} and not writing should not be eager") {
        assertNumberOfEagerness(s"MATCH n WHERE $function(n.prop) = 0 SET n.prop = 42", 1)
      }
  }

  private val MathOperators = Table(
    "operator",
    "+",
    "-",
    "/",
    "*",
    "%",
    "^"
  )

  forAll(MathOperators) {
    operator =>
      test(s"matching using $operator should insert eagerness for writing on properties") {
        assertNumberOfEagerness(s"MATCH n WHERE n.prop $operator 3 = 0 SET n.prop = 42", 1)
      }

      test(s"matching using $operator should not insert eagerness when no writing is performed") {
        assertNumberOfEagerness(s"MATCH n WHERE n.prop $operator 3 = 0 RETURN n", 0)
      }
  }

  private val SingleArgStringFunctions = Table(
    "function name",
    "toString",
    "lower",
    "upper",
    "trim",
    "ltrim",
    "rtrim"
  )

  forAll(SingleArgStringFunctions) {
    function =>
      test(s"matching using ${function.toUpperCase} should insert eagerness for writing on properties") {
        assertNumberOfEagerness(s"MATCH n WHERE $function(n.prop) = 'foo' SET n.prop = 'bar'", 1)
      }

      test(s"matching using ${function.toUpperCase} should not insert eagerness when no writing is performed") {
        assertNumberOfEagerness(s"MATCH n WHERE $function(n.prop) = 'foo' RETURN n", 0)
      }
  }

  private val ConversionFunctions = Table(
    ("function name", "initial value", "new value"),
    ("toFloat", "1.11", "2.22"),
    ("toInt", "5", "10"),
    ("toString", "'foo'", "'bar'")
  )

  forAll(ConversionFunctions) {
    (function, initialValue, newValue) =>
      test(s"matching property using $function and writing should be eager") {
        assertNumberOfEagerness(s"MATCH n WHERE $function(n.prop) = $initialValue SET n.prop = $newValue", 1)
      }

      test(s"matching property using $function and not writing should not be eager") {
        assertNumberOfEagerness(s"MATCH n WHERE $function(n.prop) = $initialValue RETURN n", 0)
      }
  }

  private val ComparisonOperators = Table(
    "operator",
    "=",
    "<>",
    "<",
    ">",
    "<=",
    ">="
  )

  forAll(ComparisonOperators) {
    operator =>
      test(s"matching property using '$operator' and writing to same property should be eager") {
        assertNumberOfEagerness(s"MATCH n WHERE n.prop $operator 10 SET n.prop = 5", 1)
      }

      test(s"matching property using '$operator' and writing to different property should not be eager") {
        assertNumberOfEagerness(s"MATCH n WHERE n.prop1 $operator 10 SET n.prop2 = 5", 0)
      }
  }

  // tests for relationship properties
  test("matching node property, writing relationship property should not be eager") {
    val query = "MATCH (n {prop : 5})-[r]-() SET r.prop = 6"

    assertNumberOfEagerness(query, 0)
  }

  test("matching relationship property, writing same relationship property should be eager") {
    val query = "MATCH ()-[r {prop : 3}]-() SET r.prop = 6"

    assertNumberOfEagerness(query, 1)
  }

  test("matching relationship property, writing node property should not be eager") {
    val query = "MATCH (n)-[r {prop : 3}]-() SET n.prop = 6"

    assertNumberOfEagerness(query, 0)
  }

  test("matching relationship property, writing different relationship property should not be eager") {
    val query = "MATCH ()-[r {prop1 : 3}]-() SET r.prop2 = 6"

    assertNumberOfEagerness(query, 0)
  }

  test("matching on relationship property existence, writing same property should be eager") {
    val query = "MATCH ()-[r]-() WHERE has(r.prop) SET r.prop = 'foo'"

    assertNumberOfEagerness(query, 1)
  }

  test("matching on relationship property existence, writing different property should not be eager") {
    val query = "MATCH ()-[r]-() WHERE has(r.prop1) SET r.prop2 = 'foo'"

    assertNumberOfEagerness(query, 0)
  }

  test("should not be eager when merging on two different labels") {
    val query = "MERGE(:L1) MERGE(p:L2) ON CREATE SET p.name = 'Blaine'"

    assertNumberOfEagerness(query, 0)
  }

  test("should be eager when merging on the same label") {
    val query = "MERGE(:L1) MERGE(p:L1) ON CREATE SET p.name = 'Blaine'"

    assertNumberOfEagerness(query, 1)
  }

  test("should be eager when only one merge has labels") {
    val query = "MERGE() MERGE(p: Person) ON CREATE SET p.name = 'Blaine'"

    assertNumberOfEagerness(query, 1)
  }

  test("should be eager when no merge has labels") {
    val query = "MERGE() MERGE(p) ON CREATE SET p.name = 'Blaine'"

    assertNumberOfEagerness(query, 1)
  }

  test("should not be eager when merging on already bound identifiers") {
    val query = "MERGE (city:City) MERGE (country:Country) MERGE (city)-[:IN]->(country)"

    assertNumberOfEagerness(query,  0)
  }

  ignore("should not be eager when creating single node after matching on pattern with relationship") {
    val query = "MATCH ()--() CREATE ()"

    assertNumberOfEagerness(query,  0)
  }

  ignore("should not be eager when creating single node after matching on pattern with relationship and also matching on label") {
    val query = "MATCH (:L) MATCH ()--() CREATE ()"

    assertNumberOfEagerness(query,  0)
  }

  test("should be eager when creating single node after matching on empty node") {
    val query = "MATCH () CREATE ()"

    assertNumberOfEagerness(query,  1)
  }

  test("should always be eager after deleted relationships if there are any subsequent expands that might load them") {
    val device = createLabeledNode("Device")
    val cookies = (0 until 2).foldLeft(Map.empty[String, Node]) { (nodes, index) =>
      val name = s"c$index"
      val cookie = createLabeledNode(Map("name" -> name), "Cookie")
      relate(device, cookie)
      relate(cookie, createNode())
      nodes + (name -> cookie)
    }

    val query =
      """
        |MATCH (c:Cookie {name: {cookie}})<-[r2]-(d:Device)
        |WITH c, d
        |MATCH (c)-[r]-()
        |DELETE c, r
        |WITH d
        |MATCH (d)-->(c2:Cookie)
        |RETURN d, c2""".stripMargin

    cookies.foreach { case (name, node)  =>
      val result = execute(query, ("cookie" -> name))
      assertStats(result, nodesDeleted = 1, relationshipsDeleted = 2)
    }
    assertNumberOfEagerness(query, 2)
  }

  test("should always be eager after deleted nodes if there are any subsequent matches that might load them") {
    val cookies = (0 until 2).foldLeft(Map.empty[String, Node]) { (nodes, index) =>
      val name = s"c$index"
      val cookie = createLabeledNode(Map("name" -> name), "Cookie")
      nodes + (name -> cookie)
    }

    val query = "MATCH (c:Cookie) DELETE c WITH 1 as t MATCH (x:Cookie) RETURN count(*) as count"

    val result = execute(query)

    result.columnAs[Int]("count").next should equal(0)
    assertStats(result, nodesDeleted = 2)
    assertNumberOfEagerness(query, 2)
  }

  test("should always be eager after deleted paths if there are any subsequent matches that might load them") {
    val cookies = (0 until 2).foldLeft(Map.empty[String, Node]) { (nodes, index) =>
      val name = s"c$index"
      val cookie = createLabeledNode(Map("name" -> name), "Cookie")
      nodes + (name -> cookie)
    }

    val query = "MATCH p=(:Cookie) DELETE p WITH 1 as t MATCH (x:Cookie) RETURN count(*) as count"

    val result = execute(query)

    result.columnAs[Int]("count").next should equal(0)
    assertStats(result, nodesDeleted = 2)
    assertNumberOfEagerness(query, 2)
  }

  private def assertNumberOfEagerness(query: String, expectedEagerCount: Int) {
    val q = if (query.contains("EXPLAIN")) query else "EXPLAIN " + query
    val result = execute(q)
    val plan = result.executionPlanDescription().toString
    result.close()
    val length = EagerRegEx.findAllIn(plan).length
    assert(length == expectedEagerCount, plan)
  }
}
