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

import java.util

import org.neo4j.cypher.ExecutionEngineFunSuite
import org.neo4j.graphdb.QueryExecutionException

class SemanticErrorAcceptanceTest extends ExecutionEngineFunSuite {

  test("return node that's not there") {
    executeAndEnsureError(
      "match (n) where id(n) = 0 return bar",
      "Variable `bar` not defined (line 1, column 34 (offset: 33))"
    )
  }

  test("don't allow a string after IN") {
    executeAndEnsureError(
      "MATCH (n) where id(n) IN '' return 1",
      "Type mismatch: expected List<T> but was String (line 1, column 26 (offset: 25))"
    )
  }

  test("don't allow a integer after IN") {
    executeAndEnsureError(
      "MATCH (n) WHERE id(n) IN 1 RETURN 1",
      "Type mismatch: expected List<T> but was Integer (line 1, column 26 (offset: 25))"
    )
  }

  test("don't allow a float after IN") {
    executeAndEnsureError(
      "MATCH (n) WHERE id(n) IN 1.0 RETURN 1",
      "Type mismatch: expected List<T> but was Float (line 1, column 26 (offset: 25))"
    )
  }

  test("don't allow a boolean after IN") {
    executeAndEnsureError(
      "MATCH (n) WHERE id(n) IN true RETURN 1",
      "Type mismatch: expected List<T> but was Boolean (line 1, column 26 (offset: 25))"
    )
  }

  test("define node and treat it as a relationship") {
    executeAndEnsureError(
      "match (r) where id(r) = 0 match (a)-[r]-(b) return r",
      "Type mismatch: r already defined with conflicting type Node (expected Relationship) (line 1, column 38 (offset: 37))"
    )
  }

  test("redefine symbol in match") {
    executeAndEnsureError(
      "match (a)-[r]-(r) return r", "Type mismatch: r already defined with conflicting type Relationship (expected Node) (line 1, column 16 (offset: 15))"
    )
  }

  test("cant use type() on nodes") {
    executeAndEnsureError(
      "MATCH (r) RETURN type(r)",
      "Type mismatch: expected Relationship but was Node (line 1, column 23 (offset: 22))"
    )
  }

  test("cant use labels() on relationships") {
    executeAndEnsureError(
      "MATCH ()-[r]-() RETURN labels(r)",
      "Type mismatch: expected Node but was Relationship (line 1, column 31 (offset: 30))"
    )
  }

  test("cant use toInt() on booleans") {
    executeAndEnsureError(
      "RETURN toInt(true)",
      "Type mismatch: expected Float, Integer, Number or String but was Boolean (line 1, column 14 (offset: 13))"
    )
  }

  test("cant use toFloat() on booleans") {
    executeAndEnsureError(
      "RETURN toFloat(false)",
      "Type mismatch: expected Float, Integer, Number or String but was Boolean (line 1, column 16 (offset: 15))"
    )
  }

  test("cant use toString() on nodes") {
    executeAndEnsureError(
      "MATCH (n) RETURN toString(n)",
      "Type mismatch: expected Boolean, Float, Integer or String but was Node (line 1, column 27 (offset: 26))"
    )
  }

  test("cant use LENGTH on nodes") {
    executeAndEnsureError(
      "match (n) where id(n) = 0 return length(n)",
      "Type mismatch: expected Path, String or List<T> but was Node (line 1, column 41 (offset: 40))"
    )
  }

  // TCK'd -- kept because of possible exclusion of this feature
  test("cant re-use relationship variable") {
    executeAndEnsureError(
      "match (a)-[r]->(b)-[r]->(a) where id(a) = 0 return r",
      "Cannot use the same relationship variable 'r' for multiple patterns (line 1, column 21 (offset: 20))"
    )
  }

  // Not TCK material; shortestPath, regex, constraints, hints, etc

  test("should know not to compare strings and numbers") {
    executeAndEnsureError(
      "match (a) where a.age =~ 13 return a",
      "Type mismatch: expected String but was Integer (line 1, column 26 (offset: 25))"
    )
  }

  test("should complain if shortest path has no relationship") {
    executeAndEnsureError(
      "match p=shortestPath(n) return p",
      "shortestPath(...) requires a pattern containing a single relationship (line 1, column 9 (offset: 8))"
    )

    executeAndEnsureError(
      "match p=allShortestPaths(n) return p",
      "allShortestPaths(...) requires a pattern containing a single relationship (line 1, column 9 (offset: 8))"
    )
  }

  test("should complain if shortest path has multiple relationships") {
    executeAndEnsureError(
      "match p=shortestPath(a--()--b) where id(a) = 0 and id(b) = 1 return p",
      "shortestPath(...) requires a pattern containing a single relationship (line 1, column 9 (offset: 8))"
    )
    executeAndEnsureError(
      "match p=allShortestPaths(a--()--b) where id(a) = 0 and id(b) = 1 return p",
      "allShortestPaths(...) requires a pattern containing a single relationship (line 1, column 9 (offset: 8))"
    )
  }

  test("should complain if shortest path has a minimal length different from 0 or 1") {
    executeAndEnsureError(
      "match p=shortestPath(a-[*2..3]->b) where id(a) = 0 and id(b) = 1 return p",
      "shortestPath(...) does not support a minimal length different from 0 or 1 (line 1, column 9 (offset: 8))"
    )
    executeAndEnsureError(
      "match p=allShortestPaths(a-[*2..3]->b) where id(a) = 0 and id(b) = 1 return p",
      "allShortestPaths(...) does not support a minimal length different from 0 or 1 (line 1, column 9 (offset: 8))"
    )
  }

  test("should be semantically incorrect to refer to unknown variable in create constraint") {
    executeAndEnsureError(
      "create constraint on (foo:Foo) assert bar.name is unique",
      "Variable `bar` not defined (line 1, column 39 (offset: 38))"
    )
  }

  test("should be semantically incorrect to refer to nexted property in create constraint") {
    executeAndEnsureError(
      "create constraint on (foo:Foo) assert foo.bar.name is unique",
      "Cannot index nested properties (line 1, column 47 (offset: 46))"
    )
  }

  test("should be semantically incorrect to refer to unknown variable in drop constraint") {
    executeAndEnsureError(
      "drop constraint on (foo:Foo) assert bar.name is unique",
      "Variable `bar` not defined (line 1, column 37 (offset: 36))"
    )
  }

  test("should be semantically incorrect to refer to nested property in drop constraint") {
    executeAndEnsureError(
      "drop constraint on (foo:Foo) assert foo.bar.name is unique",
      "Cannot index nested properties (line 1, column 45 (offset: 44))"
    )
  }

  test("should fail when trying to create shortest paths") {
    executeAndEnsureError(
      "match (a), (b) create shortestPath((a)-[:T]->(b))",
      "shortestPath(...) cannot be used to CREATE (line 1, column 23 (offset: 22))"
    )
    executeAndEnsureError(
      "match (a), (b) create allShortestPaths((a)-[:T]->(b))",
      "allShortestPaths(...) cannot be used to CREATE (line 1, column 23 (offset: 22))"
    )
  }

  test("should fail when trying to merge shortest paths") {
    executeAndEnsureError(
      "match (a), (b) MERGE shortestPath((a)-[:T]->(b))",
      "shortestPath(...) cannot be used to MERGE (line 1, column 22 (offset: 21))"
    )
    executeAndEnsureError(
      "match (a), (b) MERGE allShortestPaths((a)-[:T]->(b))",
      "allShortestPaths(...) cannot be used to MERGE (line 1, column 22 (offset: 21))"
    )
  }

  test("should fail when trying to uniquely create shortest paths") {
    executeAndEnsureError(
      "match (a), (b) create unique shortestPath((a)-[:T]->(b))",
      "shortestPath(...) cannot be used to CREATE (line 1, column 30 (offset: 29))"
    )
    executeAndEnsureError(
      "match (a), (b) create unique allShortestPaths((a)-[:T]->(b))",
      "allShortestPaths(...) cannot be used to CREATE (line 1, column 30 (offset: 29))"
    )
  }

  test("should fail when reduce used with wrong separator") {
    executeAndEnsureError("""MATCH topRoute = (s)<-[:CONNECTED_TO*1..3]-(e)
                            |WHERE id(s) = 1 AND id(e) = 2
                            |RETURN reduce(weight=0, r in relationships(topRoute) : weight+r.cost) AS score
                            |ORDER BY score ASC LIMIT 1
                          """.stripMargin,
      "reduce(...) requires '| expression' (an accumulation expression) (line 3, column 8 (offset: 84))"
    )
  }

  test("should fail if old iterable separator") {
    executeAndEnsureError(
      "match (a) where id(a) = 0 return filter(x in a.list : x.prop = 1)",
      "filter(...) requires a WHERE predicate (line 1, column 34 (offset: 33))"
    )

    executeAndEnsureError(
      "match (a) where id(a) = 0 return extract(x in a.list : x.prop)",
      "extract(...) requires '| expression' (an extract expression) (line 1, column 34 (offset: 33))"
    )

    executeAndEnsureError(
      "match (a) where id(a) = 0 return reduce(i = 0, x in a.List : i + x.prop)",
      "reduce(...) requires '| expression' (an accumulation expression) (line 1, column 34 (offset: 33))"
    )

    executeAndEnsureError(
      "match (a) where id(a) = 0 return any(x in a.list : x.prop = 1)",
      "any(...) requires a WHERE predicate (line 1, column 34 (offset: 33))"
    )

    executeAndEnsureError(
      "match (a) where id(a) = 0 return all(x in a.list : x.prop = 1)",
      "all(...) requires a WHERE predicate (line 1, column 34 (offset: 33))"
    )

    executeAndEnsureError(
      "match (a) where id(a) = 0 return single(x in a.list : x.prop = 1)",
      "single(...) requires a WHERE predicate (line 1, column 34 (offset: 33))"
    )

    executeAndEnsureError(
      "match (a) where id(a) = 0 return none(x in a.list : x.prop = 1)",
      "none(...) requires a WHERE predicate (line 1, column 34 (offset: 33))"
    )
  }

  test("should fail if using a hint with an unknown variable") {
    executeAndEnsureError(
      "match (n:Person)-->() using index m:Person(name) where n.name = \"kabam\" return n",
      "Variable `m` not defined (line 1, column 35 (offset: 34))"
    )
  }

  test("should fail if using a hint with multiple OR'd equality comparisons") {
    executeAndEnsureError(
      "match (n:Person) using index n:Person(name) where n.name = 'a' OR n.name = 'b' return n",
      "Cannot use index hint in this context. Index hints are only supported for the following "+
        "predicates in WHERE (either directly or as part of a top-level AND): equality comparison, " +
        "inequality (range) comparison, STARTS WITH, IN condition or checking property " +
        "existence. The comparison cannot be performed between two property values. Note that the " +
        "label and property comparison must be specified on a non-optional node (line 1, " +
        "column 18 (offset: 17))"
    )
  }

  test("should fail if no parens around node") {
    executeAndEnsureError(
      "match n:Person return n",
      "Parentheses are required to identify nodes in patterns, i.e. (n) (line 1, column 7 (offset: 6))"
    )
    executeAndEnsureError(
      "match n {foo: 'bar'} return n",
      "Parentheses are required to identify nodes in patterns, i.e. (n) (line 1, column 7 (offset: 6))"
    )
  }

  test("should fail if using legacy optionals match") {
    executeAndEnsureError(
      "match (n)-[?]->(m) where id(n) = 0 return n",
      "Question mark is no longer used for optional patterns - use OPTIONAL MATCH instead (line 1, column 10 (offset: 9))"
    )
  }

  test("should fail if using legacy optionals match2") {
    executeAndEnsureError(
      "match (n)-[?*]->(m) where id(n) = 0 return n",
      "Question mark is no longer used for optional patterns - use OPTIONAL MATCH instead (line 1, column 10 (offset: 9))"
    )
  }

  test("should fail if using legacy optionals match3") {
    executeAndEnsureError(
      "match shortestPath((n)-[?*]->(m)) where id(n) = 0 return n",
      "Question mark is no longer used for optional patterns - use OPTIONAL MATCH instead (line 1, column 23 (offset: 22))"
    )
  }

  test("should fail if using non update clause inside foreach") {
    executeAndEnsureError(
      "FOREACH (n in [1] | WITH foo RETURN bar)",
      "Invalid use of WITH inside FOREACH (line 1, column 21 (offset: 20))"
    )
  }

  test("should return custom type error for reduce") {
    executeAndEnsureError(
      "RETURN reduce(x = 0, y IN [1,2,3] | x + y^2)",
      "Type mismatch: accumulator is Integer but expression has type Float (line 1, column 39 (offset: 38))"
    )
  }

  test("should reject properties on shortest path relationships") {
    executeAndEnsureError(
      "MATCH (a), (b), shortestPath( (a)-[r* {x: 1}]->(b) ) RETURN *",
      "shortestPath(...) contains properties MapExpression(List((PropertyKeyName(x),SignedDecimalIntegerLiteral(1)))). This is currently not supported. (line 1, column 17 (offset: 16))"
    )
  }

  test("should reject properties on all shortest paths relationships") {
    executeAndEnsureError(
      "MATCH (a), (b), allShortestPaths( (a)-[r* {x: 1}]->(b) ) RETURN *",
      "allShortestPaths(...) contains properties MapExpression(List((PropertyKeyName(x),SignedDecimalIntegerLiteral(1)))). This is currently not supported. (line 1, column 17 (offset: 16))"
    )
  }

  test("aggregation inside looping queries is not allowed") {

    val mess = "Can't use aggregating expressions inside of expressions executing over lists"
    executeAndEnsureError(
      "MATCH (n) RETURN ALL(x in [1,2,3,4,5] WHERE count(*) = 0)",
      s"$mess (line 1, column 27 (offset: 26))")

    executeAndEnsureError(
      "MATCH (n) RETURN ANY(x in [1,2,3,4,5] WHERE count(*) = 0)",
      s"$mess (line 1, column 27 (offset: 26))")

    executeAndEnsureError(
      "MATCH (n) RETURN NONE(x in [1,2,3,4,5] WHERE count(*) = 0)",
      s"$mess (line 1, column 28 (offset: 27))")

    executeAndEnsureError(
      "MATCH (n) RETURN SINGLE(x in [1,2,3,4,5] WHERE count(*) = 0)",
      s"$mess (line 1, column 30 (offset: 29))")

    executeAndEnsureError(
      "MATCH (n) RETURN EXTRACT(x in [1,2,3,4,5] | count(*) = 0)",
      s"$mess (line 1, column 31 (offset: 30))")

    executeAndEnsureError(
      "MATCH (n) RETURN FILTER(x in [1,2,3,4,5] WHERE count(*) = 0)",
      s"$mess (line 1, column 30 (offset: 29))")

    executeAndEnsureError(
      "MATCH (n) RETURN REDUCE(acc = 0, x in [1,2,3,4,5] | acc + count(*))",
      s"$mess (line 1, column 57 (offset: 56))")
  }

  test("error message should contain full query") {
    val query = "EXPLAIN MATCH (m), (n) RETURN m, n, o LIMIT 25"
    val error = intercept[QueryExecutionException](graph.execute(query))

    val first :: second :: third :: Nil = error.getMessage.lines.toList
    first should equal("Variable `o` not defined (line 1, column 37 (offset: 36))")
    second should equal(s""""$query"""")
    third should startWith(" "*37 + "^")
  }

  test("positions should not be cached") {
    executeAndEnsureError("EXPLAIN MATCH (m), (n) RETURN m, n, o LIMIT 25",
      "Variable `o` not defined (line 1, column 37 (offset: 36))")
    executeAndEnsureError("MATCH (m), (n) RETURN m, n, o LIMIT 25",
      "Variable `o` not defined (line 1, column 29 (offset: 28))")
  }

  test("should give a nice error message if a user tries to use HAS") (
    executeAndEnsureError("MATCH (n) WHERE HAS(n.prop) RETURN n.prop",
      "HAS is no longer supported in Cypher, please use EXISTS instead (line 1, column 17 (offset: 16))")
  )

  test("give a nice error message when using unknown arguments in point") {
    executeAndEnsureError("RETURN point({xxx: 2.3, yyy: 4.5}) as point",
                          "A map with keys 'xxx', 'yyy' is not describing a valid point, a point is described either by " +
                            "using cartesian coordinates e.g. {x: 2.3, y: 4.5, crs: 'cartesian'} or using geographic " +
                            "coordinates e.g. {latitude: 12.78, longitude: 56.7, crs: 'WGS-84'}. (line 1, column 14 (offset: 13))")
  }

  // Below follows tests on integer overflow errors. Not sure if integers have bounds from a language POV -- is this TCK material?
  test("should warn on over sized integer") {
    executeAndEnsureError(
      "RETURN 1766384027365849394756747201203756",
      "integer is too large (line 1, column 8 (offset: 7))"
    )
  }

  test("should warn when addition overflows") {
    executeAndEnsureError(
      s"RETURN ${Long.MaxValue} + 1",
      "result of 9223372036854775807 + 1 cannot be represented as an integer (line 1, column 28 (offset: 27))"
    )
  }

  test("should fail nicely when addition overflows in runtime") {
    executeAndEnsureError(
      s"RETURN {t1} + {t2}",
      "result of 9223372036854775807 + 1 cannot be represented as an integer",
      "t1" -> Long.MaxValue, "t2" -> 1
    )
  }

  test("should warn when subtraction underflows") {
    executeAndEnsureError(
      s"RETURN ${Long.MinValue} - 1",
      "result of -9223372036854775808 - 1 cannot be represented as an integer (line 1, column 29 (offset: 28))"
    )
  }

  test("should fail nicely when subtraction underflows in runtime") {
    executeAndEnsureError(
      s"RETURN {t1} - {t2}",
      "result of -9223372036854775808 - 1 cannot be represented as an integer",
      "t1" -> Long.MinValue, "t2" -> 1
    )
  }

  test("should warn when multiplication overflows") {
    executeAndEnsureError(
      s"RETURN ${Long.MaxValue} * 10",
      "result of 9223372036854775807 * 10 cannot be represented as an integer (line 1, column 28 (offset: 27))"
    )
  }

  test("should fail nicely when multiplication overflows in runtime") {
    executeAndEnsureError(
      s"RETURN {t1} + {t2}",
      "result of 9223372036854775807 + 1 cannot be represented as an integer",
      "t1" -> Long.MaxValue, "t2" -> 1
    )
  }

  test("fail when parsing larger than 64 bit integers") {
    executeAndEnsureError(
      "RETURN toInt('10508455564958384115')",
      "integer, 10508455564958384115, is too large")
  }

  private def executeAndEnsureError(query: String, expected: String, params: (String,Any)*) {
    import org.neo4j.cypher.internal.frontend.v3_1.helpers.StringHelper._
    import scala.collection.JavaConverters._

    try {
      val jParams = new util.HashMap[String, Object]()
      params.foreach(kv => jParams.put(kv._1, kv._2.asInstanceOf[AnyRef]))

      graph.execute(query.fixNewLines, jParams).asScala.size
      fail(s"Did not get the expected syntax error, expected: $expected")
    } catch {
      case x: QueryExecutionException =>
        val actual = x.getMessage.lines.next().trim
        actual should equal(expected)
    }
  }
}
