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

import org.neo4j.graphdb.QueryExecutionException

import scala.collection.JavaConverters._

class SemanticErrorAcceptanceTest extends ExecutionEngineFunSuite {

  test("return node that's not there") {
    executeAndEnsureError(
      "match (n) where id(n) = 0 return bar",
      "bar not defined (line 1, column 34 (offset: 33))"
    )
  }

  test("don't allow a string after IN") {
    executeAndEnsureError(
      "MATCH (n) where id(n) IN '' return 1",
      "Type mismatch: expected Collection<T> but was String (line 1, column 26 (offset: 25))"
    )
  }

  test("don't allow a integer after IN") {
    executeAndEnsureError(
      "MATCH (n) WHERE id(n) IN 1 RETURN 1",
      "Type mismatch: expected Collection<T> but was Integer (line 1, column 26 (offset: 25))"
    )
  }

  test("don't allow a float after IN") {
    executeAndEnsureError(
      "MATCH (n) WHERE id(n) IN 1.0 RETURN 1",
      "Type mismatch: expected Collection<T> but was Float (line 1, column 26 (offset: 25))"
    )
  }

  test("don't allow a boolean after IN") {
    executeAndEnsureError(
      "MATCH (n) WHERE id(n) IN true RETURN 1",
      "Type mismatch: expected Collection<T> but was Boolean (line 1, column 26 (offset: 25))"
    )
  }

  test("define node and treat it as a relationship") {
    executeAndEnsureError(
      "match (r) where id(r) = 0 match a-[r]->b return r",
      "Type mismatch: r already defined with conflicting type Node (expected Relationship) (line 1, column 36 (offset: 35))"
    )
  }

  test("redefine symbol in match") {
    executeAndEnsureError(
      "match a-[r]->b-->r where id(a) = 0 return r",
      "Type mismatch: r already defined with conflicting type Relationship (expected Node) (line 1, column 18 (offset: 17))"
    )
  }

  test("cant use TYPE on nodes") {
    executeAndEnsureError(
      "match (r) where id(r) = 0 return type(r)",
      "Type mismatch: expected Relationship but was Node (line 1, column 39 (offset: 38))"
    )
  }

  test("cant use LENGTH on nodes") {
    executeAndEnsureError(
      "match (n) where id(n) = 0 return length(n)",
      "Type mismatch: expected Path, String or Collection<T> but was Node (line 1, column 41 (offset: 40))"
    )
  }

  test("cant re-use relationship identifier") {
    executeAndEnsureError(
      "match a-[r]->b-[r]->a where id(a) = 0 return r",
      "Cannot use the same relationship identifier 'r' for multiple patterns (line 1, column 17 (offset: 16))"
    )
  }

  test("should know not to compare strings and numbers") {
    executeAndEnsureError(
      "match (a) where a.age =~ 13 return a",
      "Type mismatch: expected String but was Integer (line 1, column 26 (offset: 25))"
    )
  }

  test("should complain about using not with a non-boolean") {
    executeAndEnsureError(
      "RETURN NOT 'foo'",
      "Type mismatch: expected Boolean but was String (line 1, column 12 (offset: 11))"
    )
  }

  test("should complain about unknown identifier") {
    executeAndEnsureError(
      "match (s) where s.name = Name and s.age = 10 return s",
      "Name not defined (line 1, column 26 (offset: 25))"
    )
  }

  test("should complain if var length rel in create") {
    executeAndEnsureError(
      "create (a)-[:FOO*2]->(b)",
      "Variable length relationships cannot be used in CREATE (line 1, column 11 (offset: 10))"
    )
  }

  test("should complain if var length rel in merge") {
    executeAndEnsureError(
      "MERGE (a)-[:FOO*2]->(b)",
      "Variable length relationships cannot be used in MERGE (line 1, column 10 (offset: 9))"
    )
  }

  test("should reject map param in match pattern") {
    executeAndEnsureError(
      "MATCH (n:Person {param}) RETURN n",
      "Parameter maps cannot be used in MATCH patterns (use a literal map instead, eg. \"{id: {param}.id}\") (line 1, column 17 (offset: 16))"
    )

    executeAndEnsureError(
      "MATCH (n:Person)-[:FOO {param}]->(m) RETURN n",
      "Parameter maps cannot be used in MATCH patterns (use a literal map instead, eg. \"{id: {param}.id}\") (line 1, column 24 (offset: 23))"
    )
  }

  test("should reject map param in merge pattern") {
    executeAndEnsureError(
      "MERGE (n:Person {param}) RETURN n",
      "Parameter maps cannot be used in MERGE patterns (use a literal map instead, eg. \"{id: {param}.id}\") (line 1, column 17 (offset: 16))"
    )

    executeAndEnsureError(
      "MATCH (n:Person) MERGE (n)-[:FOO {param}]->(m) RETURN n",
      "Parameter maps cannot be used in MERGE patterns (use a literal map instead, eg. \"{id: {param}.id}\") (line 1, column 34 (offset: 33))"
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

  test("should be semantically incorrect to refer to unknown identifier in create constraint") {
    executeAndEnsureError(
      "create constraint on (foo:Foo) assert bar.name is unique",
      "bar not defined (line 1, column 39 (offset: 38))"
    )
  }

  test("should be semantically incorrect to refer to nexted property in create constraint") {
    executeAndEnsureError(
      "create constraint on (foo:Foo) assert foo.bar.name is unique",
      "Cannot index nested properties (line 1, column 47 (offset: 46))"
    )
  }

  test("should be semantically incorrect to refer to unknown identifier in drop constraint") {
    executeAndEnsureError(
      "drop constraint on (foo:Foo) assert bar.name is unique",
      "bar not defined (line 1, column 37 (offset: 36))"
    )
  }

  test("should be semantically incorrect to refer to nested property in drop constraint") {
    executeAndEnsureError(
      "drop constraint on (foo:Foo) assert foo.bar.name is unique",
      "Cannot index nested properties (line 1, column 45 (offset: 44))"
    )
  }

  test("should fail type check when deleting") {
    //TODO: Why does the error message claim an error at column 36, but it looks like it should be column 34?
    executeAndEnsureError(
      "match (a) where id(a) = 0 delete 1 + 1",
      "Type mismatch: expected Node, Path or Relationship but was Integer (line 1, column 36 (offset: 35))"
    )
  }

  test("should not allow identifier to be overwritten by create") {
    executeAndEnsureError(
      "match (a) where id(a) = 0 create (a)",
      "a already declared (line 1, column 35 (offset: 34))"
    )
  }

  test("should not allow identifier to be overwritten by merge") {
    executeAndEnsureError(
      "match (a) where id(a) = 0 merge (a)",
      "a already declared (line 1, column 34 (offset: 33))"
    )
  }

  test("should not allow identifier to be overwritten by create relationship") {
    executeAndEnsureError(
      "match (a), ()-[r]-() where id(a) = 0 and id(r) = 1 create (a)-[r:TYP]->()",
      "r already declared (line 1, column 64 (offset: 63))"
    )
  }

  test("should not allow identifier to be overwritten by merge relationship") {
    executeAndEnsureError(
      "match (a), ()-[r]-() where id(a) = 0 and id(r) = 1 merge (a)-[r:TYP]->()",
      "r already declared (line 1, column 63 (offset: 62))"
    )
  }

  test("should not allow identifier to be introduced in pattern expression") {
    executeAndEnsureError(
      "match (n) return (n)-[:TYP]->(b)",
      "b not defined (line 1, column 31 (offset: 30))"
    )

    executeAndEnsureError(
      "match (n) return (n)-[r:TYP]->()",
      "r not defined (line 1, column 23 (offset: 22))"
    )
  }

  test("should fail when trying to create shortest paths") {
    executeAndEnsureError(
      "match a, b create shortestPath((a)-[:T]->(b))",
      "shortestPath(...) cannot be used to CREATE (line 1, column 19 (offset: 18))"
    )
    executeAndEnsureError(
      "match a, b create allShortestPaths((a)-[:T]->(b))",
      "allShortestPaths(...) cannot be used to CREATE (line 1, column 19 (offset: 18))"
    )
  }

  test("should fail when trying to merge shortest paths") {
    executeAndEnsureError(
      "match a, b MERGE shortestPath((a)-[:T]->(b))",
      "shortestPath(...) cannot be used to MERGE (line 1, column 18 (offset: 17))"
    )
    executeAndEnsureError(
      "match a, b MERGE allShortestPaths((a)-[:T]->(b))",
      "allShortestPaths(...) cannot be used to MERGE (line 1, column 18 (offset: 17))"
    )
  }

  test("should fail when trying to uniquely create shortest paths") {
    executeAndEnsureError(
      "match a, b create unique shortestPath((a)-[:T]->(b))",
      "shortestPath(...) cannot be used to CREATE (line 1, column 26 (offset: 25))"
    )
    executeAndEnsureError(
      "match a, b create unique allShortestPaths((a)-[:T]->(b))",
      "allShortestPaths(...) cannot be used to CREATE (line 1, column 26 (offset: 25))"
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
      "match (a) where id(a) = 0 return filter(x in a.collection : x.prop = 1)",
      "filter(...) requires a WHERE predicate (line 1, column 34 (offset: 33))"
    )

    executeAndEnsureError(
      "match (a) where id(a) = 0 return extract(x in a.collection : x.prop)",
      "extract(...) requires '| expression' (an extract expression) (line 1, column 34 (offset: 33))"
    )

    executeAndEnsureError(
      "match (a) where id(a) = 0 return reduce(i = 0, x in a.collection : i + x.prop)",
      "reduce(...) requires '| expression' (an accumulation expression) (line 1, column 34 (offset: 33))"
    )

    executeAndEnsureError(
      "match (a) where id(a) = 0 return any(x in a.collection : x.prop = 1)",
      "any(...) requires a WHERE predicate (line 1, column 34 (offset: 33))"
    )

    executeAndEnsureError(
      "match (a) where id(a) = 0 return all(x in a.collection : x.prop = 1)",
      "all(...) requires a WHERE predicate (line 1, column 34 (offset: 33))"
    )

    executeAndEnsureError(
      "match (a) where id(a) = 0 return single(x in a.collection : x.prop = 1)",
      "single(...) requires a WHERE predicate (line 1, column 34 (offset: 33))"
    )

    executeAndEnsureError(
      "match (a) where id(a) = 0 return none(x in a.collection : x.prop = 1)",
      "none(...) requires a WHERE predicate (line 1, column 34 (offset: 33))"
    )
  }

  test("should fail if using an hint with an unknown identifier") {
    executeAndEnsureError(
      "match (n:Person)-->() using index m:Person(name) where n.name = \"kabam\" return n",
      "m not defined (line 1, column 35 (offset: 34))"
    )
  }

  test("should fail if using an hint with property equality comparison") {
    executeAndEnsureError(
      "match (n:Person)-->(m:Person) using index n:Person(name) where n.name = m.name return n",
      "Cannot use index hint in this context. Index hints are only supported for the following "+
        "predicates in WHERE (either directly or as part of a top-level AND): equality comparison, " +
        "inequality (range) comparison, STARTS WITH, IN condition or checking property " +
        "existence. The comparison cannot be performed between two property values. Note that the " +
        "label and property comparison must be specified on a non-optional node (line 1, " +
        "column 31 (offset: 30))"
    )
  }

  test("should fail if no parens around node") {
    executeAndEnsureError(
      "match n:Person return n",
      "Parentheses are required to identify nodes in patterns (line 1, column 7 (offset: 6))"
    )
    executeAndEnsureError(
      "match n {foo: 'bar'} return n",
      "Parentheses are required to identify nodes in patterns (line 1, column 7 (offset: 6))"
    )
  }

  test("should fail if unknown identifier in merge action set clause") {
    executeAndEnsureError(
      "MERGE (n:Person) ON CREATE SET x.foo = 1",
      "x not defined (line 1, column 32 (offset: 31))"
    )
    executeAndEnsureError(
      "MERGE (n:Person) ON MATCH SET x.foo = 1",
      "x not defined (line 1, column 31 (offset: 30))")
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

  test("should require with after optional match") {
    executeAndEnsureError(
      "OPTIONAL MATCH (a)-->(b) MATCH (c)-->(d) return d",
      "MATCH cannot follow OPTIONAL MATCH (perhaps use a WITH clause between them) (line 1, column 26 (offset: 25))"
    )
  }

  test("should warn on over sized integer") {
    executeAndEnsureError(
      "RETURN 1766384027365849394756747201203756",
      "integer is too large (line 1, column 8 (offset: 7))"
    )
  }

  test("should warn on over sized double") {
    executeAndEnsureError(
      "RETURN 1.34E999",
      "floating point number is too large (line 1, column 8 (offset: 7))"
    )
  }

  test("should give type error for actions on mixed collection") {
    executeAndEnsureError(
      "RETURN (['a', 1][0]).prop",
      "Type mismatch: expected Map, Node or Relationship but was Any (line 1, column 19 (offset: 18))"
    )
  }

  test("should fail if using non update clause inside foreach") {
    executeAndEnsureError(
      "FOREACH (n in [1] | WITH foo RETURN bar)",
      "Invalid use of WITH inside FOREACH (line 1, column 21 (offset: 20))"
    )
  }

  test("should fail on non prop or pattern to exists") {
    executeAndEnsureError(
      "MATCH (n) RETURN exists(n.prop + 1)",
      "Argument to exists(...) is not a property or pattern (line 1, column 32 (offset: 31))"
    )
  }

  test("should return custom type error for reduce") {
    executeAndEnsureError(
      "RETURN reduce(x = 0, y IN [1,2,3] | x + y^2)",
      "Type mismatch: accumulator is Integer but expression has type Float (line 1, column 39 (offset: 38))"
    )
  }

  test("should return custom type when accessing a property of a non-map") {
    createNode("prop"->42)

    executeAndEnsureError(
      "MATCH n WITH n.prop AS n2 RETURN n2.prop",
      "Type mismatch: expected a map but was 42"
    )
  }

  test("Range error check") {
    executeAndEnsureError(
      "WITH range(2, 8, 0) AS r RETURN r",
      "step argument to range() cannot be zero"
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

  test("should reject unicode versions of hyphens") {
    executeAndEnsureError(
      "RETURN 42 — 41",
      """Invalid input '—': expected whitespace, comment, '.', node labels, '[', "=~", IN, STARTS, ENDS, CONTAINS, IS, '^', '*', '/', '%', '+', '-', '=', "<>", "!=", '<', '>', "<=", ">=", AND, XOR, OR, AS, ',', ORDER, SKIP, LIMIT, LOAD CSV, START, MATCH, UNWIND, MERGE, CREATE, SET, DELETE, REMOVE, FOREACH, WITH, RETURN, UNION, ';' or end of input (line 1, column 11 (offset: 10))""")
  }

  test("fail when parsing larger than 64 bit integers") {
    executeAndEnsureError(
      "RETURN toInt('10508455564958384115')",
      "integer, 10508455564958384115, is too large")
  }

  test("Should fail when calling size on a path") {
    executeAndEnsureError("match p=(a)-[*]->(b) return size(p)",
                          "Type mismatch: expected String or Collection<T> but was Path (line 1, column 34 (offset: 33))")
  }

  test("aggregation inside looping queries is not allowed") {

    val mess = "Can't use aggregating expressions inside of expressions executing over collections"
    executeAndEnsureError(
      "MATCH n RETURN [x in [1,2,3,4,5] | count(*)]",
      s"$mess (line 1, column 22 (offset: 21))")

    executeAndEnsureError(
      "MATCH n RETURN ALL(x in [1,2,3,4,5] WHERE count(*) = 0)",
      s"$mess (line 1, column 25 (offset: 24))")

    executeAndEnsureError(
      "MATCH n RETURN ANY(x in [1,2,3,4,5] WHERE count(*) = 0)",
      s"$mess (line 1, column 25 (offset: 24))")

    executeAndEnsureError(
      "MATCH n RETURN NONE(x in [1,2,3,4,5] WHERE count(*) = 0)",
      s"$mess (line 1, column 26 (offset: 25))")

    executeAndEnsureError(
      "MATCH n RETURN SINGLE(x in [1,2,3,4,5] WHERE count(*) = 0)",
      s"$mess (line 1, column 28 (offset: 27))")

    executeAndEnsureError(
      "MATCH n RETURN EXTRACT(x in [1,2,3,4,5] | count(*) = 0)",
      s"$mess (line 1, column 29 (offset: 28))")

    executeAndEnsureError(
      "MATCH n RETURN FILTER(x in [1,2,3,4,5] WHERE count(*) = 0)",
      s"$mess (line 1, column 28 (offset: 27))")

    executeAndEnsureError(
      "MATCH n RETURN REDUCE(acc = 0, x in [1,2,3,4,5] | acc + count(*))",
      s"$mess (line 1, column 55 (offset: 54))")
  }

  test("error message should contain full query") {
    val query = "EXPLAIN MATCH m, n RETURN m, n, o LIMIT 25"
    val error = intercept[QueryExecutionException](graph.execute(query))

    val first :: second :: third :: Nil = error.getMessage.lines.toList
    first should equal("o not defined (line 1, column 33 (offset: 32))")
    second should equal(s""""$query"""")
    third should startWith(" "*33 + "^")
  }

  test("positions should not be cached") {
    executeAndEnsureError("EXPLAIN MATCH m, n RETURN m, n, o LIMIT 25",
      "o not defined (line 1, column 33 (offset: 32))")
    executeAndEnsureError("MATCH m, n RETURN m, n, o LIMIT 25",
      "o not defined (line 1, column 25 (offset: 24))")

  }

  test("not allowed to refer to identifiers in SKIP")(
    executeAndEnsureError("MATCH n RETURN n SKIP n.count",
                          "It is not allowed to refer to identifiers in SKIP (line 1, column 23 (offset: 22))")
  )

  test("only allowed to use positive integer literals in SKIP") (
    executeAndEnsureError("MATCH n RETURN n SKIP -1",
                          "Invalid input '-1' is not a valid value, must be a positive integer (line 1, column 23 (offset: 22))")
  )

  test("not allowed to refer to identifiers in LIMIT")(
    executeAndEnsureError("MATCH n RETURN n LIMIT n.count",
                          "It is not allowed to refer to identifiers in LIMIT (line 1, column 24 (offset: 23))")
  )

  test("only allowed to use positive integer literals in LIMIT") (
    executeAndEnsureError("MATCH n RETURN n LIMIT 1.7",
                          "Invalid input '1.7' is not a valid value, must be a positive integer (line 1, column 24 (offset: 23))")
  )

  def executeAndEnsureError(query: String, expected: String) {
    import org.neo4j.cypher.internal.frontend.v2_3.helpers.StringHelper._

    val fixedExpected = expected.fixPosition
    try {
      graph.execute(query).asScala.size
      fail(s"Did not get the expected syntax error, expected: $fixedExpected")
    } catch {
      case x: QueryExecutionException =>
        val actual = x.getMessage.lines.next().trim
        actual should equal(fixedExpected)
    }
  }
}
