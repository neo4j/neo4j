/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.hamcrest.CoreMatchers._
import org.junit.Assert._
import org.opencypher.v9_0.util.helpers.StringHelper._

class ErrorMessagesTest extends ExecutionEngineFunSuite {

  // pure syntax errors -- not sure if TCK material?

  test("foo") {
    execute("RETURN 42")
  }

  test("noReturnColumns") {
    expectError(
      "match (s) where id(s) = 0 return",
      "Unexpected end of input: expected whitespace, DISTINCT, GRAPHS, SOURCE GRAPH [AS <name>], TARGET GRAPH [AS <name>], GRAPH AT <graph-url> [AS <name>], GRAPH OF <pattern> [AS <name>], GRAPH, GRAPH <graph-ref> [AS <name>], >>, '*' or an expression (line 1, column 33 (offset: 32))"
    )
  }

  test("bad node variable") {
    expectError(
      "match (a) where id(a) = 0 match (a)-[WORKED_ON]-, return a",
      "Invalid input ',': expected whitespace, '>' or a node pattern (line 1, column 49 (offset: 48))"
    )
  }

  test("badStart") {
    expectError(
      "starta = node(0) return a",
      "Invalid input 'a' (line 1, column 6 (offset: 5))"
    )
  }

  test("should consider extra offset in syntax error messages when there are pre-parsing options") {
    expectSyntaxError("PROFILE XX", "", 8)
  }

  test("should consider extra offset in semantic error messages when there are pre-parsing options") {
    expectSyntaxError(
      "explain match (a) where id(a) = 0 return dontDoIt(a)",
      "Unknown function 'dontDoIt' (line 1, column 42 (offset: 41))",
      41
    )
  }

  test("should consider extra offset in semantic error messages when there are pre-parsing options - multiline") {
    expectSyntaxError(
      """explain
        |match (a) where id(a) = 0 return dontDoIt(a)""".stripMargin,
      "Unknown function 'dontDoIt' (line 2, column 34 (offset: 41))",
      41
    )
  }

  test("should consider extra offset in semantic error messages when there are pre-parsing options - multiline 2") {
    expectSyntaxError(
      """explain match (a) where id(a) = 0
        |return dontDoIt(a)""".stripMargin,
      "Unknown function 'dontDoIt' (line 2, column 8 (offset: 41))",
      41
    )
  }

  test("noIndexName") {
    expectSyntaxError(
      "start a = node(name=\"sebastian\") match (a)-[:WORKED_ON]-b return b",
      "Invalid input 'n': expected whitespace, an unsigned integer, a parameter or '*' (line 1, column 16 (offset: 15))",
      15
    )
  }

  test("twoIndexQueriesInSameStart") {
    expectSyntaxError(
      "start a = node:node_auto_index(name=\"sebastian\",name=\"magnus\") return a",
      "Invalid input ',': expected whitespace or ')' (line 1, column 48 (offset: 47))",
      47
    )
  }

  test("badMatch2") {
    expectSyntaxError(
      "match (p) where id(p) = 2 match p-[:IS_A]>dude return dude.name",
      "Invalid input '>': expected whitespace or '-' (line 1, column 42 (offset: 41))",
      41
    )
  }

  test("badMatch3") {
    expectSyntaxError(
      "match (p) where id(p) = 2 match p-[:IS_A->dude return dude.name",
      "Invalid input '-': expected an identifier character, whitespace, '|', a length specification, a property map or ']' (line 1, column 41 (offset: 40))",
      40
    )
  }

  test("badMatch4") {
    expectSyntaxError(
      "match (p) where id(p) = 2 match p-[!]->dude return dude.name",
      "Invalid input '!': expected whitespace, a variable, relationship types, a length specification, a property map or ']' (line 1, column 36 (offset: 35))",
      35
    )
  }

  test("badMatch5") {
    expectSyntaxError(
      "match (p) where id(p) = 2 match p[:likes]->dude return dude.name",
      "Invalid input '[': expected an identifier character, whitespace, '=', node labels, a property map, " +
      "a relationship pattern, ',', USING, WHERE, LOAD CSV, FROM, INTO, START, MATCH, UNWIND, MERGE, " +
      "CREATE GRAPH >>, CREATE >> GRAPH, CREATE GRAPH, CREATE, SET, DELETE GRAPHS, DELETE, REMOVE, FOREACH, WITH, " +
      "CALL, PERSIST, RELOCATE, RETURN, SNAPSHOT, UNION, ';' or end of input (line 1, column 34 (offset: 33))",
      33
    )
  }

  test("invalidLabel") {
    expectSyntaxError(
      "match (p) where id(p) = 2 match (p:super-man) return p.name",
      "Invalid input 'm': expected whitespace, [ or '-' (line 1, column 42 (offset: 41))",
      41
    )
  }

  test("noEqualsSignInStart") {
    expectSyntaxError(
      "start r:relationship:rels() return r",
      "Invalid input ':': expected an identifier character, whitespace or '=' (line 1, column 8 (offset: 7))",
      7
    )
  }

  test("relTypeInsteadOfRelIdInStart") {
    expectSyntaxError(
      "start r = relationship(:WORKED_ON) return r",
      "Invalid input ':': expected whitespace, an unsigned integer, a parameter or '*' (line 1, column 24 (offset: " +
        "23))",
      23
    )
  }

  test("noNodeIdInStart") {
    expectSyntaxError(
      "start r = node() return r",
      "Invalid input ')': expected whitespace, an unsigned integer, a parameter or '*' (line 1, column 16 (offset: 15))",
      15
    )
  }

  test("start expression without variable") {
    expectSyntaxError(
      "start a = node:node_auto_index(name=\"magnus\"),node:node_auto_index(name=\"sebastian) return b,c",
      "Invalid input ':': expected an identifier character, whitespace or '=' (line 1, column 51 (offset: 50))",
      50
    )
  }

  test("fail when using exclamation mark") {
    expectError(
      "match (n) where id(n) = 0 and n.foo != 2 return n",
      "Unknown operation '!=' (you probably meant to use '<>', which is the operator for inequality testing) (line 1, column 37 (offset: 36))"
    )
  }

  test("trying to drop constraint index should return sensible error") {
    graph.createConstraint("LabelName", "Prop")

    expectError(
      "DROP INDEX ON :LabelName(Prop)",
      "Unable to drop index on :LabelName(Prop): Index belongs to constraint: :LabelName(Prop)"
    )
  }

  test("trying to drop non existent index") {
    expectError(
      "DROP INDEX ON :Person(name)",
      "Unable to drop index on :Person(name): No such INDEX ON :Person(name)."
    )
  }

  test("trying to add unique constraint when duplicates exist") {
    val node1 = createLabeledNode(Map("name" -> "A"), "Person").getId
    val node2 = createLabeledNode(Map("name" -> "A"), "Person").getId

    expectError(
      "CREATE CONSTRAINT ON (person:Person) ASSERT person.name IS UNIQUE",
      String.format("Unable to create CONSTRAINT ON ( person:Person ) ASSERT person.name IS UNIQUE:%n" +
        "Both Node(" + node1 + ") and Node(" + node2 + ") have the label `Person` and property `name` = 'A'")
    )
  }

  test("drop a non existent constraint") {
    expectError(
      "DROP CONSTRAINT ON (person:Person) ASSERT person.name IS UNIQUE",
      "No such constraint"
    )
  }

  test("report wrong usage of index hint") {
    graph.createConstraint("Person", "id")
    expectError(
      "MATCH (n:Person) USING INDEX n:Person(id) WHERE n.name = 'Andres' RETURN n",
      "Cannot use index hint in this context. Index hints are only supported for the following predicates in WHERE (either directly or as part of a top-level AND or OR): equality comparison, inequality (range) comparison, STARTS WITH, IN condition or checking property existence. The comparison cannot be performed between two property values. Note that the label and property comparison must be specified on a non-optional node (line 1, column 18 (offset: 17))"
    )
  }

  test("report wrong usage of label scan hint") {
    expectError(
      "MATCH (n) USING SCAN n:Person WHERE n:Person OR n:Bird RETURN n",
      "Cannot use label scan hint in this context. Label scan hints require using a simple label test in WHERE (either directly or as part of a top-level AND). Note that the label must be specified on a non-optional node")
  }

  test("should forbid bound relationship list in shortestPath pattern parts") {
    expectError(
      "WITH [] AS r LIMIT 1 MATCH p = shortestPath(src-[r*]->dst) RETURN src, dst",
      "Bound relationships not allowed in shortestPath(...)"
    )
  }

  test("should give nice error when trying to parse multiple statements") {
    expectError(
      "RETURN 42; RETURN 42",
      "Expected exactly one statement per query but got: 2")
  }

  test("should give proper error message when trying to use Node Key constraint on community") {
    expectError("CREATE CONSTRAINT ON (n:Person) ASSERT (n.firstname) IS NODE KEY",
                String.format("Unable to create CONSTRAINT ON ( person:Person ) ASSERT exists(person.firstname):%n" +
                  "Node Key constraint requires Neo4j Enterprise Edition"))
  }

  test("trying to store mixed type array") {
    expectError("CREATE (a) SET a.value = [datetime(), time()] RETURN a.value",
      "Neo4j only supports a subset of Cypher types for storage as singleton or array properties. " +
        "Please refer to section cypher/syntax/values of the manual for more details."
    )
  }

  private def expectError(query: String, expectedError: String) {
    val error = intercept[CypherException](executeQuery(query))
    assertThat(error.getMessage, containsString(expectedError))
  }

  private def expectSyntaxError(query: String, expectedError: String, expectedOffset: Int) {
    val error = intercept[SyntaxException](executeQuery(query))
    assertThat(error.getMessage(), containsString(expectedError))
    assertThat(error.offset, equalTo(Some(expectedOffset): Option[Int]))
  }

  private def executeQuery(query: String) {
    execute(query.fixNewLines).toList
  }
}
