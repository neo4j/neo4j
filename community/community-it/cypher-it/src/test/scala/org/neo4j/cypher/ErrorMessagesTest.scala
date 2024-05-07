/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher

import org.assertj.core.api.Assertions.assertThat
import org.neo4j.cypher.internal.util.helpers.StringHelper.RichString
import org.neo4j.exceptions.Neo4jException
import org.neo4j.exceptions.SyntaxException

class ErrorMessagesTest extends ExecutionEngineWithoutRestartFunSuite {

  // pure syntax errors -- not sure if TCK material?

  test("foo") {
    execute("RETURN 42")
  }

  test("noReturnColumns") {
    expectError(
      "match (s) where id(s) = 0 return",
      "Invalid input '': expected \"*\", \"DISTINCT\" or an expression (line 1, column 33 (offset: 32))",
      "Invalid input '': expected an expression, '*' or 'DISTINCT' (line 1, column 33 (offset: 32))"
    )
  }

  test("bad node variable") {
    expectSyntaxError(
      "match (a) where id(a) = 0 match (a)-[WORKED_ON]-, return a",
      48,
      "Invalid input ',': expected",
      "Mismatched input ',': expected '{', '+', '*', '(' (line 1, column 49 (offset: 48))"
    )
  }

  test("should consider extra offset in syntax error messages when there are pre-parsing options") {
    expectSyntaxError("PROFILE XX", 8, "")
  }

  test("should consider extra offset in semantic error messages when there are pre-parsing options") {
    expectSyntaxError(
      "explain match (a) where id(a) = 0 return dontDoIt(a)",
      41,
      "Unknown function 'dontDoIt' (line 1, column 42 (offset: 41))"
    )
  }

  test("should consider extra offset in semantic error messages when there are pre-parsing options - multiline") {
    expectSyntaxError(
      """explain
        |match (a) where id(a) = 0 return dontDoIt(a)""".stripMargin,
      41,
      "Unknown function 'dontDoIt' (line 2, column 34 (offset: 41))"
    )
  }

  test("should consider extra offset in semantic error messages when there are pre-parsing options - multiline 2") {
    expectSyntaxError(
      """explain match (a) where id(a) = 0
        |return dontDoIt(a)""".stripMargin,
      41,
      "Unknown function 'dontDoIt' (line 2, column 8 (offset: 41))"
    )
  }

  test("noSuchProcedure") {
    expectError(
      "CALL no.such.procedure YIELD foo RETURN foo",
      "There is no procedure with the name `no.such.procedure` registered for this database instance. " +
        "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed."
    )
  }

  test("noSuchProcedure - standalone") {
    expectError(
      "CALL no.such.procedure",
      "There is no procedure with the name `no.such.procedure` registered for this database instance. " +
        "Please ensure you've spelled the procedure name correctly and that the procedure is properly deployed."
    )
  }

  test("badMatch2") {
    expectSyntaxError(
      "match (p) where id(p) = 2 match (p)-[:IS_A]>dude return dude.name",
      43,
      "Invalid input '>'",
      "Missing '-' at '>' (line 1, column 44 (offset: 43))"
    )
  }

  test("badMatch3") {
    expectSyntaxError(
      "match (p) where id(p) = 2 match (p)-[:IS_A->dude return dude.name",
      42,
      "Invalid input '-'",
      "Mismatched input '-': expected '*', '{', '$', 'WHERE', ']' (line 1, column 43 (offset: 42))"
    )
  }

  test("badMatch4") {
    expectSyntaxError(
      "match (p) where id(p) = 2 match (p)-[!]->dude return dude.name",
      37,
      "Invalid input '!'",
      "Extraneous input '!': expected a variable name, ':', 'IS', '*', '{', '$', 'WHERE', ']' (line 1, column 38 (offset: 37))"
    )
  }

  test("badMatch5") {
    expectSyntaxError(
      "match (p) where id(p) = 2 match (p)[:likes]->dude return dude.name",
      35,
      "Invalid input '['",
      "Mismatched input '[': expected ';', <EOF> (line 1, column 36 (offset: 35))"
    )
  }

  test("invalidLabel") {
    expectError(
      "match (p) where id(p) = 2 match (p:super-man) return p.name",
      "Invalid input",
      "Mismatched input '-': expected '{', '$', 'WHERE', ')' (line 1, column 41 (offset: 40))"
    )
  }

  test("fail when using exclamation mark") {
    expectError(
      "match (n) where id(n) = 0 and n.foo != 2 return n",
      "Unknown operation '!=' (you probably meant to use '<>', which is the operator for inequality testing) (line 1, column 37 (offset: 36))"
    )
  }

  test("trying to drop constraint index should return sensible error") {
    graph.createNodeUniquenessConstraintWithName("my_index", "LabelName", "Prop")

    expectError(
      "DROP INDEX my_index",
      "Unable to drop index: Index belongs to constraint: `my_index`"
    )
  }

  test("trying to drop non existent index") {
    expectError(
      "DROP INDEX my_index",
      "Unable to drop index called `my_index`. There is no such index."
    )
  }

  test("trying to add unique constraint when duplicates exist") {
    val node1 = createLabeledNode(Map("name" -> "A"), "Person").getId
    val node2 = createLabeledNode(Map("name" -> "A"), "Person").getId

    expectError(
      "CREATE CONSTRAINT my_constraint FOR (person:Person) REQUIRE person.name IS UNIQUE",
      String.format(
        "Unable to create Constraint( name='my_constraint', type='UNIQUENESS', schema=(:Person {name}) ):%n" +
          "Both Node(" + node1 + ") and Node(" + node2 + ") have the label `Person` and property `name` = 'A'"
      )
    )
  }

  test("drop a non existent constraint") {
    expectError(
      "DROP CONSTRAINT my_constraint",
      "No such constraint"
    )
  }

  test("report wrong usage of index hint") {
    graph.createNodeUniquenessConstraint("Person", "id")
    expectError(
      "MATCH (n:Person) USING INDEX n:Person(id) WHERE n.name = 'Andres' RETURN n",
      "Cannot use index hint `USING INDEX n:Person(id)` in this context: Must use the property `id`, that the hint is referring to, on the node `n` either in the pattern or in supported predicates in `WHERE` (either directly or as part of a top-level `AND` or `OR`), but only `name` was found. Supported predicates are: equality comparison, inequality (range) comparison, `STARTS WITH`, `IN` condition or checking property existence. The comparison cannot be performed between two property values. Note that the property `id` must be specified on a non-optional node. (line 1, column 18 (offset: 17))"
    )
  }

  test("should forbid bound relationship list in shortestPath pattern parts") {
    expectError(
      "WITH [] AS r LIMIT 1 MATCH p = shortestPath((src)-[r*]->(dst)) RETURN src, dst",
      "Bound relationships not allowed in shortestPath(...)"
    )
  }

  test("should give nice error when trying to parse multiple statements") {
    expectError(
      "RETURN 42; RETURN 42",
      "Expected exactly one statement per query but got: 2"
    )
  }

  test("should give proper error message when trying to use Node Key constraint on community") {
    expectError(
      "CREATE CONSTRAINT FOR (n:Person) REQUIRE (n.firstname) IS NODE KEY",
      String.format("Unable to create Constraint( type='NODE KEY', schema=(:Person {firstname}) ):%n" +
        "Node Key constraint requires Neo4j Enterprise Edition")
    )
  }

  test("trying to store mixed type array") {
    expectError(
      "CREATE (a) SET a.value = [datetime(), time()] RETURN a.value",
      "Neo4j only supports a subset of Cypher types for storage as singleton or array properties. " +
        "Please refer to section cypher/syntax/values of the manual for more details."
    )
  }

  test("should render caret correctly in parser errors for queries without prefix") {
    testSyntaxErrorWithCaret(
      "MATCH 123",
      "      ^",
      "Invalid input '1",
      "Mismatched input '123'"
    )
  }

  test("should render caret correctly in parser errors for queries with prefix") {
    testSyntaxErrorWithCaret(
      "EXPLAIN MATCH 123",
      "              ^",
      "Invalid input '1",
      "Mismatched input '123'"
    )
  }

  test("should render caret correctly in planner errors for queries without prefix") {
    testSyntaxErrorWithCaret(
      "CALL db.awaitIndexes('wrong')",
      "                     ^",
      "Type mismatch: expected Integer but was String (line 1, column 22 (offset: 21))"
    )
  }

  test("should render caret correctly in planner errors for queries with prefix") {
    testSyntaxErrorWithCaret(
      "EXPLAIN CALL db.awaitIndexes('wrong')",
      "                             ^",
      "Type mismatch: expected Integer but was String (line 1, column 30 (offset: 29))"
    )
  }

  private def expectError(query: String, expectedError: String*): Unit = {
    val error = intercept[Neo4jException](executeQuery(query))
    withClue(error)(expectedError.exists(error.getMessage.contains) shouldBe true)
  }

  private def expectSyntaxError(query: String, expectedOffset: Int, expectedError: String*): Unit = {
    val error = intercept[SyntaxException](executeQuery(query))
    withClue(error) {
      expectedError.exists(error.getMessage.contains) shouldBe true
    }
    assertThat(error.getOffset).hasValue(expectedOffset);
  }

  private def testSyntaxErrorWithCaret(query: String, expectedCaret: String, expectedError: String*): Unit = {
    val error = intercept[SyntaxException](executeQuery(query))
    val errorLines = error.getMessage.linesIterator.toSeq
    withClue(error) {
      expectedError.exists { e =>
        val expected = String.format("\"%s\"\n %s", query, expectedCaret)
        errorLines.head.startsWith(e) &&
        errorLines.mkString("\n").endsWith(expected)
      } shouldBe true
    }
  }

  private def executeQuery(query: String): Unit = {
    execute(query.fixNewLines).toList
  }
}
