/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0

import commands.expressions.StringHelper
import org.neo4j.cypher.{CypherException, ExecutionEngineHelper, SyntaxException}
import org.scalatest.Assertions
import org.hamcrest.CoreMatchers._
import org.junit.Assert._
import org.junit.Test

class ErrorMessagesTest extends ExecutionEngineHelper with Assertions with StringHelper {
  @Test def noReturnColumns() {
    expectError(
      "start s = node(0) return",
      "Unexpected end of input: expected whitespace, DISTINCT, '*' or an expression (line 1, column 25)"
    )
  }

  @Test def badNodeIdentifier() {
    expectError(
      "START a = node(0) MATCH a-[WORKED_ON]-, return a",
      "Invalid input ',': expected whitespace, '>' or a node pattern (line 1, column 39)"
    )
  }

  @Test def badStart() {
    expectError(
      "starta = node(0) return a",
      "Invalid input 'a' (line 1, column 6)"
    )
  }

  @Test def functionDoesNotExist() {
    expectSyntaxError(
      "START a = node(0) return dontDoIt(a)",
      "Unknown function 'dontDoIt' (line 1, column 26)",
      25
    )
  }

  @Test def noIndexName() {
    expectSyntaxError(
      "start a = node(name=\"sebastian\") match a-[:WORKED_ON]-b return b",
      "Invalid input 'n': expected whitespace, an unsigned integer, a parameter or '*' (line 1, column 16)",
      15
    )
  }

  @Test def aggregateFunctionInWhere() {
    expectError(
      "START a = node(0) WHERE count(a) > 10 RETURN a",
      "Invalid use of aggregating function count(...) in this context (line 1, column 25)"
    )
  }

  @Test def twoIndexQueriesInSameStart() {
    expectSyntaxError(
      "start a = node:node_auto_index(name=\"sebastian\",name=\"magnus\") return a",
      "Invalid input ',': expected whitespace or ')' (line 1, column 48)",
      47
    )
  }

  @Test def badMatch2() {
    expectSyntaxError(
      "start p=node(2) match p-[:IS_A]>dude return dude.name",
      "Invalid input '>': expected whitespace or '-' (line 1, column 32)",
      31
    )
  }

  @Test def badMatch3() {
    expectSyntaxError(
      "start p=node(2) match p-[:IS_A->dude return dude.name",
      "Invalid input '-': expected an identifier character, whitespace, '|', a length specification, a property map or ']' (line 1, column 31)",
      30
    )
  }

  @Test def badMatch4() {
    expectSyntaxError(
      "start p=node(2) match p-[!]->dude return dude.name",
      "Invalid input '!': expected whitespace, an identifier, '?', relationship types, a length specification, a property map or ']' (line 1, column 26)",
      25
    )
  }

  @Test def badMatch5() {
    expectSyntaxError(
      "start p=node(2) match p[:likes]->dude return dude.name",
      "Invalid input '[': expected an identifier character, whitespace, '=', node labels, a property map, a relationship pattern, ',', USING, WHERE, START, MATCH, MERGE, CREATE, SET, DELETE, REMOVE, FOREACH, WITH, RETURN, UNION, ';' or end of input (line 1, column 24)",
      23
    )
  }

  @Test def invalidLabel() {
    expectSyntaxError(
      "start p=node(2) match (p:super-man) return p.name",
      "Invalid input 'm': expected whitespace, [ or '-' (line 1, column 32)",
      31
    )
  }

  @Test def noEqualsSignInStart() {
    expectSyntaxError(
      "start r:relationship:rels() return r",
      "Invalid input ':': expected an identifier character, whitespace or '=' (line 1, column 8)",
      7
    )
  }

  @Test def relTypeInsteadOfRelIdInStart() {
    expectSyntaxError(
      "start r = relationship(:WORKED_ON) return r",
      "Invalid input ':': expected whitespace, an unsigned integer, a parameter or '*' (line 1, column 24)",
      23
    )
  }

  @Test def noNodeIdInStart() {
    expectSyntaxError(
      "start r = node() return r",
      "Invalid input ')': expected whitespace, an unsigned integer, a parameter or '*' (line 1, column 16)",
      15
    )
  }

  @Test def startExpressionWithoutIdentifier() {
    expectSyntaxError(
      "start a = node:node_auto_index(name=\"magnus\"),node:node_auto_index(name=\"sebastian) return b,c",
      "Invalid input ':': expected an identifier character, whitespace or '=' (line 1, column 51)",
      50
    )
  }

  @Test def functions_and_stuff_have_to_be_renamed_when_sent_through_with() {
    expectError(
      "START a=node(0) with a, count(*) return a",
      "Expression in WITH must be aliased (use AS) (line 1, column 25)"
    )
  }

  @Test def missing_dependency_correctly_reported() {
    expectError(
      "START a=node(0) CREATE a-[:KNOWS]->(b {name:missing}) RETURN b",
      "missing not defined (line 1, column 45)"
    )
  }

  @Test def missing_create_dependency_correctly_reported() {
    expectError(
      "START a=node(0) CREATE a-[:KNOWS]->(b {name:missing}) RETURN b",
      "missing not defined (line 1, column 45)"
    )
  }

  @Test def missing_set_dependency_correctly_reported() {
    expectError(
      "START a=node(0) SET a.name = missing RETURN a",
      "missing not defined (line 1, column 30)"
    )
  }

  @Test def create_with_identifier_already_existing() {
    expectError(
      "START a=node(0) CREATE (a {name:'foo'}) RETURN a",
      "a already declared (line 1, column 25)"
    )
  }

  @Test def create_with_identifier_already_existing2() {
    expectError(
      "START a=node(0) CREATE UNIQUE (a {name:'foo'})-[:KNOWS]->() RETURN a",
      "Can't create `a` with properties here. It already exists in this context"
    )
  }

  @Test def type_of_identifier_is_wrong() {
    expectError(
      "start n=node(0) with [n] as users MATCH users-->messages RETURN messages",
      "Type mismatch: users already defined with conflicting type Collection<Node> (expected Node) (line 1, column 41)"
    )
  }

  @Test def warn_about_exclamation_mark() {
    expectError(
      "start n=node(0) where n.foo != 2 return n",
      "Unknown operation '!=' (you probably meant to use '<>', which is the operator for inequality testing) (line 1, column 29)"
    )
  }

  @Test def warn_about_type_error() {
    expectError(
      "START p=node(0) MATCH p-[r*]->() WHERE r.foo = 'apa' RETURN r",
      "Type mismatch: r already defined with conflicting type Collection<Relationship> (expected Map) (line 1, column 40)"
    )
  }

  @Test def error_when_using_properties_on_relationships_in_match() {
    expectError(
      "START p=node(0) MATCH p-[r {a:'foo'}]->() RETURN r",
      "Relationship properties cannot be specified in this context (line 1, column 28)"
    )
  }

  @Test def error_when_using_properties_on_relationships_in_match2() {
    expectError(
      "START p=node(0) MATCH p-[r]->({a:'foo'}) RETURN r",
      "Node properties cannot be specified in this context (line 1, column 31)"
    )
  }

  @Test def missing_something_to_delete() {
    expectError(
      "START p=node(0) DELETE x",
      "x not defined (line 1, column 24)"
    )
  }

  @Test def aggregations_must_be_included_in_return() {
    expectError(
      "START a=node(0) RETURN a ORDER BY count(*)",
      "Aggregation expressions must be listed in the RETURN clause to be used in ORDER BY"
    )
  }

  @Test def unions_must_have_the_same_columns() {
    expectError(
      """START a=node(0) RETURN a
         UNION
         START b=node(0) RETURN b""",
      "All sub queries in an UNION must have the same column names"
    )
  }

  @Test def can_not_mix_union_and_union_all() {
    expectError(
      """START a=node(0) RETURN a
         UNION
         START a=node(0) RETURN a
         UNION ALL
         START a=node(0) RETURN a""",
      "Invalid combination of UNION and UNION ALL (line 4, column 10)"
    )
  }

  @Test def can_not_use_optional_pattern_as_predicate() {
    expectError(
      "START a=node(1) RETURN a-[?]->()",
      "Optional relationships cannot be specified in this context (line 1, column 25)"
    )
  }

  @Test def trying_to_drop_constraint_index_should_return_sensible_error() {
    graph.createConstraint("LabelName", "Prop")

    expectError(
      "DROP INDEX ON :LabelName(Prop)",
      "Unable to drop index on :LabelName(Prop): Index belongs to constraint: :LabelName(Prop)"
    )
  }

  @Test def trying_to_drop_non_existent_index() {
    expectError(
      "DROP INDEX ON :Person(name)",
      "Unable to drop index on :Person(name): No such INDEX ON :Person(name)."
    )
  }

  @Test def trying_to_add_unique_constraint_when_duplicates_exist() {
    createLabeledNode(Map("name" -> "A"), "Person")
    createLabeledNode(Map("name" -> "A"), "Person")

    expectError(
      "CREATE CONSTRAINT ON (person:Person) ASSERT person.name IS UNIQUE",
      String.format("Unable to create CONSTRAINT ON ( person:Person ) ASSERT person.name IS UNIQUE:%n" +
        "Multiple nodes with label `Person` have property `name` = 'A':%n  node(1)%n  node(2)")
    )
  }

  @Test def drop_a_non_existent_constraint() {
    expectError(
      "DROP CONSTRAINT ON (person:Person) ASSERT person.name IS UNIQUE",
      "No such constraint"
    )
  }

  @Test def create_without_specifying_direction_should_fail() {
    expectError(
      "CREATE (a)-[:FOO]-(b) RETURN a,b",
      "Relationships need to have a direction when used to CREATE."
    )
  }

  @Test def create_without_specifying_direction_should_fail2() {
    expectError(
      "CREATE (a)<-[:FOO]->(b) RETURN a,b",
      "Relationships need to have a direction when used to CREATE."
    )
  }

  @Test def report_deprecated_use_of_property_name_with_question_mark() {
    expectError(
      "start n=node(1) return n.title? = \"foo\"",
      "This syntax is no longer supported (missing properties are now returned as null). Please use (not(has(<ident>.title)) OR <ident>.title=<value>) if you really need the old behavior."
    )
  }

  @Test def report_deprecated_use_of_property_name_with_exclamation_mark() {
    expectError(
      "start n=node(1) return n.title! = \"foo\"",
      "This syntax is no longer supported (missing properties are now returned as null)."
    )
  }

  @Test def recommend_using_remove_when_user_tries_to_delete_a_label() {
    expectError(
      "start n=node(1) delete n:Person",
      "DELETE doesn't support removing labels from a node. Try REMOVE."
    )
  }

  def expectError(query: String, expectedError: String) {
    val error = intercept[CypherException](executeQuery(query))
    assertThat(error.getMessage, containsString(expectedError))
  }

  private def expectSyntaxError(query: String, expectedError: String, expectedOffset: Int) {
    val error = intercept[SyntaxException](executeQuery(query))
    assertThat(error.getMessage(), containsString(expectedError))
    assertThat(error.offset, equalTo(Some(expectedOffset): Option[Int]))
  }

  def executeQuery(query: String) {
    execute(query).toList
  }
}
