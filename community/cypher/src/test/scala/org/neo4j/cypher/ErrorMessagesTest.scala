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
package org.neo4j.cypher

import org.scalatest.Assertions
import org.hamcrest.CoreMatchers._
import org.junit.Assert._
import org.junit.{Ignore, Test}
import CypherVersion._
import org.neo4j.cypher.internal.commands.expressions.StringHelper

class ErrorMessagesTest extends ExecutionEngineHelper with Assertions with StringHelper {
  @Test def noReturnColumns() {
    expectError("start s = node(0) return",
      v2_0    -> "return column list expected",
      vExperimental -> "Unexpected end of input: expected whitespace, DISTINCT, '*' or an expression (line 1, column 25)"
    )
  }

  @Test def badNodeIdentifier() {
    expectError("START a = node(0) MATCH a-[WORKED_ON]-, return a",
      v2_0    -> "expected an expression that is a node",
      vExperimental -> "Invalid input ',': expected whitespace, '>' or a node pattern (line 1, column 39)"
    )
  }

  @Test def badStart() {
    expectError("starta = node(0) return a",
      v2_0    -> "invalid start of query",
      vExperimental -> "Invalid input 'a' (line 1, column 6)"
    )
  }

  @Test def functionDoesNotExist() {
    expectSyntaxError("START a = node(0) return dontDoIt(a)",
      v2_0    -> ("unknown function", 36),
      vExperimental -> ("Unknown function 'dontDoIt' (line 1, column 26)", 25)
    )
  }

  @Test def noIndexName() {
    expectSyntaxError("start a = node(name=\"sebastian\") match a-[:WORKED_ON]-b return b",
      v2_0    -> ("expected node id, or *", 15),
      vExperimental -> ("Invalid input 'n': expected whitespace, an unsigned integer, a parameter or '*' (line 1, column 16)", 15)
    )
  }

  @Test def aggregateFunctionInWhere() {
    expectError("START a = node(0) WHERE count(a) > 10 RETURN a",
      v2_0    -> "Can't use aggregate functions in the WHERE clause.",
      vExperimental -> "Invalid use of aggregating function COUNT in this context (line 1, column 25)"
    )
  }

  @Test def twoIndexQueriesInSameStart() {
    expectSyntaxError("start a = node:node_auto_index(name=\"sebastian\",name=\"magnus\") return a",
      v2_0    -> ("Unclosed parenthesis", 47),
      vExperimental -> ("Invalid input ',': expected whitespace or ')' (line 1, column 48)", 47)
    )
  }

  @Test def badMatch2() {
    expectSyntaxError("start p=node(2) match p-[:IS_A]>dude return dude.name",
      v2_0    -> ("expected -", 31),
      vExperimental -> ("Invalid input '>': expected whitespace or '-' (line 1, column 32)", 31)
    )
  }

  @Test def badMatch3() {
    expectSyntaxError("start p=node(2) match p-[:IS_A->dude return dude.name",
      v2_0    -> ("unclosed bracket", 30),
      vExperimental -> ("Invalid input '-': expected an identifier character, whitespace, '|', length specification, property map or ']' (line 1, column 31)", 30)
    )
  }

  @Test def badMatch4() {
    expectSyntaxError("start p=node(2) match p-[!]->dude return dude.name",
      v2_0    -> ("expected relationship information", 25),
      vExperimental -> ("Invalid input '!': expected whitespace, an identifier, '?', relationship types, length specification, property map or ']' (line 1, column 26)", 25)
    )
  }

  @Test def badMatch5() {
    expectSyntaxError("start p=node(2) match p[:likes]->dude return dude.name",
      v2_0    -> ("expected valid query body", 23),
      vExperimental -> ("Invalid input '[': expected an identifier character, whitespace, '=', node labels, property map, a relationship pattern, ',', USING, WHERE, CREATE, DELETE, SET, REMOVE, RETURN, WITH, UNION, ';' or end of input (line 1, column 24)", 23)
    )
  }

  @Ignore @Test def missingComaBetweenColumns() {
    expectSyntaxError("start p=node(2) return sum wo.months",
      v2_0    -> ("Expected comma separated list of returnable values", 22),
      vExperimental -> ("Expected comma separated list of returnable values", 22)
    )
  }

  @Ignore @Test def missingComaBetweenStartNodes() {
    expectSyntaxError("start a=node(0) b=node(1) return a",
      v2_0    -> ("Expected comma separated list of returnable values", 22),
      vExperimental -> ("Expected comma separated list of returnable values", 22)
    )
  }

  @Test def tooManyLinksInShortestPath() {
    expectSyntaxError("start a=node(2),b=node(1) match shortestPath(a-->x-->b) return sum(wo.months)",
      v2_0    -> ("expected single path segment", 54),
      vExperimental -> ("shortestPath requires a pattern containing a single relationship (line 1, column 33)", 32)
    )
  }

  @Test def noEqualsSignInStart() {
    expectSyntaxError("start r:relationship:rels() return r",
      v2_0    -> ("expected identifier assignment", 7),
      vExperimental -> ("Invalid input ':': expected an identifier character, whitespace or '=' (line 1, column 8)", 7)
    )
  }

  @Test def relTypeInsteadOfRelIdInStart() {
    expectSyntaxError("start r = relationship(:WORKED_ON) return r",
      v2_0    -> ("expected relationship id, or *", 23),
      vExperimental -> ("Invalid input ':': expected whitespace, an unsigned integer or '*' (line 1, column 24)", 23)
    )
  }

  @Test def noNodeIdInStart() {
    expectSyntaxError("start r = node() return r",
      v2_0    -> ("expected node id, or *", 15),
      vExperimental -> ("Invalid input ')': expected whitespace, an unsigned integer, a parameter or '*' (line 1, column 16)", 15)
    )
  }

  @Test def startExpressionWithoutIdentifier() {
    expectSyntaxError("start a = node:node_auto_index(name=\"magnus\"),node:node_auto_index(name=\"sebastian) return b,c",
      v2_0    -> ("expected identifier assignment", 50),
      vExperimental -> ("Invalid input ':': expected an identifier character, whitespace or '=' (line 1, column 51)", 50)
    )
  }

  @Test def functions_and_stuff_have_to_be_renamed_when_sent_through_with() {
    expectError("START a=node(0) with a, count(*) return a",
      v2_0    -> "These columns can't be listen in the WITH statement without renaming: count(*)",
      vExperimental -> "Expression in WITH must be aliased (use AS) (line 1, column 25)"
    )
  }

  @Test def missing_dependency_correctly_reported() {
    expectError("START a=node(0) CREATE a-[:KNOWS]->(b {name:missing}) RETURN b",
      v2_0 -> "Unknown identifier `missing`",
      vExperimental -> "missing not defined (line 1, column 45)"
    )
  }

  @Test def missing_create_dependency_correctly_reported() {
    expectError("START a=node(0) CREATE a-[:KNOWS]->(b {name:missing}) RETURN b",
      v2_0    -> "Unknown identifier `missing`",
      vExperimental -> "missing not defined (line 1, column 45)"
    )
  }

  @Test def missing_set_dependency_correctly_reported() {
    expectError("START a=node(0) SET a.name = missing RETURN a",
      v2_0    -> "Unknown identifier `missing`",
      vExperimental -> "missing not defined (line 1, column 30)"
    )
  }

  @Test def create_with_identifier_already_existing() {
    expectError("START a=node(0) CREATE (a {name:'foo'}) RETURN a",
      v2_0    -> "Can't create `a` with properties here. It already exists in this context",
      vExperimental -> "Can't create `a` with properties here. It already exists in this context"
    )
  }

  @Test def create_with_identifier_already_existing2() {
    expectError("START a=node(0) CREATE UNIQUE (a {name:'foo'})-[:KNOWS]->() RETURN a",
      v2_0    -> "Can't create `a` with properties here. It already exists in this context",
      vExperimental -> "Can't create `a` with properties here. It already exists in this context"
    )
  }

  @Test def type_of_identifier_is_wrong() {
    expectError("start n=node(0) with [n] as users MATCH users-->messages RETURN messages",
      v2_0    -> "Expected `users` to be a Node but it was a Collection",
      vExperimental -> "Type mismatch: users already defined with conflicting type Collection<Node> (expected Node) (line 1, column 41)"
    )
  }

  @Test def warn_about_exclamation_mark() {
    expectError("start n=node(0) where n.foo != 2 return n",
      v2_0    -> "Cypher does not support != for inequality comparisons. Use <> instead.",
      vExperimental -> "Unknown operation '!=' (you probably meant to use '<>', which is the operator for inequality testing) (line 1, column 29)"
    )
  }

  @Test def warn_about_type_error() {
    expectError("START p=node(0) MATCH p-[r*]->() WHERE r.foo = 'apa' RETURN r",
      v2_0    -> "Expected `r` to be a Map but it was a Collection",
      vExperimental -> "Expected `r` to be a Map but it was a Collection"
    )
  }

  @Test def error_when_using_properties_on_relationships_in_match() {
    expectError("START p=node(0) MATCH p-[r {a:'foo'}]->() RETURN r",
      v2_0    -> "Properties on pattern elements are not allowed in MATCH",
      vExperimental -> "Relationship properties cannot be specified in this context (line 1, column 28)"
    )
  }

  @Test def error_when_using_properties_on_relationships_in_match2() {
    expectError("START p=node(0) MATCH p-[r]->({a:'foo'}) RETURN r",
      v2_0    -> "Properties on pattern elements are not allowed in MATCH",
      vExperimental -> "Node properties cannot be specified in this context (line 1, column 31)"
    )
  }

  @Test def missing_something_to_delete() {
    expectError("START p=node(0) DELETE x",
      v2_0    -> "Unknown identifier `x`",
      vExperimental -> "x not defined (line 1, column 24)"
    )
  }

  @Test def aggregations_must_be_included_in_return() {
    expectError("START a=node(0) RETURN a ORDER BY count(*)",
      v2_0    -> "Aggregation expressions must be listed in the RETURN clause to be used in ORDER BY",
      vExperimental -> "Aggregation expressions must be listed in the RETURN clause to be used in ORDER BY"
    )
  }

  @Test def unions_must_have_the_same_columns() {
    expectError(
      """START a=node(0) RETURN a
         UNION
         START b=node(0) RETURN b""",
      v2_0    -> "All sub queries in an UNION must have the same column names",
      vExperimental -> "All sub queries in an UNION must have the same column names"
    )
  }

  @Test def can_not_mix_union_and_union_all() {
    expectError(
      """START a=node(0) RETURN a
         UNION
         START a=node(0) RETURN a
         UNION ALL
         START a=node(0) RETURN a""",
      v2_0    -> "can't mix UNION and UNION ALL",
      vExperimental -> "Invalid combination of UNION and UNION ALL (line 4, column 10)"
    )
  }

  @Test def can_not_use_optional_pattern_as_predicate() {
    expectError("START a=node(1) RETURN a-[?]->()",
      v2_0    -> "Optional patterns cannot be used as predicates",
      vExperimental -> "Optional relationships cannot be specified in this context (line 1, column 25)"
    )
  }

  @Test def creating_an_index_twice_should_return_sensible_error() {
    graph.createIndex("LabelName", "Prop")

    expectError("CREATE INDEX ON :LabelName(Prop)",
      v2_0    -> "Property `Prop` is already indexed for label `LabelName`.",
      vExperimental -> "Property `Prop` is already indexed for label `LabelName`."
    )
  }

  @Test def trying_to_drop_constraint_index_should_return_sensible_error() {
    graph.createConstraint("LabelName", "Prop")

    expectError("DROP INDEX ON :LabelName(Prop)",
      v2_0 -> "Unable to drop index on :LabelName(Prop): Index belongs to constraint: :LabelName(Prop)"
    )
  }

  @Test def trying_to_drop_non_existent_index() {
    expectError("DROP INDEX ON :Person(name)",
      v2_0 -> "Unable to drop index on :Person(name): No such INDEX ON :Person(name)."
    )
  }

  @Test def trying_to_add_unique_constraint_when_duplicates_exist() {
    createLabeledNode(Map("name"->"A"), "Person")
    createLabeledNode(Map("name"->"A"), "Person")

    expectError("CREATE CONSTRAINT ON (person:Person) ASSERT person.name IS UNIQUE",
      v2_0 -> String.format("Unable to create CONSTRAINT ON ( person:Person ) ASSERT person.name IS UNIQUE:%n" +
        "Multiple nodes with label `Person` have property `name` = 'A':%n  node(1)%n  node(2)")
    )
  }

  @Test def trying_to_add_a_constraint_that_already_exists() {
    parseAndExecute("CREATE CONSTRAINT ON (person:Person) ASSERT person.name IS UNIQUE")

    expectError("CREATE CONSTRAINT ON (person:Person) ASSERT person.name IS UNIQUE",
      v2_0 -> String.format("Already constrained CONSTRAINT ON ( person:Person ) ASSERT person.name IS UNIQUE.")
    )
  }

  @Test def drop_a_non_existent_constraint() {
    expectError("DROP CONSTRAINT ON (person:Person) ASSERT person.name IS UNIQUE",
      v2_0 -> String.format("Constraint not found")
    )
  }

  @Test def create_without_specifying_direction_should_fail() {
    expectError("CREATE (a)-[:FOO]-(b) RETURN a,b",
      v2_0 -> String.format("Relationships need to have a direction.")
    )
  }

  @Test def create_without_specifying_direction_should_fail2() {
    expectError("CREATE (a)<-[:FOO]->(b) RETURN a,b",
      v2_0 -> String.format("Relationships need to have a direction.")
    )
  }

  private def expectError(query: String, variants: (CypherVersion, String)*) {
    for ((version, message) <- variants) {
      expectError(version, query, message)
    }
  }

  def expectError(version: CypherVersion, query: String, expectedError: String) {
    val error = intercept[CypherException](executeQuery(version, query))
    assertThat(error.getMessage, containsString(expectedError))
  }

  private def expectSyntaxError(query: String, variants: (CypherVersion, (String, Int))*) {
    for ((version, (message, offset)) <- variants) {
      expectSyntaxError(version, query, message, offset)
    }
  }

  private def expectSyntaxError(version: CypherVersion, query: String, expectedError: String, expectedOffset: Int) {
    val error = intercept[SyntaxException](executeQuery(version, query))
    assertThat(error.getMessage(), containsString(expectedError))
    assertThat(error.offset, equalTo(Some(expectedOffset) : Option[Int]))
  }
  
  def executeQuery(version: CypherVersion, query: String) {
    val qWithVer = version match {
      case `v2_0` => query
      case _      => s"cypher ${version.name} " + query
    }
    engine.execute(qWithVer).toList
  }
}
