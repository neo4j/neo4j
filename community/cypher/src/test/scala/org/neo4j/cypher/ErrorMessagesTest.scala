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
      vLegacy -> "return column list expected",
      v2_0 -> "Unexpected end of input: expected whitespace, DISTINCT, '*' or an expression (line 1, column 25)"
    )
  }

  @Test def badNodeIdentifier() {
    expectError("START a = node(0) MATCH a-[WORKED_ON]-, return a",
      vLegacy -> "expected an expression that is a node",
      v2_0 -> "Invalid input ',': expected whitespace, '>' or a node pattern (line 1, column 39)"
    )
  }

  @Test def badStart() {
    expectError("starta = node(0) return a",
      vLegacy -> "expected valid query body",
      v2_0 -> "Invalid input 'a' (line 1, column 6)"
    )
  }

  @Test def functionDoesNotExist() {
    expectSyntaxError("START a = node(0) return dontDoIt(a)",
      vLegacy ->("unknown function", 36),
      v2_0 ->("Unknown function 'dontDoIt' (line 1, column 26)", 25)
    )
  }

  @Test def noIndexName() {
    expectSyntaxError("start a = node(name=\"sebastian\") match a-[:WORKED_ON]-b return b",
      vLegacy ->("expected node id, or *", 15),
      v2_0 ->("Invalid input 'n': expected whitespace, an unsigned integer, a parameter or '*' (line 1, column 16)", 15)
    )
  }

  @Test def aggregateFunctionInWhere() {
    expectError("START a = node(0) WHERE count(a) > 10 RETURN a",
      vLegacy -> "Can't use aggregate functions in the WHERE clause.",
      v2_0 -> "Invalid use of aggregating function COUNT in this context (line 1, column 25)"
    )
  }

  @Test def twoIndexQueriesInSameStart() {
    expectSyntaxError("start a = node:node_auto_index(name=\"sebastian\",name=\"magnus\") return a",
      vLegacy ->("Unclosed parenthesis", 47),
      v2_0 ->("Invalid input ',': expected whitespace or ')' (line 1, column 48)", 47)
    )
  }

  @Test def badMatch2() {
    expectSyntaxError("start p=node(2) match p-[:IS_A]>dude return dude.name",
      vLegacy ->("expected -", 31),
      v2_0 ->("Invalid input '>': expected whitespace or '-' (line 1, column 32)", 31)
    )
  }

  @Test def badMatch3() {
    expectSyntaxError("start p=node(2) match p-[:IS_A->dude return dude.name",
      vLegacy ->("unclosed bracket", 30),
      v2_0 ->("Invalid input '-': expected an identifier character, whitespace, '|', a length specification, a property map or ']' (line 1, column 31)", 30)
    )
  }

  @Test def badMatch4() {
    expectSyntaxError("start p=node(2) match p-[!]->dude return dude.name",
      vLegacy ->("expected relationship information", 25),
      v2_0 ->("Invalid input '!': expected whitespace, an identifier, '?', relationship types, a length specification, a property map or ']' (line 1, column 26)", 25)
    )
  }

  @Test def badMatch5() {
    expectSyntaxError("start p=node(2) match p[:likes]->dude return dude.name",
      vLegacy ->("expected valid query body", 23),
      v2_0 ->("Invalid input '[': expected an identifier character, whitespace, '=', node labels, a relationship pattern, ',', USING, WHERE, CREATE, DELETE, SET, REMOVE, RETURN, WITH, UNION, ';' or end of input (line 1, column 24)", 23)
    )
  }

  @Ignore
  @Test def missingComaBetweenColumns() {
    expectSyntaxError("start p=node(2) return sum wo.months",
      vLegacy ->("Expected comma separated list of returnable values", 22),
      v2_0 ->("Expected comma separated list of returnable values", 22)
    )
  }

  @Ignore
  @Test def missingComaBetweenStartNodes() {
    expectSyntaxError("start a=node(0) b=node(1) return a",
      vLegacy ->("Expected comma separated list of returnable values", 22),
      v2_0 ->("Expected comma separated list of returnable values", 22)
    )
  }

  @Test def tooManyLinksInShortestPath() {
    expectSyntaxError("start a=node(2),b=node(1) match shortestPath(a-->x-->b) return sum(wo.months)",
      vLegacy ->("expected single path segment", 54),
      v2_0 ->("shortestPath requires a pattern containing a single relationship (line 1, column 33)", 32)
    )
  }

  @Test def noEqualsSignInStart() {
    expectSyntaxError("start r:relationship:rels() return r",
      vLegacy ->("expected identifier assignment", 7),
      v2_0 ->("Invalid input ':': expected an identifier character, whitespace or '=' (line 1, column 8)", 7)
    )
  }

  @Test def relTypeInsteadOfRelIdInStart() {
    expectSyntaxError("start r = relationship(:WORKED_ON) return r",
      vLegacy ->("expected relationship id, or *", 23),
      v2_0 ->("Invalid input ':': expected whitespace, an unsigned integer, a parameter or '*' (line 1, column 24)", 23)
    )
  }

  @Test def noNodeIdInStart() {
    expectSyntaxError("start r = node() return r",
      vLegacy ->("expected node id, or *", 15),
      v2_0 ->("Invalid input ')': expected whitespace, an unsigned integer, a parameter or '*' (line 1, column 16)", 15)
    )
  }

  @Test def startExpressionWithoutIdentifier() {
    expectSyntaxError("start a = node:node_auto_index(name=\"magnus\"),node:node_auto_index(name=\"sebastian) return b,c",
      vLegacy ->("expected identifier assignment", 50),
      v2_0 ->("Invalid input ':': expected an identifier character, whitespace or '=' (line 1, column 51)", 50)
    )
  }

  @Test def functions_and_stuff_have_to_be_renamed_when_sent_through_with() {
    expectError("START a=node(0) with a, count(*) return a",
      vLegacy -> "These columns can't be listen in the WITH statement without renaming: count(*)",
      v2_0 -> "Expression in WITH must be aliased (use AS) (line 1, column 25)"
    )
  }

  @Test def missing_dependency_correctly_reported() {
    expectError("START a=node(0) CREATE a-[:KNOWS]->(b {name:missing}) RETURN b",
      vLegacy -> "Unknown identifier `missing`",
      v2_0 -> "missing not defined (line 1, column 45)"
    )
  }

  @Test def missing_create_dependency_correctly_reported() {
    expectError("START a=node(0) CREATE a-[:KNOWS]->(b {name:missing}) RETURN b",
      vLegacy -> "Unknown identifier `missing`",
      v2_0 -> "missing not defined (line 1, column 45)"
    )
  }

  @Test def missing_set_dependency_correctly_reported() {
    expectError("START a=node(0) SET a.name = missing RETURN a",
      vLegacy -> "Unknown identifier `missing`",
      v2_0 -> "missing not defined (line 1, column 30)"
    )
  }

  @Test def create_with_identifier_already_existing() {
    expectError("START a=node(0) CREATE (a {name:'foo'}) RETURN a",
      vLegacy -> "Can't create `a` with properties here. It already exists in this context",
      v2_0 -> "Can't create `a` with properties here. It already exists in this context"
    )
  }

  @Test def create_with_identifier_already_existing2() {
    expectError("START a=node(0) CREATE UNIQUE (a {name:'foo'})-[:KNOWS]->() RETURN a",
      vLegacy -> "Can't create `a` with properties here. It already exists in this context",
      v2_0 -> "Can't create `a` with properties here. It already exists in this context"
    )
  }

  @Test def type_of_identifier_is_wrong() {
    expectError("start n=node(0) with [n] as users MATCH users-->messages RETURN messages",
      vLegacy -> "Expected `users` to be a Node but it was a Collection",
      v2_0 -> "Type mismatch: users already defined with conflicting type Collection<Node> (expected Node) (line 1, column 41)"
    )
  }

  @Test def warn_about_exclamation_mark() {
    expectError("start n=node(0) where n.foo != 2 return n",
      vLegacy -> "Cypher does not support != for inequality comparisons. Use <> instead.",
      v2_0 -> "Unknown operation '!=' (you probably meant to use '<>', which is the operator for inequality testing) (line 1, column 29)"
    )
  }

  @Test def warn_about_type_error() {
    expectError("START p=node(0) MATCH p-[r*]->() WHERE r.foo = 'apa' RETURN r",
      vLegacy -> "Expected `r` to be a Map but it was a Collection",
      v2_0 -> "Type mismatch: r already defined with conflicting type Collection<Relationship> (expected Map) (line 1, column 40)"
    )
  }

  @Test def error_when_using_properties_on_relationships_in_match() {
    expectError("START p=node(0) MATCH p-[r {a:'foo'}]->() RETURN r",
      vLegacy -> "Properties on pattern elements are not allowed in MATCH",
      v2_0 -> "Relationship properties cannot be specified in this context (line 1, column 28)"
    )
  }

  @Test def error_when_using_properties_on_relationships_in_match2() {
    expectError("START p=node(0) MATCH p-[r]->({a:'foo'}) RETURN r",
      vLegacy -> "Properties on pattern elements are not allowed in MATCH",
      v2_0 -> "Node properties cannot be specified in this context (line 1, column 31)"
    )
  }

  @Test def missing_something_to_delete() {
    expectError("START p=node(0) DELETE x",
      vLegacy -> "Unknown identifier `x`",
      v2_0 -> "x not defined (line 1, column 24)"
    )
  }

  @Test def aggregations_must_be_included_in_return() {
    expectError("START a=node(0) RETURN a ORDER BY count(*)",
      vLegacy -> "Aggregation expressions must be listed in the RETURN clause to be used in ORDER BY",
      v2_0 -> "Aggregation expressions must be listed in the RETURN clause to be used in ORDER BY"
    )
  }

  @Test def unions_must_have_the_same_columns() {
    expectError(
      """START a=node(0) RETURN a
         UNION
         START b=node(0) RETURN b""",
      vLegacy -> "All sub queries in an UNION must have the same column names",
      v2_0 -> "All sub queries in an UNION must have the same column names"
    )
  }

  @Test def can_not_mix_union_and_union_all() {
    expectError(
      """START a=node(0) RETURN a
         UNION
         START a=node(0) RETURN a
         UNION ALL
         START a=node(0) RETURN a""",
      vLegacy -> "can't mix UNION and UNION ALL",
      v2_0 -> "Invalid combination of UNION and UNION ALL (line 4, column 10)"
    )
  }

  @Test def can_not_use_optional_pattern_as_predicate() {
    expectError("START a=node(1) RETURN a-[?]->()",
      vLegacy -> "Optional patterns cannot be used as predicates",
      v2_0 -> "Optional relationships cannot be specified in this context (line 1, column 25)"
    )
  }

  @Test def creating_an_index_twice_should_return_sensible_error() {
    graph.createIndex("LabelName", "Prop")

    expectError("CREATE INDEX ON :LabelName(Prop)",
      vLegacy -> "Property `Prop` is already indexed for label `LabelName`.",
      v2_0 -> "Property `Prop` is already indexed for label `LabelName`."
    )
  }

  @Test def trying_to_drop_constraint_index_should_return_sensible_error() {
    graph.createConstraint("LabelName", "Prop")

    expectError("DROP INDEX ON :LabelName(Prop)",
      vLegacy -> "Unable to drop index on :LabelName(Prop): Index belongs to constraint: :LabelName(Prop)"
    )
  }

  @Test def trying_to_drop_non_existent_index() {
    expectError("DROP INDEX ON :Person(name)",
      vLegacy -> "Unable to drop index on :Person(name): No such INDEX ON :Person(name)."
    )
  }

  @Test def trying_to_add_unique_constraint_when_duplicates_exist() {
    createLabeledNode(Map("name" -> "A"), "Person")
    createLabeledNode(Map("name" -> "A"), "Person")

    expectError("CREATE CONSTRAINT ON (person:Person) ASSERT person.name IS UNIQUE",
      vLegacy -> String.format("Unable to create CONSTRAINT ON ( person:Person ) ASSERT person.name IS UNIQUE:%n" +
        "Multiple nodes with label `Person` have property `name` = 'A':%n  node(1)%n  node(2)")
    )
  }

  @Test def trying_to_add_a_constraint_that_already_exists() {
    parseAndExecute("CREATE CONSTRAINT ON (person:Person) ASSERT person.name IS UNIQUE")

    expectError("CREATE CONSTRAINT ON (person:Person) ASSERT person.name IS UNIQUE",
      vLegacy -> String.format("Already constrained CONSTRAINT ON ( person:Person ) ASSERT person.name IS UNIQUE.")
    )
  }

  @Test def drop_a_non_existent_constraint() {
    expectError("DROP CONSTRAINT ON (person:Person) ASSERT person.name IS UNIQUE",
      vLegacy -> String.format("Constraint not found")
    )
  }

  @Test def create_without_specifying_direction_should_fail() {
    expectError("CREATE (a)-[:FOO]-(b) RETURN a,b",
      vLegacy -> String.format("Relationships need to have a direction.")
    )
  }

  @Test def create_without_specifying_direction_should_fail2() {
    expectError("CREATE (a)<-[:FOO]->(b) RETURN a,b",
      vLegacy -> String.format("Relationships need to have a direction.")
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
    assertThat(error.offset, equalTo(Some(expectedOffset): Option[Int]))
  }

  def executeQuery(version: CypherVersion, query: String) {
    val qWithVer = version match {
      case `vDefault` => query
      case _          => s"cypher ${version.name} " + query
    }
    engine.execute(qWithVer).toList
  }
}
