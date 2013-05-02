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

import internal.StringExtras
import org.scalatest.Assertions
import org.hamcrest.CoreMatchers._
import org.junit.Assert._
import org.junit.{Ignore, Test}
import CypherVersion._

class ErrorMessagesTest extends ExecutionEngineHelper with Assertions with StringExtras {
  @Test def noReturnColumns() {
    expectError("start s = node(0) return",
      v2_0 -> "return column list expected"
    )
  }

  @Test def badNodeIdentifier() {
    expectError("START a = node(0) MATCH a-[WORKED_ON]-, return a",
      v2_0 -> "expected an expression that is a node"
    )
  }

  @Test def badStart() {
    expectError("starta = node(0) return a",
      v2_0 -> "invalid start of query"
    )
  }

  @Test def functionDoesNotExist() {
    expectSyntaxError("START a = node(0) return dontDoIt(a)",
      v2_0 -> ("unknown function", 36)
    )
  }

  @Test def noIndexName() {
    expectSyntaxError("start a = node(name=\"sebastian\") match a-[:WORKED_ON]-b return b",
      v2_0 -> ("expected node id, or *", 15)
    )
  }

  @Test def aggregateFunctionInWhere() {
    expectError("START a = node(0) WHERE count(a) > 10 RETURN a",
      v2_0 -> "Can't use aggregate functions in the WHERE clause."
    )
  }

  @Test def twoIndexQueriesInSameStart() {
    expectSyntaxError("start a = node:node_auto_index(name=\"sebastian\",name=\"magnus\") return a",
      v2_0 -> ("Unclosed parenthesis", 47)
    )
  }

  @Ignore // We feel guilty
  @Test def semiColonInMiddleOfQuery() {
    expectSyntaxError(
      """start n=node(2)
         match n<-[r:IS_A]-p
         ;
         start n=node(2)
         match p-[IS_A]->n, p-[r:WORKED_ON]->u
         return p, sum(r.months)""",
      v2_0 -> ("expected return clause", 44)
    )
  }

  @Test def badMatch2() {
    expectSyntaxError("start p=node(2) match p-[:IS_A]>dude return dude.name",
      v2_0 -> ("expected -", 31)
    )
  }

  @Test def badMatch3() {
    expectSyntaxError("start p=node(2) match p-[:IS_A->dude return dude.name",
      v2_0 -> ("unclosed bracket", 30)
    )
  }

  @Test def badMatch4() {
    expectSyntaxError("start p=node(2) match p-[!]->dude return dude.name",
      v2_0 -> ("expected relationship information", 25)
    )
  }

  @Test def badMatch5() {
    expectSyntaxError("start p=node(2) match p[:likes]->dude return dude.name",
      v2_0 -> ("expected valid query body", 23)
    )
  }

  @Ignore @Test def missingComaBetweenColumns() {
    expectSyntaxError("start p=node(2) return sum wo.months",
      v2_0 -> ("Expected comma separated list of returnable values", 22)
    )
  }

  @Ignore @Test def missingComaBetweenStartNodes() {
    expectSyntaxError("start a=node(0) b=node(1) return a",
      v2_0 -> ("Expected comma separated list of returnable values", 22)
    )
  }

  @Test def tooManyLinksInShortestPath() {
    expectSyntaxError("start a=node(2),b=node(1) match shortestPath(a-->x-->b)  return sum wo.months",
      v2_0 -> ("expected single path segment", 54)
    )
  }

  @Test def noEqualsSignInStart() {
    expectSyntaxError("start r:relationship:rels() return r",
      v2_0 -> ("expected identifier assignment", 7)
    )
  }

  @Test def relTypeInsteadOfRelIdInStart() {
    expectSyntaxError("start r = relationship(:WORKED_ON) return r",
      v2_0 -> ("expected relationship id, or *", 23)
    )
  }

  @Test def nonExistingProperty() {
    expectError("start n = node(0) return n.month",
      v2_0 -> "The property 'month' does not exist on Node[0]"
    )
  }

  @Test def noNodeIdInStart() {
    expectSyntaxError("start r = node() return r",
      v2_0 -> ("expected node id, or *", 15)
    )
  }

  @Test def startExpressionWithoutIdentifier() {
    expectSyntaxError("start a = node:node_auto_index(name=\"magnus\"),node:node_auto_index(name=\"sebastian) return b,c",
      v2_0 -> ("expected identifier assignment", 50)
    )
  }

  @Test def functions_and_stuff_have_to_be_renamed_when_sent_through_with() {
    expectError("START a=node(0) with a, count(*) return a",
      v2_0 -> "These columns can't be listen in the WITH statement without renaming: count(*)"
    )
  }

  @Test def missing_dependency_correctly_reported() {
    expectError("START a=node(0) CREATE UNIQUE a-[:KNOWS]->(b {name:missing}) RETURN b",
      v2_0 -> "Unknown identifier `missing`"
    )
  }

  @Test def missing_create_dependency_correctly_reported() {
    expectError("START a=node(0) CREATE a-[:KNOWS]->(b {name:missing}) RETURN b",
      v2_0 -> "Unknown identifier `missing`"
    )
  }

  @Test def missing_set_dependency_correctly_reported() {
    expectError("START a=node(0) SET a.name = missing RETURN a",
      v2_0 -> "Unknown identifier `missing`"
    )
  }

  @Test def create_with_identifier_already_existing() {
    expectError("START a=node(0) CREATE (a {name:'foo'}) RETURN a",
      v2_0 -> "Can't create `a` with properties here. It already exists in this context"
    )
  }

  @Test def create_with_identifier_already_existing2() {
    expectError("START a=node(0) CREATE UNIQUE (a {name:'foo'})-[:KNOWS]->() RETURN a",
      v2_0 -> "Can't create `a` with properties here. It already exists in this context"
    )
  }

  @Test def type_of_identifier_is_wrong() {
    expectError("start n=node(0) with [n] as users MATCH users-->messages RETURN messages",
      v2_0 -> "Expected `users` to be a Node but it was a Collection"
    )
  }

  @Test def warn_about_exclamation_mark() {
    expectError("start n=node(0) where n.foo != 2 return n",
      v2_0 -> "Cypher does not support != for inequality comparisons. It's used for nullable properties instead.\nYou probably meant <> instead. Read more about this in the operators chapter in the manual."
    )
  }

  @Test def warn_about_type_error() {
    expectError("START p=node(0) MATCH p-[r*]->() WHERE r.foo = 'apa' RETURN r",
      v2_0 -> "Expected `r` to be a Map but it was a Collection"
    )
  }

  @Test def error_when_using_properties_on_relationships_in_match() {
    expectError("START p=node(0) MATCH p-[r {a:'foo'}]->() RETURN r",
      v2_0 -> "Properties on pattern elements are not allowed in MATCH"
    )
  }

  @Test def error_when_using_properties_on_relationships_in_match2() {
    expectError("START p=node(0) MATCH p-[r]->({a:'foo'}) RETURN r",
      v2_0 -> "Properties on pattern elements are not allowed in MATCH"
    )
  }

  @Test def missing_something_to_delete() {
    expectError("START p=node(0) DELETE x",
      v2_0 -> "Unknown identifier `x`"
    )
  }

  @Test def aggregations_must_be_included_in_return() {
    expectError("START a=node(0) RETURN a ORDER BY count(*)",
      v2_0 -> "Aggregation expressions must be listed in the RETURN clause to be used in ORDER BY"
    )
  }

  @Test def unions_must_have_the_same_columns() {
    expectError(
      """START a=node(0) RETURN a
         UNION
         START b=node(0) RETURN b""",
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
   v2_0 -> "can't mix UNION and UNION ALL")
  }

  @Test def can_not_use_optional_pattern_as_predicate() {
    expectError("START a=node(1) RETURN a-[?]->()",
      v2_0 -> "Optional patterns cannot be used as predicates"
    )
  }

  @Test def creating_an_index_twice_should_return_sensible_error() {
    graph.createIndex("LabelName", "Prop")

    expectError("CREATE INDEX ON :LabelName(Prop)",
      v2_0 -> "Property `Prop` is already indexed for label `LabelName`."
    )
  }

  private def expectError(query: String, variants: (CypherVersion, String)*) {
    for ((version, message) <- variants) {
      expectError(version, query, message)
    }
  }

  def expectError(version: CypherVersion, query: String, expectedError: String) {
    val error = intercept[CypherException](executeQuery(version, query))
    assertThat(error.getMessage(), containsString(expectedError))
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
    engine.execute(query).toList
  }
}
