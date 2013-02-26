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
import org.junit.Assert._
import org.junit.{Ignore, Test}

class ErrorMessagesTest extends ExecutionEngineHelper with Assertions with StringExtras {
  @Test def noReturnColumns() {
    expectError(
      "start s = node(0) return",
      "return column list expected")
  }

  @Test def badNodeIdentifier() {
    expectError(
      "START a = node(0) MATCH a-[WORKED_ON]-, return a",
      "expected an expression that is a node")
  }

  @Test def badStart() {
    expectError(
      "starta = node(0) return a",
      "expected START or CREATE")
  }

  @Test def functionDoesNotExist() {
    expectSyntaxError(
      "START a = node(0) return dontDoIt(a)",
      "unknown function", 36)
  }

  @Test def noIndexName() {
    expectSyntaxError(
      "start a = node(name=\"sebastian\") match a-[:WORKED_ON]-b return b",
      "expected node id, or *", 15)
  }

  @Test def aggregateFunctionInWhere() {
    expectError(
      "START a = node(0) WHERE count(a) > 10 RETURN a",
      "Can't use aggregate functions in the WHERE clause.")
  }

  @Test def twoIndexQueriesInSameStart() {
    expectSyntaxError(
      "start a = node:node_auto_index(name=\"sebastian\",name=\"magnus\") return a",
      "Unclosed parenthesis",
      47)
  }

  @Test def semiColonInMiddleOfQuery() {
    expectSyntaxError(
      """start n=node(2)
    match n<-[r:IS_A]-p
    ;
    start n=node(2)
    match p-[IS_A]->n, p-[r:WORKED_ON]->u
    return p, sum(r.months)""",
      "expected return clause",
      44)
  }

  @Test def badMatch2() {
    expectSyntaxError(
      "start p=node(2) match p-[:IS_A]>dude return dude.name",
      "expected -", 31)
  }

  @Test def badMatch3() {
    expectSyntaxError(
      "start p=node(2) match p-[:IS_A->dude return dude.name",
      "unclosed bracket", 30)
  }

  @Test def badMatch4() {
    expectSyntaxError(
      "start p=node(2) match p-[!]->dude return dude.name",
      "expected relationship information", 25)
  }

  @Test def badMatch5() {
    expectSyntaxError(
      "start p=node(2) match p[:likes]->dude return dude.name",
      "failed to parse MATCH pattern", 24)
  }

  @Test def badMatch7() {
    expectSyntaxError(
      "start p=node(2) match p->dude return dude.name",
      "failed to parse MATCH pattern", 24)
  }

  @Test def badMatch8() {
    expectSyntaxError(
      "start p=node(2) match p->dude return dude.name",
      "failed to parse MATCH pattern", 24)
  }

  @Ignore @Test def missingComaBetweenColumns() {
    expectSyntaxError(
      "start p=node(2) return sum wo.months",
      "Expected comma separated list of returnable values",
      22)
  }

  @Ignore @Test def missingComaBetweenStartNodes() {
    expectSyntaxError(
      "start a=node(0) b=node(1) return a",
      "Expected comma separated list of returnable values",
      22)
  }

  @Test def tooManyLinksInShortestPath() {
    expectSyntaxError(
      "start a=node(2),b=node(1) match shortestPath(a-->x-->b)  return sum wo.months",
      "expected single path segment",
      54)
  }

  @Test def noEqualsSignInStart() {
    expectSyntaxError(
      "start r:relationship:rels() return r",
      "expected identifier assignment",
      7)
  }

  @Test def relTypeInsteadOfRelIdInStart() {
    expectSyntaxError(
      "start r = relationship(:WORKED_ON) return r",
      "expected relationship id, or *",
      23)
  }

  @Test def nonExistingProperty() {
    expectError(
      "start n = node(0) return n.month",
      "The property 'month' does not exist on Node[0]")
  }

  @Test def noNodeIdInStart() {
    expectSyntaxError(
      "start r = node() return r",
      "expected node id, or *",
      15)
  }

  @Test def startExpressionWithoutIdentifier() {
    expectSyntaxError(
      "start a = node:node_auto_index(name=\"magnus\"),node:node_auto_index(name=\"sebastian) return b,c",
      "expected identifier assignment",
      50)
  }

  @Test def functions_and_stuff_have_to_be_renamed_when_sent_through_with() {
    expectError(
      "START a=node(0) with a, count(*) return a",
      "These columns can't be listen in the WITH statement without renaming: count(*)")
  }

  @Test def missing_dependency_correctly_reported() {
    expectError(
      "START a=node(0) CREATE UNIQUE a-[:KNOWS]->(b {name:missing}) RETURN b",
      "Unknown identifier `missing`")
  }

  @Test def missing_create_dependency_correctly_reported() {
    expectNotFoundError(
      "START a=node(0) CREATE a-[:KNOWS]->(b {name:missing}) RETURN b",
      "Unknown identifier `missing`")
  }

  @Test def missing_set_dependency_correctly_reported() {
    expectError(
      "START a=node(0) SET a.name = missing RETURN a",
      "Unknown identifier `missing`")
  }

  @Test def create_with_identifier_already_existing() {
    expectError(
      "START a=node(0) CREATE a = {name:'foo'} RETURN a",
      "Can't create `a` with properties here. It already exists in this context")
  }

  @Test def create_with_identifier_already_existing2() {
    expectError(
      "START a=node(0) CREATE UNIQUE (a {name:'foo'})-[:KNOWS]->() RETURN a",
      "Can't create `a` with properties here. It already exists in this context")
  }

  @Test def type_of_identifier_is_wrong() {
    expectError(
      "start n=node(0) with [n] as users MATCH users-->messages RETURN messages",
      "Expected `users` to be a Node but it was a Collection")
  }

  @Test def warn_about_exclamation_mark() {
    expectError(
      "start n=node(0) where n.foo != 2 return n",
      "Cypher does not support != for inequality comparisons. It's used for nullable properties instead.\nYou probably meant <> instead. Read more about this in the operators chapter in the manual.")
  }

  @Test def warn_about_type_error() {
    expectError(
      "START p=node(0) MATCH p-[r*]->() WHERE r.foo = 'apa' RETURN r",
      "Expected `r` to be a Map but it was a Collection")
  }

  @Test def error_when_using_properties_on_relationships_in_match() {
    expectError(
      "START p=node(0) MATCH p-[r {a:'foo'}]->() RETURN r",
      "Properties on pattern elements are not allowed in MATCH")
  }

  @Test def error_when_using_properties_on_relationships_in_match2() {
    expectError(
      "START p=node(0) MATCH p-[r]->({a:'foo'}) RETURN r",
      "Properties on pattern elements are not allowed in MATCH")
  }

  @Test def missing_something_to_delete() {
    expectError(
      "START p=node(0) DELETE x",
      "Unknown identifier `x`")
  }

  @Test def aggregations_must_be_included_in_return() {
    expectError(
      "START a=node(0) RETURN a ORDER BY count(*)",
      "Aggregation expressions must be listed in the RETURN clause to be used in ORDER BY")
  }

  @Test def unions_must_have_the_same_columns() {
    expectError(
"""START a=node(0) RETURN a
   UNION
   START b=node(0) RETURN b""",
      "All sub queries in an UNION must have the same column names")
  }

  @Test def can_not_mix_union_and_union_all() {
    expectError(
"""START a=node(0) RETURN a
   UNION
   START a=node(0) RETURN a
   UNION ALL
   START a=node(0) RETURN a""",
      "can't mix UNION and UNION ALL")
  }

  @Test def can_not_use_optional_pattern_as_predicate() {
    expectError(
      "START a=node(1) RETURN a-[?]->()",
      "Optional patterns cannot be used as predicates")
  }

  @Test def creating_an_index_twice_should_return_sensible_error() {
    createIndex("LabelName", "Prop")

    expectError(
      "CREATE INDEX ON :LabelName(Prop)",
      "Property `Prop` is already indexed for label `LabelName`.")
  }

  private def expectError(query: String, expectedError: String):CypherException = {
    val error = intercept[CypherException](engine.execute(query).toList)
    val s = """
Wrong error message produced: %s
Expected: %s
     Got: %s
            """.format(query, expectedError, error)

    if(!error.getMessage.contains(expectedError)) {
      fail(s)
    }

    error
  }

  private def expectNotFoundError(query: String, expectedError: String):CypherException = {
    val error = intercept[CypherException](engine.execute(query).toList)
    val s = """
Wrong error message produced: %s
Expected: %s
     Got: %s
""".format(query, expectedError, error)


    if(!error.getMessage.contains(expectedError)) {
      fail(s)
    }

    error
  }

  private def expectSyntaxError(query: String, expectedError: String, expectedOffset: Int) {
    expectError(query, expectedError) match {
      case e: SyntaxException =>
        val s = query + "\n" + e.toString()
        assertEquals(s, Some(expectedOffset), e.offset)

      case e => fail("Expected a SyntaxException, but got: " + e.getMessage)
    }
  }
}
