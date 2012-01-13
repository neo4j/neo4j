package org.neo4j.cypher

/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import internal.StringExtras
import org.scalatest.Assertions
import org.junit.{Ignore, Test}

@Ignore
class ErrorMessagesTest extends ExecutionEngineHelper with Assertions with StringExtras {
  @Test def noReturnColumns() {
    expectError(
      "start s = node(0) return",
      "Expected comma separated list of returnable values")
  }

  @Test def badNodeIdentifier() {
    expectError(
      "START a = node(0) MATCH a-[WORKED_ON]-[30] return a",
      "Expected node, got '[30]'")
  }

  @Test def functionDoesNotExist() {
    expectSyntaxError(
      "START a = node(0) return dontDoIt(a)",
      "No function named 'dontDoIt' exists.", 22)
  }

  @Test def noIndexName() {
    expectSyntaxError(
      "start a = node(name=\"sebastian\") match a-[:WORKED_ON]-b return b",
      "Expected index name", 22)
  }

  @Test def aggregateFunctionInWhere() {
    expectError(
      "START a = node(0) WHERE count(a) > 10 RETURN a",
      "Aggregate functions (like 'count') are not valid in the WHERE clause.")
  }


  @Test def twoIndexQueriesInSameStart() {
    expectSyntaxError(
      "start a = node:node_auto_index(name=\"sebastian\",name=\"magnus\") return a",
      "')' expected but ',' found",
      22)
  }

  @Test def semiColonInMiddleOfQuery() {
    expectSyntaxError(
      """start n=node(2)
    match n<-[r:IS_A]-p
    ;
    start n=node(2)
    match p-[IS_A]->n, p-[r:WORKED_ON]->u
    return p, sum(r.months)""",
      "Return expected but ';' found",
      42)
  }

  @Test def extraGTSymbol() {
    expectSyntaxError(
      "start p=node(2) match p->[:IS_A]->dude return dude.name",
      "'-' expected but '>' found",
      22)
  }

  @Test def sumOnNonNumericalValue() {
    createNode("prop" -> "fish")
    expectError(
      "START n=node(1) RETURN sum(n.prop)",
      "sum(n.prop) - Sum can only handle numerical values. Node[77]{prop->'fish'}.")
  }

  @Test def missingComaBetweenColumns() {
    expectSyntaxError(
      "start p=node(2) return sum wo.months",
      "Expected comma separated list of returnable values",
      22)
  }

  @Test def noEqualsSignInStart() {
    expectSyntaxError(
      "start r:relationship:rels() return r",
      "'=' expected but ':' found",
      22)
  }

  @Test def relTypeInsteadOfRelIdInStart() {
    expectSyntaxError(
      "start r = relationship(:WORKED_ON) return r",
      "Expected an indexquery, got ':WORKED_ON'",
      22)
  }

  @Test def nonExistingProperty() {
    expectError(
      "start n = node(0) return n.month",
      "The property 'month' does not exist on Node[0]")
  }

  @Test def noNodeIdInStart() {
    expectSyntaxError(
      "start r = node() return r",
      "Need at least one node id to bind to identifier 'r'",
      22)
  }

  @Test def startExpressionWithoutIdentifier() {
    expectSyntaxError(
      "start a = node:node_auto_index(name=\"magnus\"),node:node_auto_index(name=\"sebastian) return b,c",
      "Need an identifier to bind to",
      22)
  }

  @Test def noPredicatesInWhereClause() {
    expectSyntaxError(
      "START a=node(0) where return a",
      "?",
      22)
  }

  private def expectError[T <: CypherException](query: String, expectedError: String)(implicit manifest: Manifest[T]): T = {
    val error = intercept[T](engine.execute(query).toList)
    assert(expectedError === error.getMessage)
    error
  }

  private def expectSyntaxError(query: String, expectedError: String, expectedOffset: Int) {
    assert(expectedOffset === expectError[SyntaxException](query, expectedError).offset)
  }

}
