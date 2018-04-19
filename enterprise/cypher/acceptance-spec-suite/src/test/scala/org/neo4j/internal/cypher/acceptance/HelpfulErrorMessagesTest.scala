/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.InvalidSemanticsException
import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, SyntaxException}

class HelpfulErrorMessagesTest extends ExecutionEngineFunSuite with NewPlannerTestSupport {

  test("should provide sensible error message when omitting colon before relationship type on create") {
    val exception = intercept[SyntaxException](executeScalarWithAllPlannersAndRuntimes("CREATE (a)-[ASSOCIATED_WITH]->(b)"))
    val exceptionMsg = "Exactly one relationship type must be specified for CREATE. Did you forget to prefix your relationship type with a ':'?"
    exception.getMessage should include(exceptionMsg)
  }

  test("should provide sensible error message when trying to add multiple relationship types on create") {
    val exception = intercept[SyntaxException](executeScalarWithAllPlannersAndRuntimes("CREATE (a)-[:ASSOCIATED_WITH|:KNOWS]->(b)"))
    exception.getMessage should include("A single relationship type must be specified for CREATE")
  }

  test("should provide sensible error message when omitting colon before relationship type on merge") {
    val exception = intercept[SyntaxException](executeScalarWithAllPlannersAndRuntimes("MERGE (a)-[ASSOCIATED_WITH]->(b)"))
    val exceptionMsg = "Exactly one relationship type must be specified for MERGE. Did you forget to prefix your relationship type with a ':'?"
    exception.getMessage should include(exceptionMsg)
  }

  test("should provide sensible error message when trying to add multiple relationship types on merge") {
    val exception = intercept[SyntaxException](executeScalarWithAllPlannersAndRuntimes("MERGE (a)-[:ASSOCIATED_WITH|:KNOWS]->(b)"))
    exception.getMessage should include("A single relationship type must be specified for MERGE")
  }

  test("should provide sensible error message for invalid regex syntax together with index") {

    graph.execute("CREATE (n:Person {text:'abcxxxdefyyyfff'})")

    val exception = intercept[InvalidSemanticsException](
     executeWithCostPlannerAndInterpretedRuntimeOnly("MATCH (x:Person) WHERE x.text =~ '*xxx*yyy*' RETURN x.text"))

    exception.getMessage should include("Invalid Regex:")
  }

  test("should give correct error message with invalid number literal in a subtract") {
    a[SyntaxException] shouldBe thrownBy {
      innerExecute("with [1a-1] as list return list")
    }
  }
}
