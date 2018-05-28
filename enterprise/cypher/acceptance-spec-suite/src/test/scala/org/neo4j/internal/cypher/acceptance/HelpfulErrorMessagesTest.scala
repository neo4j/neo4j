/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
