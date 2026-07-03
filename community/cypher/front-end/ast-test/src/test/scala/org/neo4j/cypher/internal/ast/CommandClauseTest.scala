/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.CypherVersionHelpers.versionedSemanticContext
import org.neo4j.cypher.internal.CypherVersionTestSupport
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes

class CommandClauseTest extends CypherFunSuite with AstConstructionTestSupport with CypherVersionTestSupport {

  private val initialState = SemanticState.clean

  // Checks that the rewritten query only causes the expected 22N04 error
  // and does not cause an unexpected 42I37 - Invalid use of return star
  testVersions("TERMINATE TRANSACTION 'neo4j-transaction-2' YIELD nope") { version =>
    val rewrittenTerminate = SingleQuery(List(
      TerminateTransactionsClause(
        List(
          ShowAndTerminateColumn("transactionId", CTString),
          ShowAndTerminateColumn("username", CTString),
          ShowAndTerminateColumn("message", CTString)
        ),
        ExpressionNames(StringLiteral("neo4j-transaction-2")(pos)),
        List(CommandResultItem("nope", Variable("nope")(pos, false))(pos)),
        false,
        None,
        None
      )(pos),
      With(
        false,
        ReturnItems(AdditiveProjection, List(), Some(List("nope")))(pos),
        None,
        None,
        None,
        None,
        None,
        ParsedAsYield
      )(pos),
      Return(
        false,
        ReturnItems(AdditiveProjection, List(), Some(List("nope")))(pos),
        None,
        None,
        None,
        None,
        Set(),
        ReturnAddedInRewrite,
        ImportingWithSubqueryCall
      )(pos)
    ))(pos)

    rewrittenTerminate.semanticCheck.run(initialState, versionedSemanticContext(version)).errors shouldBe
      SemanticCheckResult.error(
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(pos.offset, pos.line, pos.column)
          .withCause(
            ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_22N04)
              .atPosition(pos.offset, pos.line, pos.column)
              .withParam(GqlParams.StringParam.input, "nope")
              .withParam(GqlParams.StringParam.context, "column name")
              .withParam(
                GqlParams.ListParam.inputList,
                java.util.List.of("message", "transactionId", "username")
              ).build()
          ).build(),
        initialState,
        "Trying to YIELD non-existing column: `nope`",
        pos
      ).errors
  }
}
