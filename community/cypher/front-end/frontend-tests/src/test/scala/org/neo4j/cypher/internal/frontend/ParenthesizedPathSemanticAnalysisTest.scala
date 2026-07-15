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
package org.neo4j.cypher.internal.frontend

import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlParams
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.scalatest.LoneElement

import scala.jdk.CollectionConverters.SeqHasAsJava

class ParenthesizedPathSemanticAnalysisTest extends SemanticAnalysisTestSuite with LoneElement {

  test("can use sub-path variable in WHERE") {
    val q =
      """
        |MATCH SHORTEST 1 (p = (a)-[r]->+(b) WHERE length(p) % 2 = 0)
        |RETURN b
        |""".stripMargin
    run(q).hasNoErrors
  }

  test("cannot use path variable from the same MATCH clause in WHERE") {
    val q =
      """MATCH p = SHORTEST 1 ((a)-[r]->+(b) WHERE length(p) % 2 = 0)
        |RETURN b
        |""".stripMargin

    run(q).hasAtLeastOneGqlErrorIn(_ =>
      Seq((
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(6, 1, 7)
          .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I21)
            .atPosition(6, 1, 7)
            .withParam(GqlParams.ListParam.variableList, Seq("p").asJava)
            .withParam(GqlParams.StringParam.pat, "((a) (()-[r]->())+ (b) WHERE length(p) % 2 = 0)")
            .build())
          .build(),
        """From within a parenthesized path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
          |In this case, `p` is defined in the same `MATCH` clause as ((a) (()-[r]->())+ (b) WHERE length(p) % 2 = 0).""".stripMargin,
        InputPosition(6, 1, 7)
      ))
    )
  }

  test("can use path variable from a previous MATCH clause in WHERE") {
    val q =
      """
        |MATCH p = (x)-->(y)
        |MATCH SHORTEST 1 ((a)-[r]->+(b) WHERE length(p) % 2 = 0)
        |RETURN b
        |""".stripMargin

    run(q).hasNoErrors
  }

  test("can not use a variable from the same MATCH clause in a subquery expression") {
    val q =
      """MATCH p = SHORTEST 1 ((a)-[r]->+(b) WHERE 0 = COUNT { (x)-->(y) WHERE length(p) % 2 = 0} )
        |RETURN b
        |""".stripMargin

    run(q).hasAtLeastOneGqlErrorIn(_ =>
      Seq((
        ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42001)
          .atPosition(6, 1, 7)
          .withCause(ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42I21)
            .atPosition(6, 1, 7)
            .withParam(GqlParams.ListParam.variableList, Seq("p").asJava)
            .withParam(
              GqlParams.StringParam.pat,
              """((a) (()-[r]->())+ (b) WHERE 0 = COUNT {
                |  MATCH (x)-->(y)
                |    WHERE length(p) % 2 = 0
                |})""".stripMargin
            )
            .build())
          .build(),
        """From within a parenthesized path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
          |In this case, `p` is defined in the same `MATCH` clause as ((a) (()-[r]->())+ (b) WHERE 0 = COUNT {
          |  MATCH (x)-->(y)
          |    WHERE length(p) % 2 = 0
          |}).""".stripMargin,
        InputPosition(6, 1, 7)
      ))
    )
  }

  test("can not re-declare a path variable from the outer MATCH clause in a subquery expression") {
    val q =
      """
        |MATCH p = ()--()
        |MATCH ANY (p = ()--+())
        |RETURN *""".stripMargin

    run(q).hasError(
      GqlHelper.getGql42001_42N59("p", 29, 3, 12),
      """Variable `p` already declared""".stripMargin,
      InputPosition(29, 3, 12)
    )
  }

  test("can not shadow a path variable in a subquery expression") {
    val q =
      """
        |MATCH p = (a:A)-->+(b:B)
        |WHERE NOT EXISTS { ANY (p = (a)<--+(b) WHERE length(p) % 2 = 1) }
        |RETURN *""".stripMargin

    run(q).hasAllErrorMessagesIn(_ =>
      Seq(
        """The variable `p` is shadowing a variable with the same name from the outer scope and needs to be renamed""".stripMargin
      )
    )
  }
}
