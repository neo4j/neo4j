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

import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.gqlstatus.GqlHelper

import scala.jdk.CollectionConverters.SeqHasAsJava

class HintsSemanticAnalysisTest extends SemanticAnalysisTestSuite {

  private val expandHints: Seq[SemanticFeature] = Seq(SemanticFeature.ExpandHints)

  test("USING EXPAND VIA on a node variable should fail with a type error") {
    runWithCarets(
      """MATCH (a)-->(b)
        |USING EXPAND VIA a
        |                 ^
        |RETURN *""".stripMargin,
      semanticFeatures = expandHints
    ).hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql22G03_22N27(
          "NODE",
          "`a`",
          Seq("RELATIONSHIP", "LIST<RELATIONSHIP>").asJava,
          pos.offset,
          pos.line,
          pos.column
        ),
      "Type mismatch: expected Relationship or List<Relationship> but was Node"
    )
  }

  test("USING EXPAND VIA on an undefined variable should fail with an undefined variable error") {
    runWithCarets(
      """MATCH (a)-->(b)
        |USING EXPAND VIA undefined_r
        |                 ^
        |RETURN *""".stripMargin,
      semanticFeatures = expandHints
    ).hasErrorWithMarkedPosition(
      pos =>
        GqlHelper.getGql42001_42N62(
          "undefined_r",
          pos.offset,
          pos.line,
          pos.column
        ),
      "Variable `undefined_r` not defined"
    )
  }
}
