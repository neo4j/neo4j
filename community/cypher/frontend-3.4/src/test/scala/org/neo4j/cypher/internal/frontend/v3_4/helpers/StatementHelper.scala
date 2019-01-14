/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.helpers

import org.neo4j.cypher.internal.frontend.v3_4.ast.Statement
import org.neo4j.cypher.internal.frontend.v3_4.semantics.{Scope, SemanticCheckResult, SemanticFeature, SemanticState}
import org.scalatest.Assertions

object StatementHelper extends Assertions {

  implicit class RichStatement(ast: Statement) {
    def semanticState(features: SemanticFeature*): SemanticState =
      ast.semanticCheck(SemanticState.clean.withFeatures(features: _*)) match {
        case SemanticCheckResult(state, errors) =>
          if (errors.isEmpty) {
            state
          } else
            fail(s"Failure during semantic checking of $ast with errors $errors")
      }

    def scope: Scope = semanticState().scopeTree
  }
}
