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

import org.neo4j.cypher.internal.ast.semantics.Scope
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions.Variable
import org.scalatest.Assertions

object StatementHelper extends Assertions {

  implicit class RichStatement(ast: Statement) {
    private val allVariables = ast.folder.findAllByClass[Variable]

    def semanticState(features: SemanticFeature*): SemanticState =
      ast.semanticCheck(SemanticState.clean.withFeatures(features: _*)) match {
        case SemanticCheckResult(state, errors) =>
          if (errors.isEmpty) {
            state
          } else
            fail(s"Failure during semantic checking of $ast with errors $errors")
      }

    def scope: Scope = semanticState().scopeTree

    def varAt(name: String)(offset: Int): Variable =
      allVariables.find(v => v.name == name && v.position.offset == offset) match {
        case Some(value) => value
        case None =>
          val foundOffsets = allVariables.filter(v => v.name == name).map(_.position.offset)
          throw new IllegalStateException(
            s"Variable `$name` not found at position $offset. Found positions: $foundOffsets"
          )
      }
  }
}
