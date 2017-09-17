/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.apa.v3_4.ASTNode
import org.neo4j.cypher.internal.frontend.v3_4.semantics._
import org.neo4j.cypher.internal.frontend.v3_4.SemanticCheck
import org.neo4j.cypher.internal.apa.v3_4.symbols.CTInteger
import org.neo4j.cypher.internal.v3_4.expressions._

// Skip/Limit
trait ASTSlicingPhrase extends SemanticCheckable with SemanticAnalysisTooling {
  self: ASTNode =>
  def name: String
  def dependencies: Set[Variable] = expression.dependencies
  def expression: Expression

  def semanticCheck: SemanticCheck =
    containsNoVariables chain
      literalShouldBeUnsignedInteger chain
      SemanticExpressionCheck.simple(expression) chain
      expectType(CTInteger.covariant, expression)

  private def containsNoVariables: SemanticCheck = {
    val deps = dependencies
    if (deps.nonEmpty) {
      val id = deps.toSeq.minBy(_.position)
      error(s"It is not allowed to refer to variables in $name", id.position)
    }
    else SemanticCheckResult.success
  }

  private def literalShouldBeUnsignedInteger: SemanticCheck = {
    expression match {
      case _: UnsignedDecimalIntegerLiteral => SemanticCheckResult.success
      case i: SignedDecimalIntegerLiteral if i.value >= 0 => SemanticCheckResult.success
      case lit: Literal => error(s"Invalid input '${lit.asCanonicalStringVal}' is not a valid value, " +
                                  "must be a positive integer", lit.position)
      case _ => SemanticCheckResult.success
    }
  }
}
