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

import org.neo4j.cypher.internal.ast.ASTSlicingPhrase.checkExpressionIsStaticInt
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.SubqueryExpression
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.symbols.CTInteger

// Skip/Limit
trait ASTSlicingPhrase extends SemanticCheckable with SemanticAnalysisTooling {
  self: ASTNode =>
  def name: String
  def expression: Expression
  def semanticCheck: SemanticCheck = checkExpressionIsStaticInt(expression, name, acceptsZero = true)
}

object ASTSlicingPhrase extends SemanticAnalysisTooling {

  /**
   * Checks that the given expression
   *
   *  - contains no variable references
   *  - does not try to read the graph
   *  - is a CTInteger
   *  - is either non-negative or positive, depending on `acceptsZero`
   *
   * @param expression  the expression to check
   * @param name        the name of the construct. Used for error messages.
   * @param acceptsZero if `true` then 0 is an accepted value, otherwise not.
   * @return a SemanticCheck
   */
  def checkExpressionIsStaticInt(expression: Expression, name: String, acceptsZero: Boolean): SemanticCheck =
    // We need to check doesNotTouchTheGraph first. If we find a SubqueryExpression we already have an error,
    // and it would not be safe to run containsNoVariables, since these SubqueryExpression haven't computed their
    // scopeDependencies yet. Therefore we use `ifOkChain`.
    doesNotTouchTheGraph(expression, name) ifOkChain
      containsNoVariables(expression, name) chain
      literalShouldBeUnsignedInteger(expression, name, acceptsZero) chain
      SemanticExpressionCheck.simple(expression) chain
      expectType(CTInteger.covariant, expression)

  private def containsNoVariables(expression: Expression, name: String): SemanticCheck = {
    val deps = expression.dependencies
    if (deps.nonEmpty) {
      val id = deps.toSeq.minBy(_.position)
      error(
        s"It is not allowed to refer to variables in $name, so that the value for $name can be statically calculated.",
        id.position
      )
    } else SemanticCheck.success
  }

  private def doesNotTouchTheGraph(expression: Expression, name: String): SemanticCheck = {
    val badExpressionFound = expression.folder.treeExists {
      case _: SubqueryExpression |
        _: PathExpression =>
        true
    }
    when(badExpressionFound) {
      error(
        s"It is not allowed to use patterns in the expression for $name, so that the value for $name can be statically calculated.",
        expression.position
      )
    }
  }

  private def literalShouldBeUnsignedInteger(
    expression: Expression,
    name: String,
    acceptsZero: Boolean
  ): SemanticCheck = {
    try {
      expression match {
        case _: UnsignedDecimalIntegerLiteral                              => SemanticCheck.success
        case i: SignedDecimalIntegerLiteral if i.value > 0                 => SemanticCheck.success
        case i: SignedDecimalIntegerLiteral if i.value == 0 && acceptsZero => SemanticCheck.success
        case lit: Literal =>
          val accepted = if (acceptsZero) "non-negative" else "positive"
          error(
            s"Invalid input. '${lit.asCanonicalStringVal}' is not a valid value. Must be a $accepted integer.",
            lit.position
          )
        case _ => SemanticCheck.success
      }
    } catch {
      case _: NumberFormatException =>
        // We rely on getting a SemanticError from SemanticExpressionCheck.simple(expression)
        SemanticCheck.success
    }
  }
}
