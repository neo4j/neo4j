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
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisToolingErrorWithGqlInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.expressions.DoubleLiteral
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IntegerLiteral
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.PathExpression
import org.neo4j.cypher.internal.expressions.SubqueryExpression
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTNumber
import org.neo4j.cypher.internal.util.symbols.TypeSpec

import java.lang.Double._

// Skip/Limit
trait ASTSlicingPhrase extends SemanticCheckable with SemanticAnalysisTooling {
  self: ASTNode =>
  def name: String
  def expression: Expression
  def semanticCheck: SemanticCheck = checkExpressionIsStaticInt(expression, name, acceptsZero = true)

  def semanticCheckWithUpperBound(upperBound: Long): SemanticCheck =
    checkExpressionIsStaticInt(expression, name, acceptsZero = true, upperBound)
}

object ASTSlicingPhrase extends SemanticAnalysisTooling {

  /**
   * Checks that the given expression
   *
   *  - contains no variable references
   *  - does not try to read the graph
   *
   * @param expression  the expression to check
   * @param name        the name of the construct. Used for error messages.
   * @param acceptsZero if `true` then 0 is an accepted value, otherwise not.
   * @param upperBound the upper bound of the value (by default Long.MaxValue)
   * @return a SemanticCheck
   */
  def checkExpressionIsStatic(
    expression: Expression,
    name: String,
    expectedType: TypeSpec
  ): SemanticCheck =
    // We need to check doesNotTouchTheGraph first. If we find a SubqueryExpression we already have an error,
    // and it would not be safe to run containsNoVariables, since these SubqueryExpression haven't computed their
    // scopeDependencies yet. Therefore we use `ifOkChain`.
    doesNotTouchTheGraph(expression, name) ifOkChain
      containsNoVariables(expression, name) chain
      SemanticExpressionCheck.simple(expression) chain
      expectType(expectedType, expression)

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
   * @param upperBound the upper bound of the value (by default Long.MaxValue)
   * @return a SemanticCheck
   */
  def checkExpressionIsStaticInt(
    expression: Expression,
    name: String,
    acceptsZero: Boolean,
    upperBound: Long = Long.MaxValue
  ): SemanticCheck =
    // We need to check doesNotTouchTheGraph first. If we find a SubqueryExpression we already have an error,
    // and it would not be safe to run containsNoVariables, since these SubqueryExpression haven't computed their
    // scopeDependencies yet. Therefore we use `ifOkChain`.
    doesNotTouchTheGraph(expression, name) ifOkChain
      containsNoVariables(expression, name) chain
      literalShouldBeUnsignedInteger(expression, name, acceptsZero, upperBound) chain
      SemanticExpressionCheck.simple(expression) chain
      expectType(CTInteger.covariant, expression)

  /**
   * Checks that the given expression
   *
   *  - contains no variable references
   *  - does not try to read the graph
   *  - is a CTNumber
   *  - is non-negative, positive or anything, depending on `acceptsZero` and `acceptsNegative`
   *
   * @param expression      the expression to check
   * @param name            the name of the construct. Used for error messages.
   * @param acceptsZero     if `true` then 0 is an accepted value, otherwise not.
   * @param acceptsNegative if `true` then negative values are accepted, otherwise not.
   * @return a SemanticCheck
   */
  def checkExpressionIsStaticNumber(
    expression: Expression,
    name: String,
    acceptsZero: Boolean,
    acceptsNegative: Boolean
  ): SemanticCheck =
    // We need to check doesNotTouchTheGraph first. If we find a SubqueryExpression we already have an error,
    // and it would not be safe to run containsNoVariables, since these SubqueryExpression haven't computed their
    // scopeDependencies yet. Therefore we use `ifOkChain`.
    doesNotTouchTheGraph(expression, name) ifOkChain
      containsNoVariables(expression, name) chain
      literalShouldBeNumber(expression, name, acceptsZero, acceptsNegative) chain
      SemanticExpressionCheck.simple(expression) chain
      expectType(CTNumber.covariant, expression)

  private def containsNoVariables(expression: Expression, name: String): SemanticCheck = {
    val deps = expression.dependencies
    if (deps.nonEmpty) {
      val id = deps.toSeq.minBy(_.position)
      error(SemanticError.notStaticallyInferrableVariable(name, id.position))
    } else SemanticCheck.success
  }

  private def doesNotTouchTheGraph(expression: Expression, name: String): SemanticCheck = {
    val badExpressionFound = expression.folder.treeExists {
      case _: SubqueryExpression |
        _: PathExpression =>
        true
    }
    when(badExpressionFound) {
      error(SemanticError.notStaticallyInferrablePattern(name, expression.position))
    }
  }

  private def literalShouldBeUnsignedInteger(
    expression: Expression,
    name: String,
    acceptsZero: Boolean,
    upperBound: Long
  ): SemanticCheck = {
    try {
      expression match {
        case i: IntegerLiteral if i.value > 0 && i.value <= upperBound                 => SemanticCheck.success
        case i: IntegerLiteral if i.value == 0 && acceptsZero && i.value <= upperBound => SemanticCheck.success
        case lit: Literal =>
          val accepted = if (acceptsZero) "non-negative" else "positive"
          val lowerBound = if (acceptsZero) 0 else 1
          val upperBoundText = if (upperBound != Long.MaxValue) s" smaller than or equal to $upperBound" else ""
          SemanticAnalysisToolingErrorWithGqlInfo.specifiedNumberOutOfRangeError(
            name,
            "INTEGER NOT NULL",
            lowerBound,
            upperBound,
            lit.asCanonicalStringVal,
            s"Invalid input. '${lit.asCanonicalStringVal}' is not a valid value. Must be a $accepted integer$upperBoundText.",
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

  private def literalShouldBeNumber(
    expression: Expression,
    name: String,
    acceptsZero: Boolean,
    acceptsNegative: Boolean
  ): SemanticCheck = {
    try {
      expression match {
        case i: IntegerLiteral if i.value > 0                      => SemanticCheck.success
        case i: IntegerLiteral if i.value == 0 && acceptsZero      => SemanticCheck.success
        case i: IntegerLiteral if i.value < 0 && acceptsNegative   => SemanticCheck.success
        case i: DoubleLiteral if i.value > 0.0d                    => SemanticCheck.success
        case i: DoubleLiteral if i.value == 0.0d && acceptsZero    => SemanticCheck.success
        case i: DoubleLiteral if i.value < 0.0d && acceptsNegative => SemanticCheck.success
        case lit: Literal =>
          val accepted = (acceptsZero, acceptsNegative) match {
            case (true, true)   => ""
            case (true, false)  => " non-negative"
            case (false, true)  => " non-zero"
            case (false, false) => " positive"
          }
          val lowerBound: java.lang.Number =
            if (acceptsNegative) valueOf(Double.MinValue)
            else if (acceptsZero) valueOf(0.0d)
            else valueOf(1.0d)
          SemanticAnalysisToolingErrorWithGqlInfo.specifiedNumberOutOfRangeError(
            name,
            "NUMBER",
            lowerBound,
            Double.MaxValue,
            lit.asCanonicalStringVal,
            s"Invalid input. '${lit.asCanonicalStringVal}' is not a valid value. Must be a$accepted number.",
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
