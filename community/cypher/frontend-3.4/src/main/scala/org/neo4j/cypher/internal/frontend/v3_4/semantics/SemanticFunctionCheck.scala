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
package org.neo4j.cypher.internal.frontend.v3_4.semantics

import org.neo4j.cypher.internal.v3_4.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.v3_4.functions._
import org.neo4j.cypher.internal.frontend.v3_4.notification.LengthOnNonPathNotification
import org.neo4j.cypher.internal.aux.v3_4.symbols.{CTAny, CTBoolean, CTList, CTPath, CTString}
import org.neo4j.cypher.internal.frontend.v3_4.{SemanticCheck, TypeGenerator, ast}
import org.neo4j.cypher.internal.v3_4.expressions._

object SemanticFunctionCheck extends SemanticAnalysisTooling {

  def check(ctx: Expression.SemanticContext, invocation: FunctionInvocation): SemanticCheck =
    invocation.function match {
      case f:AggregatingFunction =>
        when(ctx == Expression.SemanticContext.Simple) {
          error(s"Invalid use of aggregating function ${f.name}(...) in this context", invocation.position)
        } chain {
          SemanticExpressionCheck.check(ctx, invocation.arguments) chain
          semanticCheck(ctx, invocation)
        }

      case Reduce =>
        error(s"${Reduce.name}(...) requires '| expression' (an accumulation expression)", invocation.position)

      case f:Function =>
        when(invocation.distinct) {
          error(s"Invalid use of DISTINCT with function '${f.name}'", invocation.position)
        } chain SemanticExpressionCheck.check(ctx, invocation.arguments) chain semanticCheck(ctx, invocation)
    }

  protected def semanticCheck(ctx: Expression.SemanticContext, invocation: FunctionInvocation): SemanticCheck =
    invocation.function match {
      case Coalesce =>
        checkMinArgs(invocation, 1) chain
          expectType(CTAny.covariant, invocation.arguments) chain
          specifyType(leastUpperBoundsOfTypes(invocation.arguments), invocation)

      case Collect =>
        checkArgs(invocation, 1) ifOkChain {
          expectType(CTAny.covariant, invocation.arguments(0)) chain
            specifyType(types(invocation.arguments(0))(_).wrapInList, invocation)
        }

      case Exists =>
        checkArgs(invocation, 1) ifOkChain {
          expectType(CTAny.covariant, invocation.arguments.head) chain
            (invocation.arguments.head match {
              case _: Property => None
              case _: PatternExpression => None
              case _: ContainerIndex => None
              case e =>
                Some(SemanticError(s"Argument to ${invocation.name}(...) is not a property or pattern", e.position, invocation.position))
            })
        } chain specifyType(CTBoolean, invocation)

      case Head =>
        checkArgs(invocation, 1) ifOkChain {
          expectType(CTList(CTAny).covariant, invocation.arguments.head) chain
            specifyType(possibleTypes(invocation.arguments.head), invocation)
        }

      case Last =>
        def possibleTypes(expression: Expression) : TypeGenerator = s =>
          (types(expression)(s) constrain CTList(CTAny)).unwrapLists

        checkArgs(invocation, 1) ifOkChain {
          expectType(CTList(CTAny).covariant, invocation.arguments.head) chain
            specifyType(possibleTypes(invocation.arguments.head), invocation)
        }

      case Length =>
        def checkForInvalidUsage(ctx: SemanticContext, invocation: FunctionInvocation) = (originalState: SemanticState) => {
          val newState = invocation.args.foldLeft(originalState) {
            case (state, expr) if state.expressionType(expr).actual != CTPath.invariant =>
              state.addNotification(LengthOnNonPathNotification(expr.position))
            case (state, expr) =>
              state
          }

          SemanticCheckResult(newState, Seq.empty)
        }
        checkTypeSignatures(ctx, Length, invocation) chain checkForInvalidUsage(ctx, invocation)

      case Max =>
        expectType(CTAny.covariant, invocation.arguments) chain
          specifyType(leastUpperBoundsOfTypes(invocation.arguments), invocation)

      case Min =>
        expectType(CTAny.covariant, invocation.arguments) chain
          specifyType(leastUpperBoundsOfTypes(invocation.arguments), invocation)

      case PercentileCont =>
        checkTypeSignatures(ctx, PercentileCont, invocation) ifOkChain
          checkPercentileRange(invocation.args(1))

      case PercentileDisc =>
        checkTypeSignatures(ctx, PercentileDisc, invocation) ifOkChain
          checkPercentileRange(invocation.args(1))

      case Point =>
        checkTypeSignatures(ctx, Point, invocation) ifOkChain
          checkPointMap(invocation.args(0))

      case Tail =>
        checkArgs(invocation, 1) ifOkChain {
          expectType(CTList(CTAny).covariant, invocation.arguments(0)) chain
            specifyType(types(invocation.arguments(0)), invocation)
        }

      case ToBoolean =>
        checkMinArgs(invocation, 1) ifOkChain
          checkMaxArgs(invocation, 1) ifOkChain
          checkToBooleanTypeOfArgument(invocation) ifOkChain
          specifyType(CTBoolean, invocation)

      case UnresolvedFunction =>
        // We cannot do a full semantic check until we have resolved the function call.
        SemanticCheckResult.success

      case x:TypeSignatures =>
        checkTypeSignatures(ctx, x, invocation)
    }

  /**
    * Check that invocation align with one of the functions type signatures
    */
  def checkTypeSignatures(
                           ctx: Expression.SemanticContext,
                           f:TypeSignatures,
                           invocation: FunctionInvocation
                         ): SemanticCheck =
    checkMinArgs(invocation, f.signatureLengths.min) chain
      checkMaxArgs(invocation, f.signatureLengths.max) chain
      checkTypes(invocation, f.signatures)

  protected def checkArgs(invocation: FunctionInvocation, n: Int): Option[SemanticError] =
    Vector(checkMinArgs(invocation, n), checkMaxArgs(invocation, n)).flatten.headOption

  protected def checkMaxArgs(invocation: FunctionInvocation, n: Int): Option[SemanticError] =
    if (invocation.arguments.length > n)
      Some(SemanticError(s"Too many parameters for function '${invocation.function.name}'", invocation.position))
    else
      None

  protected def checkMinArgs(invocation: FunctionInvocation, n: Int): Option[SemanticError] =
    if (invocation.arguments.length < n)
      Some(SemanticError(s"Insufficient parameters for function '${invocation.function.name}'", invocation.position))
    else
      None


  /*
   * Checks so that the expression is in the range [min, max]
   */
  def checkPercentileRange(expression: Expression): SemanticCheck = {
    expression match {
      case d: DoubleLiteral if d.value >= 0.0 && d.value <= 1.0 =>
        SemanticCheckResult.success
      case i: IntegerLiteral if i.value == 0L || i.value == 1L =>
        SemanticCheckResult.success
      case d: DoubleLiteral =>
        error(s"Invalid input '${d.value}' is not a valid argument, must be a number in the range 0.0 to 1.0", d.position)

      case l: Literal =>
        error(s"Invalid input '${
          l.asCanonicalStringVal
        }' is not a valid argument, must be a number in the range 0.0 to 1.0", l.position)

      //for other types we'll have to wait until runtime to fail
      case _ => SemanticCheckResult.success

    }
  }

  /*
   * Checks so that the point map is properly formatted
   */
  protected def checkPointMap(expression: Expression): SemanticCheck =
    expression match {

      //Cartesian point
      case map: MapExpression if map.items.exists(withKey("x")) && map.items.exists(withKey("y")) =>
        SemanticCheckResult.success

      //Geographic point
      case map: MapExpression if map.items.exists(withKey("longitude")) && map.items.exists(withKey("latitude")) =>
        SemanticCheckResult.success

      case map: MapExpression => error(
        s"A map with keys ${map.items.map((a) => s"'${a._1.name}'").mkString(", ")} is not describing a valid point, " +
          s"a point is described either by using cartesian coordinates e.g. {x: 2.3, y: 4.5, crs: 'cartesian'} or using " +
          s"geographic coordinates e.g. {latitude: 12.78, longitude: 56.7, crs: 'WGS-84'}.", map.position)

      //if using variable or parameter we can't introspect the map here
      case _ => SemanticCheckResult.success
    }

  private def withKey(key: String)(kv: (PropertyKeyName, Expression)) = kv._1.name == key


  private def checkToBooleanTypeOfArgument(invocation: FunctionInvocation): SemanticCheck =
    (s: SemanticState) => {
      val argument = invocation.args.head
      val specifiedType = s.expressionType(argument).specified
      val correctType = Seq(CTString, CTBoolean, CTAny).foldLeft(false) {
        case (acc, t) => acc || specifiedType.contains(t)
      }

      if (correctType) SemanticCheckResult.success(s)
      else {
        val msg = s"Type mismatch: expected Boolean or String but was ${specifiedType.mkString(", ")}"
        error(msg, argument.position)(s)
      }
    }
}

