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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.DoubleLiteral
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.IntegerLiteral
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.TypeSignatures
import org.neo4j.cypher.internal.expressions.functions.AggregatingFunction
import org.neo4j.cypher.internal.expressions.functions.Coalesce
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.expressions.functions.Distance
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.expressions.functions.Function
import org.neo4j.cypher.internal.expressions.functions.GraphByElementId
import org.neo4j.cypher.internal.expressions.functions.GraphByName
import org.neo4j.cypher.internal.expressions.functions.Head
import org.neo4j.cypher.internal.expressions.functions.IsEmpty
import org.neo4j.cypher.internal.expressions.functions.Last
import org.neo4j.cypher.internal.expressions.functions.Max
import org.neo4j.cypher.internal.expressions.functions.Min
import org.neo4j.cypher.internal.expressions.functions.PercentileCont
import org.neo4j.cypher.internal.expressions.functions.PercentileDisc
import org.neo4j.cypher.internal.expressions.functions.Point
import org.neo4j.cypher.internal.expressions.functions.Reduce
import org.neo4j.cypher.internal.expressions.functions.Reverse
import org.neo4j.cypher.internal.expressions.functions.Tail
import org.neo4j.cypher.internal.expressions.functions.ToBoolean
import org.neo4j.cypher.internal.expressions.functions.ToString
import org.neo4j.cypher.internal.expressions.functions.UnresolvedFunction
import org.neo4j.cypher.internal.expressions.functions.WithinBBox
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CypherType

import java.util.Locale

object SemanticFunctionCheck extends SemanticAnalysisTooling {

  def check(
    ctx: Expression.SemanticContext,
    invocation: FunctionInvocation
  ): SemanticCheck =
    invocation.function match {
      case f: AggregatingFunction =>
        when(ctx == Expression.SemanticContext.Simple) {
          error(s"Invalid use of aggregating function ${f.name}(...) in this context", invocation.position)
        } chain {
          checkNoNestedAggregateFunctions(invocation) chain
            SemanticExpressionCheck.check(ctx, invocation.arguments) chain
            semanticCheck(ctx, invocation)
        }

      case Reduce =>
        error(s"${Reduce.name}(...) requires '| expression' (an accumulation expression)", invocation.position)

      case f: Function =>
        when(invocation.distinct) {
          error(s"Invalid use of DISTINCT with function '${invocation.functionName.name}'", invocation.position)
        } chain SemanticExpressionCheck.check(ctx, invocation.arguments) chain semanticCheck(
          ctx,
          invocation
        )
    }

  private def checkNoNestedAggregateFunctions(invocation: FunctionInvocation): SemanticCheck =
    invocation.args.collectFirst {
      case expr if expr.containsAggregate => expr.findAggregate.get
    } foldSemanticCheck {
      expr => error("Can't use aggregate functions inside of aggregate functions.", expr.position)
    }

  protected def semanticCheck(ctx: Expression.SemanticContext, invocation: FunctionInvocation): SemanticCheck =
    invocation.function match {
      case Coalesce =>
        checkMinArgs(invocation, 1) chain
          expectType(CTAny.covariant, invocation.arguments) chain
          specifyType(unionOfTypes(invocation.arguments), invocation)

      case Collect =>
        checkTypeSignatures(ctx, Collect, invocation) ifOkChain {
          specifyType(types(invocation.arguments(0))(_).wrapInList, invocation)
        }

      case Exists =>
        checkArgs(invocation, 1) ifOkChain {
          expectType(CTAny.covariant, invocation.arguments.head) chain
            (invocation.arguments.head match {
              case _: PatternExpression => None
              case _: Property | _: ContainerIndex =>
                Some(SemanticError(
                  "The property existence syntax `... exists(variable.property)` is no longer supported. Please use `variable.property IS NOT NULL` instead.",
                  invocation.position
                ))
              case e =>
                Some(SemanticError(s"Argument to ${invocation.name}(...) is not a pattern", e.position))
            })
        } chain specifyType(CTBoolean, invocation)

      case Head =>
        checkArgs(invocation, 1) ifOkChain {
          expectType(CTList(CTAny).covariant, invocation.arguments.head) chain
            specifyType(possibleTypes(invocation.arguments.head), invocation)
        }

      case GraphByName =>
        checkTypeSignatures(ctx, GraphByName, invocation) ifOkChain {
          if (invocation.calledFromUseClause) {
            SemanticCheck.success
          } else {
            error(
              "`graph.byName` is only allowed at the first position of a USE clause.",
              invocation.position
            )
          }
        }

      case GraphByElementId =>
        checkTypeSignatures(ctx, GraphByElementId, invocation) ifOkChain {
          if (invocation.calledFromUseClause) {
            SemanticCheck.success
          } else {
            error(
              "`graph.byElementId` is only allowed at the first position of a USE clause.",
              invocation.position
            )
          }
        }

      case Last =>
        def possibleTypes(expression: Expression): TypeGenerator = s =>
          (types(expression)(s) constrain CTList(CTAny)).unwrapLists

        checkArgs(invocation, 1) ifOkChain {
          expectType(CTList(CTAny).covariant, invocation.arguments.head) chain
            specifyType(possibleTypes(invocation.arguments.head), invocation)
        }

      case Max =>
        checkTypeSignatures(ctx, Max, invocation) ifOkChain {
          specifyType(types(invocation.arguments(0))(_), invocation)
        }

      case IsEmpty =>
        checkTypeSignatures(ctx, IsEmpty, invocation)

      case Min =>
        checkTypeSignatures(ctx, Min, invocation) ifOkChain {
          specifyType(types(invocation.arguments(0))(_), invocation)
        }

      case PercentileCont =>
        checkTypeSignatures(ctx, PercentileCont, invocation) ifOkChain
          checkPercentileRange(invocation.args(1))

      case PercentileDisc =>
        checkTypeSignatures(ctx, PercentileDisc, invocation) ifOkChain
          checkPercentileRange(invocation.args(1))

      case Point =>
        checkTypeSignatures(ctx, Point, invocation) ifOkChain
          checkPointMap(invocation.args(0))

      case Reverse =>
        checkArgs(invocation, 1) ifOkChain {
          expectType(CTList(CTAny).covariant | CTString, invocation.arguments.head) chain
            specifyType(types(invocation.arguments.head), invocation)
        }

      case Tail =>
        checkArgs(invocation, 1) ifOkChain {
          expectType(CTList(CTAny).covariant, invocation.arguments(0)) chain
            specifyType(types(invocation.arguments(0)), invocation)
        }

      case ToBoolean =>
        checkArgs(invocation, 1) ifOkChain
          checkToSpecifiedTypeOfArgument(invocation, Seq(CTString, CTBoolean, CTInteger)) ifOkChain
          specifyType(CTBoolean, invocation)

      case ToString =>
        checkArgs(invocation, 1) ifOkChain
          checkToSpecifiedTypeOfArgument(invocation, ToString.validInputTypes) ifOkChain
          specifyType(CTString, invocation)

      // distance has been replaced with point.distance, make sure we provide a nice error message
      case UnresolvedFunction
        if invocation.functionName.namespace.parts.isEmpty && invocation.functionName.name.toLowerCase(
          Locale.ROOT
        ) == "distance" =>
        SemanticError(s"'distance' has been replaced by 'point.distance'", invocation.position)

      case Distance =>
        checkArgs(invocation, 2) ifOkChain
          specifyType(CTFloat, invocation)

      case WithinBBox =>
        checkArgs(invocation, 3) ifOkChain
          specifyType(CTBoolean, invocation)

      case UnresolvedFunction =>
        // We cannot do a full semantic check until we have resolved the function call.
        SemanticCheck.success

      case x: TypeSignatures =>
        checkTypeSignatures(ctx, x, invocation)
    }

  /**
   * Check that invocation align with one of the functions type signatures
   */
  def checkTypeSignatures(
    ctx: Expression.SemanticContext,
    f: TypeSignatures,
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
        SemanticCheck.success
      case i: IntegerLiteral if i.value == 0L || i.value == 1L =>
        SemanticCheck.success
      case d: DoubleLiteral =>
        error(
          s"Invalid input '${d.value}' is not a valid argument, must be a number in the range 0.0 to 1.0",
          d.position
        )

      case l: Literal =>
        error(
          s"Invalid input '${l.asCanonicalStringVal}' is not a valid argument, must be a number in the range 0.0 to 1.0",
          l.position
        )

      // for other types we'll have to wait until runtime to fail
      case _ => SemanticCheck.success

    }
  }

  /*
   * Checks so that the point map is properly formatted
   */
  protected def checkPointMap(expression: Expression): SemanticCheck =
    expression match {

      // Cartesian point
      case map: MapExpression if map.items.exists(withKey("x")) && map.items.exists(withKey("y")) =>
        SemanticCheck.success

      // Geographic point
      case map: MapExpression if map.items.exists(withKey("longitude")) && map.items.exists(withKey("latitude")) =>
        SemanticCheck.success

      case map: MapExpression => error(
          s"A map with keys ${map.items.map(a => s"'${a._1.name}'").mkString(", ")} is not describing a valid point, " +
            s"a point is described either by using cartesian coordinates e.g. {x: 2.3, y: 4.5, crs: 'cartesian'} or using " +
            s"geographic coordinates e.g. {latitude: 12.78, longitude: 56.7, crs: 'WGS-84'}.",
          map.position
        )

      // if using variable or parameter we can't introspect the map here
      case _ => SemanticCheck.success
    }

  private def withKey(key: String)(kv: (PropertyKeyName, Expression)) = kv._1.name == key

  private def checkToSpecifiedTypeOfArgument(
    invocation: FunctionInvocation,
    allowedTypes: Seq[CypherType]
  ): SemanticCheck =
    (s: SemanticState) => {
      val argument = invocation.args.head
      val specifiedType = s.expressionType(argument).specified
      val correctType = allowedTypes.foldLeft(false) {
        case (acc, t) => acc || specifiedType.contains(t)
      }

      if (correctType) SemanticCheckResult.success(s)
      else {
        val msg = invocation.function match {
          case ToString =>
            s"Type mismatch: expected Boolean, Float, Integer, Point, String, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was ${specifiedType.mkString(", ")}"
          case ToBoolean =>
            s"Type mismatch: expected Boolean, Integer or String but was ${specifiedType.mkString(", ")}"
          case _ =>
            s"Type mismatch: expected Boolean or String but was ${specifiedType.mkString(", ")}"
        }
        SemanticCheckResult.error(s, msg, argument.position)
      }
    }
}
