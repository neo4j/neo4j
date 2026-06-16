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

import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.fromContext
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.DoubleLiteral
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionTypeSignatures
import org.neo4j.cypher.internal.expressions.IntegerLiteral
import org.neo4j.cypher.internal.expressions.Literal
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NumberLiteral
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.TypeSignature
import org.neo4j.cypher.internal.expressions.functions.AggregatingFunction
import org.neo4j.cypher.internal.expressions.functions.AllReduce
import org.neo4j.cypher.internal.expressions.functions.Cardinality
import org.neo4j.cypher.internal.expressions.functions.Coalesce
import org.neo4j.cypher.internal.expressions.functions.CollDistinct
import org.neo4j.cypher.internal.expressions.functions.CollFlatten
import org.neo4j.cypher.internal.expressions.functions.CollIndexOf
import org.neo4j.cypher.internal.expressions.functions.CollInsert
import org.neo4j.cypher.internal.expressions.functions.CollMax
import org.neo4j.cypher.internal.expressions.functions.CollMin
import org.neo4j.cypher.internal.expressions.functions.CollRemove
import org.neo4j.cypher.internal.expressions.functions.CollSort
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.expressions.functions.Distance
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.expressions.functions.Function
import org.neo4j.cypher.internal.expressions.functions.GraphByElementId
import org.neo4j.cypher.internal.expressions.functions.GraphByName
import org.neo4j.cypher.internal.expressions.functions.Head
import org.neo4j.cypher.internal.expressions.functions.IsEmpty
import org.neo4j.cypher.internal.expressions.functions.Last
import org.neo4j.cypher.internal.expressions.functions.LocalFunction
import org.neo4j.cypher.internal.expressions.functions.Max
import org.neo4j.cypher.internal.expressions.functions.Min
import org.neo4j.cypher.internal.expressions.functions.PercentileCont
import org.neo4j.cypher.internal.expressions.functions.PercentileDisc
import org.neo4j.cypher.internal.expressions.functions.Point
import org.neo4j.cypher.internal.expressions.functions.Reduce
import org.neo4j.cypher.internal.expressions.functions.Replace
import org.neo4j.cypher.internal.expressions.functions.Reverse
import org.neo4j.cypher.internal.expressions.functions.Tail
import org.neo4j.cypher.internal.expressions.functions.ToBoolean
import org.neo4j.cypher.internal.expressions.functions.ToString
import org.neo4j.cypher.internal.expressions.functions.UUIDConstructor
import org.neo4j.cypher.internal.expressions.functions.UUIDLeastSignificantBits
import org.neo4j.cypher.internal.expressions.functions.UUIDMostSignificantBits
import org.neo4j.cypher.internal.expressions.functions.UnresolvedFunction
import org.neo4j.cypher.internal.expressions.functions.WithinBBox
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.neo4j.cypher.internal.util.symbols.invariantTypeSpec
import org.neo4j.gqlstatus.GqlHelper

object SemanticFunctionCheck extends SemanticAnalysisTooling {

  def check(
    ctx: Expression.SemanticContext,
    invocation: FunctionInvocation
  ): SemanticCheck = {
    fromContext(semanticCheckContext => {
      invocation.functionWithScope(semanticCheckContext.cypherVersion) match {
        case lf: LocalFunction =>
          SemanticExpressionCheck.check(ctx, invocation.arguments) chain
            checkFunctionTypeSignatures(semanticCheckContext, lf, invocation) ifOkChain {
              specifyType(lf.returnType, invocation)
            }

        case f: AggregatingFunction =>
          when(ctx == Expression.SemanticContext.Simple) {
            SemanticCheck.error(
              SemanticError.aggregateExpressionsNotAllowedInSimpleExpressions(
                invocation.asCanonicalStringVal,
                f.name,
                invocation.position
              )
            )
          } chain {
            checkNoNestedAggregateFunctions(invocation) chain
              SemanticExpressionCheck.check(ctx, invocation.arguments) chain
              semanticCheck(ctx, invocation)
          }

        case Reduce =>
          error(SemanticError.invalidReduceAccumulator(invocation.position))

        case AllReduce =>
          error(SemanticError.invalidAllReduceSyntax(invocation.position))

        case f: Function => whenState(
            !_.features.contains(SemanticFeature.UUIDType) && isUUIDFunction(f)
          ) {
            error(SemanticError.uuidTypeNotSupported("UUID Type", invocation.position))
          } chain
            when(invocation.distinct) {
              error(SemanticError.invalidDistinct(invocation.functionName.name, invocation.position))
            } chain
            SemanticExpressionCheck.check(ctx, invocation.arguments) chain
            semanticCheck(ctx, invocation)
      }
    })
  }

  // Remove once the feature flag for uuids is removed :)
  private def isUUIDFunction(function: Function): Boolean =
    function match {
      case UUIDConstructor          => true
      case UUIDMostSignificantBits  => true
      case UUIDLeastSignificantBits => true
      case _                        => false
    }

  private def checkNoNestedAggregateFunctions(invocation: FunctionInvocation): SemanticCheck =
    invocation.args.collectFirst {
      case expr if expr.containsAggregate => expr.findAggregate.get
    } foldSemanticCheck {
      val prettifier = ExpressionStringifier()
      expr =>
        error(SemanticError.aggregateExpressionsNotAllowedInAggregationFunctions(
          prettifier(expr),
          expr.position
        ))
    }

  private def semanticCheck(ctx: Expression.SemanticContext, invocation: FunctionInvocation): SemanticCheck =
    fromContext(semanticCheckContext =>
      invocation.functionWithScope(semanticCheckContext.cypherVersion) match {
        case Cardinality =>
          checkMinArgs(invocation, 1, Cardinality.signatures) ifOkChain
            checkMaxArgs(invocation, 1, Cardinality.signatures) ifOkChain
            expectType(
              CTMap.invariant | CTList(CTAny).covariant | CTPath.invariant,
              invocation.arguments.head
            ) ifOkChain
            specifyType(CTInteger, invocation)

        case Coalesce =>
          checkMinArgs(invocation, 1, Coalesce.signatures) chain
            expectType(CTAny.covariant, invocation.arguments) chain
            specifyType(unionOfTypes(invocation.arguments), invocation)

        case Collect =>
          checkFunctionTypeSignatures(semanticCheckContext, Collect, invocation) ifOkChain {
            specifyType(types(invocation.arguments(0))(_).wrapInList, invocation)
          }

        case CollDistinct =>
          checkFunctionTypeSignatures(semanticCheckContext, CollDistinct, invocation) ifOkChain {
            specifyType(types(invocation.arguments.head), invocation)
          }

        case CollFlatten =>
          checkFunctionTypeSignatures(semanticCheckContext, CollFlatten, invocation) ifOkChain {
            specifyType(CTAny.covariant, invocation)
          }

        case CollIndexOf =>
          checkFunctionTypeSignatures(semanticCheckContext, CollIndexOf, invocation) ifOkChain {
            specifyType(CTInteger, invocation)
          }

        case CollInsert =>
          checkFunctionTypeSignatures(semanticCheckContext, CollInsert, invocation) ifOkChain {
            specifyType(
              (s: SemanticState) => // Original list + new item
                possibleTypes(invocation.arguments.head)(s) coerceOrLeastUpperBound types(
                  invocation.arguments(2)
                )(s).wrapInList,
              invocation
            )
          }

        case CollMax =>
          checkFunctionTypeSignatures(semanticCheckContext, CollMax, invocation) ifOkChain {
            specifyType(possibleTypes(invocation.arguments.head), invocation)
          }

        case CollMin =>
          checkFunctionTypeSignatures(semanticCheckContext, CollMin, invocation) ifOkChain {
            specifyType(possibleTypes(invocation.arguments.head), invocation)
          }

        case CollRemove =>
          checkFunctionTypeSignatures(semanticCheckContext, CollRemove, invocation) ifOkChain {
            specifyType(types(invocation.arguments.head), invocation)
          }

        case CollSort =>
          checkFunctionTypeSignatures(semanticCheckContext, CollSort, invocation) ifOkChain {
            specifyType(types(invocation.arguments.head), invocation)
          }

        case Exists =>
          checkArgs(invocation, 1, Exists.signatures) ifOkChain {
            expectType(CTAny.covariant, invocation.arguments.head) chain
              (invocation.arguments.head match {
                case _: PatternExpression => None
                case _: Property | _: ContainerIndex =>
                  val position = invocation.position
                  val message =
                    "The property existence syntax `... exists(variable.property)` is no longer supported. Please use `variable.property IS NOT NULL` instead."
                  Some(SemanticError(
                    GqlHelper.getGql42001_42I52(message, position.offset, position.line, position.column),
                    message,
                    position
                  ))
                case e =>
                  Some(SemanticError.invalidEntityType(
                    "argument",
                    invocation.name,
                    List("pattern"),
                    s"Argument to ${invocation.name}(...) is not a pattern",
                    e.position
                  ))
              })
          } chain specifyType(CTBoolean, invocation)

        case Head =>
          checkArgs(invocation, 1, Head.signatures) ifOkChain {
            expectType(CTList(CTAny).covariant, invocation.arguments.head) chain
              specifyType(possibleTypes(invocation.arguments.head), invocation)
          }

        case GraphByName =>
          checkFunctionTypeSignatures(semanticCheckContext, GraphByName, invocation) ifOkChain {
            if (invocation.calledFromUseClause) {
              SemanticCheck.success
            } else {
              SemanticCheck.error(SemanticError.invalidUseOfGraphFunction("graph.byName", invocation.position))
            }
          }

        case GraphByElementId =>
          checkFunctionTypeSignatures(semanticCheckContext, GraphByElementId, invocation) ifOkChain {
            if (invocation.calledFromUseClause) {
              SemanticCheck.success
            } else {
              SemanticCheck.error(SemanticError.invalidUseOfGraphFunction("graph.byElementId", invocation.position))
            }
          }

        case Last =>
          def possibleTypes(expression: Expression): TypeGenerator = s =>
            (types(expression)(s) constrain CTList(CTAny)).unwrapLists

          checkArgs(invocation, 1, Last.signatures) ifOkChain {
            expectType(CTList(CTAny).covariant, invocation.arguments.head) chain
              specifyType(possibleTypes(invocation.arguments.head), invocation)
          }

        case Max =>
          checkFunctionTypeSignatures(semanticCheckContext, Max, invocation) ifOkChain {
            specifyType(types(invocation.arguments(0))(_), invocation)
          }

        case IsEmpty =>
          checkFunctionTypeSignatures(semanticCheckContext, IsEmpty, invocation)

        case Min =>
          checkFunctionTypeSignatures(semanticCheckContext, Min, invocation) ifOkChain {
            specifyType(types(invocation.arguments(0))(_), invocation)
          }

        case PercentileCont =>
          checkFunctionTypeSignatures(semanticCheckContext, PercentileCont, invocation) ifOkChain
            checkPercentileRange(invocation.args(1))

        case PercentileDisc =>
          checkFunctionTypeSignatures(semanticCheckContext, PercentileDisc, invocation) ifOkChain
            checkPercentileRange(invocation.args(1))

        case Point =>
          checkFunctionTypeSignatures(semanticCheckContext, Point, invocation) ifOkChain
            checkPointMap(invocation.args(0))

        case Reverse =>
          checkArgs(invocation, 1, Reverse.signatures) ifOkChain {
            specifyType(types(invocation.arguments.head), invocation)
          }

        case Replace =>
          checkFunctionTypeSignatures(semanticCheckContext, Replace, invocation) ifOkChain {
            if (invocation.arguments.size == 4) {
              invocation.arguments(3) match {
                case i: IntegerLiteral if i.value >= 0 => SemanticCheck.success
                case lit: NumberLiteral =>
                  SemanticAnalysisToolingErrorWithGqlInfo.specifiedNumberOutOfRangeError(
                    "limit",
                    "INTEGER",
                    0,
                    Long.MaxValue,
                    String.valueOf(lit.value),
                    "The limit needs to be greater than or equal to 0.",
                    lit.position
                  )
                case _ => SemanticCheck.success
              }
            } else {
              SemanticCheck.success
            }
          }

        case Tail =>
          checkArgs(invocation, 1, Tail.signatures) ifOkChain {
            expectType(CTList(CTAny).covariant, invocation.arguments(0)) chain
              specifyType(types(invocation.arguments(0)), invocation)
          }

        case ToBoolean =>
          checkArgs(invocation, 1, ToBoolean.signatures) ifOkChain
            checkToSpecifiedTypeOfArgument(invocation, Seq(CTString, CTBoolean, CTInteger)) ifOkChain
            specifyType(CTBoolean, invocation)

        case ToString =>
          checkArgs(invocation, 1, ToString.signatures) ifOkChain
            checkToSpecifiedTypeOfArgument(invocation, ToString.validInputTypes) ifOkChain
            specifyType(CTString, invocation)

        case Distance =>
          checkArgs(invocation, 2, Distance.signatures) ifOkChain
            specifyType(CTFloat, invocation)

        case WithinBBox =>
          checkArgs(invocation, 3, WithinBBox.signatures) ifOkChain
            specifyType(CTBoolean, invocation)

        case UnresolvedFunction =>
          // We cannot do a full semantic check until we have resolved the function call.
          SemanticCheck.success

        case x: FunctionTypeSignatures =>
          checkFunctionTypeSignatures(semanticCheckContext, x, invocation)
      }
    )

  /**
   * Check that invocation align with one of the functions type signatures
   */
  def checkFunctionTypeSignatures(
    ctx: SemanticCheckContext,
    f: FunctionTypeSignatures,
    invocation: FunctionInvocation
  ): SemanticCheck =
    checkMinArgs(
      invocation,
      f.signatureLengthsByScope(ctx.cypherVersion).min,
      f.signaturesByScope(ctx.cypherVersion)
    ) chain
      checkMaxArgs(
        invocation,
        f.signatureLengthsByScope(ctx.cypherVersion).max,
        f.signaturesByScope(ctx.cypherVersion)
      ) chain
      checkTypes(invocation, f.signaturesByScope(ctx.cypherVersion))

  protected def checkArgs(
    invocation: FunctionInvocation,
    n: Int,
    signatures: Vector[TypeSignature]
  ): Option[SemanticError] =
    Vector(checkMinArgs(invocation, n, signatures), checkMaxArgs(invocation, n, signatures)).flatten.headOption

  protected def checkMaxArgs(
    invocation: FunctionInvocation,
    n: Int,
    signatures: Seq[TypeSignature]
  ): Option[SemanticError] =
    if (invocation.arguments.length > n)
      Some(SemanticError.invalidNumberOfProcedureOrFunctionArguments(
        n,
        invocation.arguments.length,
        invocation.name,
        signatures.map(signature => signature.getSignatureAsString).mkString(" or "),
        s"Too many parameters for function '${invocation.function.name}'",
        invocation.position
      ))
    else
      None

  protected def checkMinArgs(
    invocation: FunctionInvocation,
    n: Int,
    signatures: Seq[TypeSignature]
  ): Option[SemanticError] =
    if (invocation.arguments.length < n)
      Some(SemanticError.invalidNumberOfProcedureOrFunctionArguments(
        n,
        invocation.arguments.length,
        invocation.name,
        signatures.map(signature => signature.getSignatureAsString).mkString(" or "),
        s"Insufficient parameters for function '${invocation.function.name}'",
        invocation.position
      ))
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
        SemanticAnalysisToolingErrorWithGqlInfo.specifiedNumberOutOfRangeError(
          "percentile range",
          "FLOAT",
          0.0,
          1.0,
          String.valueOf(d.value),
          s"Invalid input '${d.value}' is not a valid argument, must be a number in the range 0.0 to 1.0",
          d.position
        )

      case l: Literal =>
        SemanticAnalysisToolingErrorWithGqlInfo.specifiedNumberOutOfRangeError(
          "percentile range",
          "FLOAT",
          0.0,
          1.0,
          l.asCanonicalStringVal,
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
          SemanticError.invalidPoint(map.items.map(a => a._1.name), map.position)
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
        val error = invocation.function match {
          case ToString =>
            SemanticCheckResult.error(
              s,
              SemanticError.invalidEntityType(
                TypeSpec.cypherTypeForTypeSpec(specifiedType).normalizedCypherTypeString(),
                "argument to function toString()",
                List(
                  "BOOLEAN",
                  "FLOAT",
                  "INTEGER",
                  "POINT",
                  "STRING",
                  "DURATION",
                  "DATE",
                  "ZONED TIME",
                  "LOCAL TIME",
                  "LOCAL DATETIME",
                  "ZONED DATETIME"
                ),
                s"Type mismatch: expected Boolean, Float, Integer, Point, String, Duration, Date, Time, LocalTime, LocalDateTime or DateTime but was ${specifiedType.mkString(", ")}",
                argument.position
              )
            )
          case ToBoolean =>
            SemanticCheckResult.error(
              s,
              SemanticError.invalidEntityType(
                TypeSpec.cypherTypeForTypeSpec(specifiedType).normalizedCypherTypeString(),
                "argument of function toBoolean()",
                List("BOOLEAN", "INTEGER", "STRING"),
                s"Type mismatch: expected Boolean, Integer or String but was ${specifiedType.mkString(", ")}",
                argument.position
              )
            )
          case _ =>
            SemanticCheckResult.error(
              s,
              SemanticError.invalidEntityType(
                specifiedType.mkString(", "),
                invocation.functionName.name,
                List("BOOLEAN", "STRING"),
                s"Type mismatch: expected Boolean or String but was ${specifiedType.mkString(", ")}",
                argument.position
              )
            )
        }
        error
      }
    }
}
