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
package org.neo4j.cypher.internal.frontend.v3_4.semantics

import org.neo4j.cypher.internal.util.v3_4.symbols.{CTList, CTMap, CTNode, CTPath, CTRelationship}
import org.neo4j.cypher.internal.frontend.v3_4.SemanticCheck
import org.neo4j.cypher.internal.frontend.v3_4.notification.UnboundedShortestPathNotification
import org.neo4j.cypher.internal.v3_4.expressions.Pattern.SemanticContext.name
import org.neo4j.cypher.internal.v3_4.expressions.Pattern.{SemanticContext, findDuplicateRelationships}
import org.neo4j.cypher.internal.v3_4.expressions._

object SemanticPatternCheck extends SemanticAnalysisTooling {

  def check(ctx: SemanticContext, pattern:Pattern): SemanticCheck =
    pattern match {
      case x:Pattern =>
        semanticCheckFold(x.patternParts)(declareVariables(ctx)) chain
          semanticCheckFold(x.patternParts)(check(ctx)) chain
          ensureNoDuplicateRelationships(x, ctx)
    }

  def check(ctx: SemanticContext, pattern:RelationshipsPattern): SemanticCheck =
    declareVariables(ctx, pattern.element) chain
      check(ctx, pattern.element)

  def declareVariables(ctx:SemanticContext)(part:PatternPart):SemanticCheck =
    part match {
      case x:NamedPatternPart =>
        declareVariables(ctx)(x.patternPart) chain
          declareVariable(x.variable, CTPath)

      case x:EveryPath =>
        (x.element, ctx) match {
          // single node variable is allowed to be already bound in MATCH and GRAPH OF
          case (_: NodePattern, SemanticContext.GraphOf) =>
            declareVariables(ctx, x.element)
          case (_: NodePattern, SemanticContext.Match) =>
            declareVariables(ctx, x.element)
          case (n: NodePattern, _) =>
            n.variable.fold(SemanticCheckResult.success)(declareVariable(_, CTNode)) chain
              declareVariables(ctx, n)
          case _ =>
            declareVariables(ctx, x.element)
        }

      case x:ShortestPaths =>
        declareVariables(ctx, x.element)
    }

  def check(ctx: SemanticContext)(part:PatternPart):SemanticCheck =
    part match {
      case x:NamedPatternPart =>
        check(ctx)(x.patternPart)

      case x:EveryPath =>
        check(ctx, x.element)

      case x:ShortestPaths =>
        def checkContext: SemanticCheck =
          ctx match {
            case SemanticContext.Merge =>
              SemanticError(s"${
                x.name
              }(...) cannot be used to MERGE", x.position, x.element.position)
            case SemanticContext.Create | SemanticContext.CreateUnique =>
              SemanticError(s"${
                x.name
              }(...) cannot be used to CREATE", x.position, x.element.position)
            case _ =>
              None
          }

        def checkContainsSingle: SemanticCheck =
          x.element match {
            case RelationshipChain(_: NodePattern, r, _: NodePattern) =>
              r.properties.map {
                props =>
                  SemanticError(s"${
                    x.name
                  }(...) contains properties $props. This is currently not supported.", x.position, x.element.position)
              }
            case _ =>
              SemanticError(s"${
                x.name
              }(...) requires a pattern containing a single relationship", x.position, x.element.position)
          }

        def checkLength: SemanticCheck =
          (state: SemanticState) =>
            x.element match {
              case RelationshipChain(_, rel, _) =>
                rel.length match {
                  case Some(Some(Range(Some(min), _))) if min.value < 0 || min.value > 1 =>
                    SemanticCheckResult(state, Seq(SemanticError(s"${
                      x.name
                    }(...) does not support a minimal length different " +
                      s"from 0 or 1", x.position, x.element.position)))

                  case Some(None) =>
                    val newState = state.addNotification(UnboundedShortestPathNotification(x.element.position))
                    SemanticCheckResult(newState, Seq.empty)
                  case _ => SemanticCheckResult(state, Seq.empty)
                }
              case _ => SemanticCheckResult(state, Seq.empty)
            }

        def checkRelVariablesUnknown: SemanticCheck =
          state => {
            x.element match {
              case RelationshipChain(_, rel, _) =>
                rel.variable.flatMap(id => state.symbol(id.name)) match {
                  case Some(symbol) if symbol.positions.size > 1 => {
                    SemanticCheckResult.error(state, SemanticError(s"Bound relationships not allowed in ${
                      x.name
                    }(...)", rel.position, symbol.positions.head))
                  }
                  case _ =>
                    SemanticCheckResult.success(state)
                }
              case _ =>
                SemanticCheckResult.success(state)
            }
          }

        checkContext chain
          checkContainsSingle chain
          checkLength chain
          checkRelVariablesUnknown chain
          check(ctx, x.element)
    }

  def check(ctx:SemanticContext, element:PatternElement):SemanticCheck =
    element match {
    case x:RelationshipChain =>
      check(ctx, x.element) chain
        check(ctx, x.relationship) chain
        check(ctx, x.rightNode)

    case x:InvalidNodePattern =>
      checkNodeProperties(ctx, x.properties) chain
        error(s"Parentheses are required to identify nodes in patterns, i.e. (${x.id.name})", x.position)

    case x:NodePattern =>
      checkNodeProperties(ctx, x.properties)

    }

  def check(ctx:SemanticContext, x:RelationshipPattern):SemanticCheck = {
    def checkNotUndirectedWhenCreating: SemanticCheck = {
      ctx match {
        case SemanticContext.Create | SemanticContext.GraphOf if x.direction == SemanticDirection.BOTH =>
          error(s"Only directed relationships are supported in ${name(ctx)}", x.position)
        case _ =>
          SemanticCheckResult.success
      }
    }

    def checkNoVarLengthWhenUpdating: SemanticCheck =
      when(!x.isSingleLength) {
        ctx match {
          case SemanticContext.Merge |
               SemanticContext.Create |
               SemanticContext.CreateUnique |
               SemanticContext.GraphOf =>
            error(s"Variable length relationships cannot be used in ${name(ctx)}", x.position)
          case _ =>
            None
        }
      }

    def checkProperties: SemanticCheck =
      SemanticExpressionCheck.simple(x.properties) chain
        expectType(CTMap.covariant, x.properties)

    checkNoVarLengthWhenUpdating chain
      checkNoParamMapsWhenMatching(x.properties, ctx) chain
      checkProperties chain
      checkNotUndirectedWhenCreating
  }

  def declareVariables(ctx:SemanticContext, element:PatternElement):SemanticCheck =
    element match {
      case x:RelationshipChain =>
        declareVariables(ctx, x.element) chain
          declareVariables(ctx, x.relationship) chain
          declareVariables(ctx, x.rightNode)

      case x:NodePattern =>
        x.variable.fold(SemanticCheckResult.success) {
          variable =>
            ctx match {
              case SemanticContext.Expression =>
                ensureDefined(variable) chain
                  expectType(CTNode.covariant, variable)
              case _                          =>
                implicitVariable(variable, CTNode)
            }
        }
    }

  def declareVariables(ctx:SemanticContext, x:RelationshipPattern):SemanticCheck =
    x.variable.fold(SemanticCheckResult.success) {
      variable =>
        val possibleType = if (x.length.isEmpty) CTRelationship else CTList(CTRelationship)

        ctx match {
          case SemanticContext.Match | SemanticContext.GraphOf =>
            implicitVariable(variable, possibleType)
          case SemanticContext.Expression =>
            ensureDefined(variable) chain
              expectType(possibleType.covariant, variable)
          case _ =>
            declareVariable(variable, possibleType)
        }
    }

  private def ensureNoDuplicateRelationships(pattern: Pattern, ctx: SemanticContext): SemanticCheck = {
    findDuplicateRelationships(pattern).foldLeft(SemanticCheckResult.success) {
      (acc, duplicates) =>
        val id = duplicates.head
        val dups = duplicates.tail

        acc chain SemanticError(s"Cannot use the same relationship variable '${id.name}' for multiple patterns", id.position, dups.map(_.position):_*)
    }
  }

  def checkNodeProperties(ctx: SemanticContext, properties: Option[Expression]): SemanticCheck =
    checkNoParamMapsWhenMatching(properties, ctx) chain
      SemanticExpressionCheck.simple(properties) chain
      expectType(CTMap.covariant, properties)
}

object checkNoParamMapsWhenMatching {
  def apply(properties: Option[Expression], ctx: SemanticContext): SemanticCheck = (properties, ctx) match {
    case (Some(e: Parameter), SemanticContext.Match) =>
      SemanticError("Parameter maps cannot be used in MATCH patterns (use a literal map instead, eg. \"{id: {param}.id}\")", e.position)
    case (Some(e: Parameter), SemanticContext.Merge) =>
      SemanticError("Parameter maps cannot be used in MERGE patterns (use a literal map instead, eg. \"{id: {param}.id}\")", e.position)
    case (Some(e: Parameter), SemanticContext.GraphOf) =>
      SemanticError("Parameter maps cannot be used in GRAPH OF patterns (use a literal map instead, eg. \"{id: {param}.id}\")", e.position)
    case _ =>
      None
  }
}
