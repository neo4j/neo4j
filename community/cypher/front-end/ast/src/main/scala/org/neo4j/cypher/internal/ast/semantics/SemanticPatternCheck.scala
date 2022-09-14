/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ast.semantics

import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.success
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FixedQuantifier
import org.neo4j.cypher.internal.expressions.GraphPatternQuantifier
import org.neo4j.cypher.internal.expressions.IntervalQuantifier
import org.neo4j.cypher.internal.expressions.LabelExpression
import org.neo4j.cypher.internal.expressions.LabelExpression.ColonDisjunction
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.PathFactor
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.Pattern.SemanticContext
import org.neo4j.cypher.internal.expressions.Pattern.SemanticContext.Match
import org.neo4j.cypher.internal.expressions.Pattern.SemanticContext.Merge
import org.neo4j.cypher.internal.expressions.Pattern.SemanticContext.name
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipsPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.ShortestPaths
import org.neo4j.cypher.internal.expressions.SymbolicName
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.UnboundedShortestPathNotification
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.topDown

object SemanticPatternCheck extends SemanticAnalysisTooling {

  def check(ctx: SemanticContext, pattern: Pattern): SemanticCheck =
    semanticCheckFold(pattern.patternParts)(declareVariables(ctx)) chain
      semanticCheckFold(pattern.patternParts)(check(ctx)) chain
      semanticCheckFold(pattern.patternParts)(checkMinimumNodeCount) chain
      ensureNoReferencesOutFromQuantifiedPath(pattern) chain
      ensureNoDuplicateRelationships(pattern)

  def check(ctx: SemanticContext, pattern: RelationshipsPattern): SemanticCheck =
    declareVariables(ctx, pattern.element) chain
      check(ctx, pattern.element) chain
      ensureNoDuplicateRelationships(pattern)

  def declareVariables(ctx: SemanticContext)(part: PatternPart): SemanticCheck =
    part match {
      case x: NamedPatternPart =>
        declareVariables(ctx)(x.patternPart) chain
          declareVariable(x.variable, CTPath)

      case x: EveryPath =>
        (x.element, ctx) match {
          case (_: NodePattern, SemanticContext.Match) =>
            declareVariables(ctx, x.element)
          case (n: NodePattern, _) =>
            n.variable.foldSemanticCheck(declareVariable(_, CTNode)) chain
              declareVariables(ctx, n)
          case _ =>
            declareVariables(ctx, x.element)
        }

      case x: ShortestPaths =>
        declareVariables(ctx, x.element)
    }

  def check(ctx: SemanticContext)(part: PatternPart): SemanticCheck =
    part match {
      case x: NamedPatternPart =>
        check(ctx)(x.patternPart) chain
          checkNoQuantifiedPathPatterns(x.patternPart)

      case x: EveryPath =>
        check(ctx, x.element)

      case x: ShortestPaths =>
        def checkContext: SemanticCheck =
          ctx match {
            case SemanticContext.Merge =>
              SemanticError(s"${x.name}(...) cannot be used to MERGE", x.position)
            case SemanticContext.Create =>
              SemanticError(s"${x.name}(...) cannot be used to CREATE", x.position)
            case _ =>
              None
          }

        def checkContainsSingle: SemanticCheck =
          x.element match {
            case RelationshipChain(_: NodePattern, r, _: NodePattern) =>
              r.properties.map {
                props =>
                  SemanticError(
                    s"${x.name}(...) contains properties $props. This is currently not supported.",
                    x.position
                  )
              }
            case _ =>
              SemanticError(s"${x.name}(...) requires a pattern containing a single relationship", x.position)
          }

        def checkKnownEnds: SemanticCheck =
          (ctx, x.element) match {
            case (Match, _) => None
            case (_, RelationshipChain(l: NodePattern, _, r: NodePattern)) =>
              if (l.variable.isEmpty)
                SemanticError(s"A ${x.name}(...) requires bound nodes when not part of a MATCH clause.", x.position)
              else if (r.variable.isEmpty)
                SemanticError(s"A ${x.name}(...) requires bound nodes when not part of a MATCH clause.", x.position)
              else
                None
            case (_, _) =>
              None
          }

        def checkLength: SemanticCheck =
          (state: SemanticState) =>
            x.element match {
              case RelationshipChain(_, rel, _) =>
                rel.length match {
                  case Some(Some(Range(Some(min), _))) if min.value < 0 || min.value > 1 =>
                    SemanticCheckResult(
                      state,
                      Seq(SemanticError(
                        s"${x.name}(...) does not support a minimal length different " +
                          s"from 0 or 1",
                        min.position
                      ))
                    )

                  case Some(None) =>
                    val newState = state.addNotification(UnboundedShortestPathNotification(x.element.position))
                    SemanticCheckResult(newState, Seq.empty)
                  case _ => SemanticCheckResult(state, Seq.empty)
                }
              case _ => SemanticCheckResult(state, Seq.empty)
            }

        def checkRelVariablesUnknown: SemanticCheck =
          (state: SemanticState) => {
            x.element match {
              case RelationshipChain(_, rel, _) =>
                rel.variable.flatMap(id => state.symbol(id.name)) match {
                  case Some(symbol) if symbol.references.size > 1 =>
                    SemanticCheckResult.error(
                      state,
                      SemanticError(s"Bound relationships not allowed in ${x.name}(...)", rel.position)
                    )
                  case _ =>
                    SemanticCheckResult.success(state)
                }
              case _ =>
                SemanticCheckResult.success(state)
            }
          }

        checkContext chain
          checkContainsSingle chain
          checkKnownEnds chain
          checkLength chain
          checkRelVariablesUnknown chain
          check(ctx, x.element)
    }

  private def checkNoQuantifiedPathPatterns(x: PatternPart): SemanticCheck = {
    x.folder.treeFindByClass[QuantifiedPath].foldSemanticCheck(qpp =>
      error("Assigning a path with a quantified path pattern is not yet supported.", qpp.position)
    )
  }

  private val stringifier = ExpressionStringifier()

  private def checkMinimumNodeCount(x: PatternPart): SemanticCheck = {
    when(x.element.folder.treeFold(true) {
      case QuantifiedPath(_, quantifier, _, _) if quantifier.canBeEmpty =>
        acc => SkipChildren(acc)
      case _: PathFactor =>
        _ => SkipChildren(false)
    }) {
      val fixedZeroQuantifier =
        FixedQuantifier(UnsignedDecimalIntegerLiteral("0")(InputPosition.NONE))(InputPosition.NONE)
      val minimalPatternPart = x.element.endoRewrite {
        topDown(Rewriter.lift {
          case q: QuantifiedPath => q.copy(quantifier = fixedZeroQuantifier)(InputPosition.NONE)
        })
      }
      val stringifiedMinimalPatternPart = stringifier.patterns(minimalPatternPart)
      error(
        s"""A top-level path pattern in a `MATCH` clause must be written such that it always evaluates to at least one node pattern.
           |In this case, `$stringifiedMinimalPatternPart` would result in an empty pattern.""".stripMargin,
        x.position
      )
    }
  }

  def check(ctx: SemanticContext, element: PatternElement): SemanticCheck =
    element match {
      case x: RelationshipChain =>
        check(ctx, x.element) chain
          check(ctx, x.relationship) chain
          check(ctx, x.rightNode)

      case x: NodePattern =>
        checkNodeProperties(ctx, x.properties) chain
          checkLabelExpressions(ctx, x.labelExpression) chain
          checkNodePredicate(ctx, x.predicate)

      case PathConcatenation(factors) =>
        factors.map(check(ctx, _)).reduce(_ chain _) chain
          checkValidConcatenation(factors)

      case q @ QuantifiedPath(pattern, quantifier, _, _) =>
        def checkFeatureFlag: SemanticCheck =
          whenState(!_.features.contains(SemanticFeature.QuantifiedPathPatterns)) {
            error("Quantified path patterns are not yet supported.", q.position)
          }

        def checkContext: SemanticCheck =
          when(ctx != SemanticContext.Match) {
            error(
              s"Quantified path patterns are not allowed in ${ctx.name}, but only in MATCH clause.",
              q.position
            )
          }

        def checkContainedPatterns: SemanticCheck =
          pattern.folder.treeFold(SemanticCheck.success) {
            case quant: QuantifiedPath => acc =>
                SkipChildren(acc chain SemanticError(
                  "Quantified path patterns are not allowed to be nested.",
                  quant.position
                ))
            case shortestPaths: ShortestPaths => acc =>
                SkipChildren(acc chain SemanticError(
                  "shortestPath is only allowed as a top-level element and not inside a quantified path pattern",
                  shortestPaths.position
                ))
            case rel @ RelationshipPattern(_, _, Some(_), _, _, _) => acc =>
                SkipChildren(acc chain SemanticError(
                  "Variable length relationships cannot be part of a quantified path pattern.",
                  rel.position
                ))
          }

        def checkRelCount: SemanticCheck =
          when(pattern.folder.treeFindByClass[RelationshipPattern].isEmpty) {
            val patternStringified = stringifier.patterns(q)
            val nodeCount = pattern.folder.findAllByClass[NodePattern].size
            val nodeCountDescription = nodeCount match {
              case 1 => "one node"
              case _ => s"nodes"
            }
            error(
              s"""A quantified path pattern needs to have at least one relationship.
                 |In this case, the quantified path pattern $patternStringified consists of only $nodeCountDescription.""".stripMargin,
              q.position
            )
          }

        checkFeatureFlag chain
          checkContext chain
          checkContainedPatterns chain
          checkRelCount chain
          checkQuantifier(quantifier) chain
          withScopedStateWithVariablesFromRecordedScope(q) {
            // Here we import the variables from the previously recorded scope when we did all declarations.
            check(ctx)(pattern) chain
              q.optionalWhereExpression.foldSemanticCheck(Where.checkExpression)
          }

      case ParenthesizedPath(NamedPatternPart(variable, _)) =>
        error("Sub-path assignment is currently not supported outside quantified path patterns.", variable.position)
      case ParenthesizedPath(path @ ShortestPaths(_, _)) =>
        error(
          "shortestPath is only allowed as a top-level element and not inside a parenthesized path pattern",
          path.position
        )
      case ParenthesizedPath(pattern) =>
        check(ctx)(pattern)
    }

  private def getTypeString(factor: PathFactor) = factor match {
    case _: ParenthesizedPath => "(non-quantified) parenthesized path pattern"
    case _: QuantifiedPath    => "quantified path pattern"
    case _: RelationshipChain => "simple path pattern"
    case _: NodePattern       => "single node"
  }

  private def checkValidConcatenation(factors: Seq[PathFactor]) = {
    factors.sliding(2).map {
      case Seq(_, _: QuantifiedPath) => SemanticCheck.success
      case Seq(_: QuantifiedPath, _) => SemanticCheck.success
      case Seq(a, b) =>
        val aString = stringifier.patterns(a)
        val aTypeString = getTypeString(a)
        val bString = stringifier.patterns(b)
        val bTypeString = getTypeString(b)
        val inThisCase =
          if (aTypeString == bTypeString) {
            s"In this case, both $aString and $bString are ${aTypeString}s."
          } else {
            s"In this case, $aString is a $aTypeString and $bString is a $bTypeString."
          }
        error(
          s"""Concatenation is currently only supported for quantified path patterns.
             |$inThisCase
             |That is, neither of these is a quantified path pattern.""".stripMargin,
          b.position
        )
    }.reduce(_ chain _)
  }

  private def checkQuantifier(quantifier: GraphPatternQuantifier): SemanticCheck =
    quantifier match {
      case FixedQuantifier(UnsignedDecimalIntegerLiteral("0")) =>
        error("A quantifier for a path pattern must not be limited by 0.", quantifier.position)
      case IntervalQuantifier(Some(lower), Some(upper)) if upper.value < lower.value =>
        error(
          s"""A quantifier for a path pattern must not have a lower bound which exceeds its upper bound.
             |In this case, the lower bound ${lower.value} is greater than the upper bound ${upper.value}.""".stripMargin,
          quantifier.position
        )
      case IntervalQuantifier(_, Some(UnsignedDecimalIntegerLiteral("0"))) =>
        error("A quantifier for a path pattern must not be limited by 0.", quantifier.position)
      case _ => SemanticCheck.success
    }

  def legacyRelationshipDisjunctionError(sanitizedLabelExpression: String, isNode: Boolean = false): String = {
    if (isNode) {
      s"""Label expressions are not allowed to contain '|:'.
         |If you want to express a disjunction of labels, please use `:$sanitizedLabelExpression` instead""".stripMargin
    } else {
      s"""The semantics of using colon in the separation of alternative relationship types in conjunction with
         |the use of variable binding, inlined property predicates, or variable length is no longer supported.
         |Please separate the relationships types using `:$sanitizedLabelExpression` instead.""".stripMargin
    }
  }

  private def check(ctx: SemanticContext, x: RelationshipPattern): SemanticCheck = {
    def checkNotUndirectedWhenCreating: SemanticCheck = {
      ctx match {
        case SemanticContext.Create if x.direction == SemanticDirection.BOTH =>
          error(s"Only directed relationships are supported in ${name(ctx)}", x.position)
        case _ =>
          SemanticCheck.success
      }
    }

    def checkNoVarLengthWhenUpdating: SemanticCheck =
      when(!x.isSingleLength) {
        ctx match {
          case SemanticContext.Merge | SemanticContext.Create =>
            error(s"Variable length relationships cannot be used in ${name(ctx)}", x.position)
          case _ =>
            None
        }
      }

    def checkProperties: SemanticCheck =
      SemanticExpressionCheck.simple(x.properties) chain
        expectType(CTMap.covariant, x.properties)

    val stringifier = ExpressionStringifier()

    def checkForLegacyTypeSeparator: SemanticCheck = {
      val maybeLabelExpression = x match {
        // We will not complain about this particular case here because that is still allowed although deprecated.
        case RelationshipPattern(variable, expression, None, None, None, _)
          if !variable.exists(variable => AnonymousVariableNameGenerator.isNamed(variable.name)) &&
            expression.forall(!_.containsGpmSpecificRelTypeExpression) => None
        case RelationshipPattern(_, Some(labelExpression), _, _, _, _) => Some(labelExpression)
        case _                                                         => None
      }
      val maybeOffendingLabelExpression = maybeLabelExpression.flatMap(_.folder.treeFindByClass[ColonDisjunction])
      maybeOffendingLabelExpression.foldSemanticCheck { illegalColonDisjunction =>
        val sanitizedLabelExpression = stringifier.stringifyLabelExpression(maybeLabelExpression.get
          .replaceColonSyntax)
        error(
          legacyRelationshipDisjunctionError(sanitizedLabelExpression),
          illegalColonDisjunction.position
        )
      }
    }

    def checkForQuantifiedLabelExpression: SemanticCheck = {
      x match {
        case RelationshipPattern(_, Some(labelExpression), Some(_), _, _, _)
          if labelExpression.containsGpmSpecificRelTypeExpression =>
          error(
            """Variable length relationships must not use relationship type expressions.""".stripMargin,
            labelExpression.position
          )
        case _ => SemanticCheck.success
      }
    }

    def checkLabelExpressions(ctx: SemanticContext, labelExpression: Option[LabelExpression]): SemanticCheck =
      labelExpression.foldSemanticCheck { labelExpression =>
        when(ctx != SemanticContext.Match && labelExpression.containsGpmSpecificRelTypeExpression) {
          error(
            s"Relationship type expressions in patterns are not allowed in ${ctx.name}, but only in MATCH clause",
            labelExpression.position
          )
        } chain
          SemanticExpressionCheck.checkLabelExpression(Some(RELATIONSHIP_TYPE), labelExpression)
      }

    def checkPredicate(ctx: SemanticContext, relationshipPattern: RelationshipPattern): SemanticCheck =
      relationshipPattern.predicate.foldSemanticCheck { predicate =>
        when(ctx != SemanticContext.Match) {
          error(
            s"Relationship pattern predicates are not allowed in ${ctx.name}, but only in MATCH clause or inside a pattern comprehension",
            predicate.position
          )
        } chain relationshipPattern.length.foldSemanticCheck { _ =>
          error(
            "Relationship pattern predicates are not supported for variable-length relationships.",
            predicate.position
          )
        } ifOkChain withScopedState {
          Where.checkExpression(predicate)
        }
      }

    checkNoVarLengthWhenUpdating chain
      checkForLegacyTypeSeparator chain
      checkForQuantifiedLabelExpression chain
      checkNoParamMapsWhenMatching(x.properties, ctx) chain
      checkProperties chain
      checkValidPropertyKeyNamesInPattern(x.properties) chain
      checkLabelExpressions(ctx, x.labelExpression) chain
      checkPredicate(ctx, x) chain
      checkNotUndirectedWhenCreating
  }

  def variableIsGenerated(variable: LogicalVariable): Boolean = !AnonymousVariableNameGenerator.isNamed(variable.name)

  private def declareVariables(
    ctx: SemanticContext,
    element: PatternElement,
    quantified: Option[QuantifiedPath] = None
  ): SemanticCheck =
    element match {
      case x: RelationshipChain =>
        declareVariables(ctx, x.element, quantified) chain
          declareVariables(ctx, x.relationship, quantified) chain
          declareVariables(ctx, x.rightNode, quantified)

      case x: NodePattern =>
        x.variable.foldSemanticCheck {
          variable =>
            ctx match {
              case SemanticContext.Expression =>
                ensureDefined(variable) chain
                  expectType(CTNode.covariant, variable)
              case _ =>
                implicitVariable(variable, CTNode, quantified)
            }
        }
      case PathConcatenation(factors) =>
        factors.map(declareVariables(ctx, _, quantified)).reduce(_ chain _)

      case q @ QuantifiedPath(pattern, _, _, entityBindings) =>
        withScopedState {
          declareVariables(ctx, pattern.element, Some(q)) chain
            ensureNoPathVariable(pattern) ifOkChain
            entityBindings.foldSemanticCheck { entityBinding =>
              ensureDefined(entityBinding.singleton)
            } chain
            recordCurrentScope(q) // We need to record the inner scope of q to import the variables for later checks.
        } chain entityBindings.foldSemanticCheck { entityBinding =>
          declareVariable(entityBinding.group, _.expressionType(entityBinding.singleton).actual.wrapInList)
        }

      case ParenthesizedPath(pattern) =>
        declareVariables(ctx, pattern.element, quantified) chain
          declarePathVariable(pattern, quantified)
    }

  private def declarePathVariable(pattern: PatternPart, quantified: Option[QuantifiedPath]): SemanticCheck =
    pattern match {
      case n: NamedPatternPart => implicitVariable(n.variable, CTPath, quantified)
      case _                   => SemanticCheck.success
    }

  private def ensureNoPathVariable(pattern: PatternPart): SemanticCheck =
    pattern match {
      case n: NamedPatternPart =>
        error("Assigning a path in a quantified path pattern is not yet supported.", n.position)
      case _ => SemanticCheck.success
    }

  private def declareVariables(
    ctx: SemanticContext,
    x: RelationshipPattern,
    quantified: Option[QuantifiedPath]
  ): SemanticCheck =
    x.variable.foldSemanticCheck {
      variable =>
        val possibleType = if (x.length.isEmpty) CTRelationship else CTList(CTRelationship)

        ctx match {
          case SemanticContext.Match =>
            implicitVariable(variable, possibleType, quantified)
          case SemanticContext.Expression =>
            ensureDefined(variable) chain
              expectType(possibleType.covariant, variable)
          case _ =>
            declareVariable(variable, possibleType)
        }
    }

  private def ensureNoReferencesOutFromQuantifiedPath(pattern: Pattern): SemanticCheck = {
    val quantifiedPathPatterns = pattern.patternParts.flatMap(_.element.folder.findAllByClass[QuantifiedPath])
    quantifiedPathPatterns.foldSemanticCheck { qpp =>
      val definitionsInQpp = qpp.allVariables

      val definitionsInPattern = pattern.patternParts.flatMap(_.element.allVariables).toSet

      val definitionsOutsideQpp = definitionsInPattern.diff(definitionsInQpp)

      val referencesInQpp = qpp.folder.findAllByClass[LogicalVariable]
      val crossReferences = referencesInQpp.filter(definitionsOutsideQpp.contains)
      crossReferences.foldSemanticCheck { variable =>
        val stringifiedQpp = stringifier.patterns(qpp)
        error(
          s"""From within a quantified path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
             |In this case, ${variable.name} is defined in the same `MATCH` clause as $stringifiedQpp.""".stripMargin,
          variable.position
        )
      }
    }
  }

  /**
   * Traverse the sub-tree at astNode. Warn or fail if any duplicate relationships are found at that sub-tree.
   *
   * @param astNode the sub-tree to traverse.
   */
  private def ensureNoDuplicateRelationships(astNode: ASTNode): SemanticCheck = {
    RelationshipChain.findDuplicateRelationships(astNode).foldSemanticCheck {
      duplicate =>
        SemanticError(
          s"Cannot use the same relationship variable '${duplicate.name}' for multiple relationships",
          duplicate.position
        )
    }
  }

  private def checkNodeProperties(ctx: SemanticContext, properties: Option[Expression]): SemanticCheck =
    checkNoParamMapsWhenMatching(properties, ctx) chain
      checkValidPropertyKeyNamesInPattern(properties) chain
      SemanticExpressionCheck.simple(properties) chain
      expectType(CTMap.covariant, properties)

  private def checkNodePredicate(ctx: SemanticContext, predicate: Option[Expression]): SemanticCheck =
    predicate.foldSemanticCheck { predicate =>
      when(ctx != SemanticContext.Match) {
        error(
          s"Node pattern predicates are not allowed in ${ctx.name}, but only in MATCH clause or inside a pattern comprehension",
          predicate.position
        )
      } chain withScopedState {
        Where.checkExpression(predicate)
      }
    }

  private def checkLabelExpressions(
    ctx: SemanticContext,
    labelExpression: Option[LabelExpression]
  ): SemanticCheck =
    labelExpression.foldSemanticCheck { labelExpression =>
      when(
        labelExpression.containsGpmSpecificLabelExpression && (ctx != SemanticContext.Match && ctx != SemanticContext.Expression)
      ) {
        error(
          s"Label expressions in patterns are not allowed in ${ctx.name}, but only in MATCH clause and in expressions",
          labelExpression.position
        )
      } chain
        SemanticExpressionCheck.checkLabelExpression(Some(NODE_TYPE), labelExpression)
    }

  def checkValidPropertyKeyNamesInReturnItems(returnItems: ReturnItems): SemanticCheck = {
    val propertyKeys = returnItems.items.collect { case item =>
      item.expression.folder.findAllByClass[Property] map (prop => prop.propertyKey)
    }.flatten
    SemanticPatternCheck.checkValidPropertyKeyNames(propertyKeys)
  }

  def checkValidPropertyKeyNames(propertyKeys: Seq[PropertyKeyName]): SemanticCheck = {
    val error = propertyKeys.collectFirst {
      case key if checkValidTokenName(key.name).nonEmpty =>
        (checkValidTokenName(key.name).get, key.position)
    }
    if (error.nonEmpty) SemanticError(error.get._1, error.get._2) else None
  }

  def checkValidLabels(labelNames: Seq[SymbolicName], pos: InputPosition): SemanticCheck =
    labelNames.view.flatMap {
      case LabelName(name)   => checkValidTokenName(name)
      case RelTypeName(name) => checkValidTokenName(name)

      case LabelOrRelTypeName(name) => checkValidTokenName(name)
      case _                        => None
    }.headOption.map(message => SemanticError(message, pos))

  private def checkValidTokenName(name: String): Option[String] = {
    if (name == null || name.isEmpty || name.contains("\u0000")) {
      Some(String.format(
        "%s is not a valid token name. " + "Token names cannot be empty or contain any null-bytes.",
        if (name != null) "'" + name + "'" else "Null"
      ))
    } else {
      None
    }
  }
}

object checkNoParamMapsWhenMatching {

  def apply(properties: Option[Expression], ctx: SemanticContext): SemanticCheck = (properties, ctx) match {
    case (Some(e: Parameter), ctx) if ctx == Match || ctx == Merge =>
      SemanticError(
        s"Parameter maps cannot be used in `${ctx.name}` patterns (use a literal map instead, e.g. `{id: $$${e.name}.id}`)",
        e.position
      )
    case _ =>
      None
  }
}

object checkValidPropertyKeyNamesInPattern {

  def apply(properties: Option[Expression]): SemanticCheck = properties match {
    case Some(e: MapExpression) => SemanticPatternCheck.checkValidPropertyKeyNames(e.items.map(i => i._1))
    case _                      => None
  }
}
