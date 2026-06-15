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

import org.neo4j.cypher.internal.ast.FullSubqueryExpression
import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.PatternStringifier
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.success
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck.TypeMismatchContext
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FixedQuantifier
import org.neo4j.cypher.internal.expressions.GraphPatternQuantifier
import org.neo4j.cypher.internal.expressions.IntervalQuantifier
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.MatchMode.DifferentRelationships
import org.neo4j.cypher.internal.expressions.MatchMode.MatchMode
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Null
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.PathFactor
import org.neo4j.cypher.internal.expressions.PathLengthQuantifier
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.Pattern.SemanticContext
import org.neo4j.cypher.internal.expressions.Pattern.SemanticContext.Match
import org.neo4j.cypher.internal.expressions.Pattern.SemanticContext.Merge
import org.neo4j.cypher.internal.expressions.Pattern.SemanticContext.name
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPart.CountedSelector
import org.neo4j.cypher.internal.expressions.PatternPart.ShortestGroups
import org.neo4j.cypher.internal.expressions.PrefixedPatternPart
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
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonDisjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpressionDynamicLeafExpression
import org.neo4j.cypher.internal.label_expressions.SolvableLabelExpression
import org.neo4j.cypher.internal.notification.RepeatedRelationshipReference
import org.neo4j.cypher.internal.notification.RepeatedVarLengthRelationshipReference
import org.neo4j.cypher.internal.notification.UnboundedShortestPathNotification
import org.neo4j.cypher.internal.notification.UnsatisfiableRelationshipTypeExpression
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.SymbolicName
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.invariantTypeSpec
import org.neo4j.cypher.internal.util.topDown

object SemanticPatternCheck extends SemanticAnalysisTooling {

  // Clauses like CREATE, MERGE, patternComprehension call this method.
  // Explicit match modes is not supported for them, therefore they always have the default: DIFFERENT RELATIONSHIPS
  def check(ctx: SemanticContext, pattern: Pattern): SemanticCheck =
    check(ctx, pattern, MatchMode.default())

  def check(ctx: SemanticContext, pattern: Pattern, matchMode: MatchMode): SemanticCheck =
    declareVariablesInSeparateScope(ctx, pattern.patternParts) chain
      semanticCheckFold(pattern.patternParts)(check(ctx)) ifOkChain
      semanticCheckFold(pattern.patternParts)(checkMinimumNodeCount) ifOkChain
      when(ctx != SemanticContext.Create && ctx != SemanticContext.Insert) {
        ensureNoOutOfScopePathReferences(pattern) chain
          ensureDifferentRelationships(pattern, matchMode)
      }

  private def declareVariablesInSeparateScope(ctx: SemanticContext, parts: Seq[PatternPart]): SemanticCheck = {
    when(parts.folder.findAllByClass[FullSubqueryExpression].nonEmpty) {
      withScopedState {
        collectDeclaredVariables(ctx, parts)
      }
    } chain semanticCheckFold(parts)(declareVariables(ctx))
  }

  private def collectDeclaredVariables(ctx: SemanticContext, parts: Seq[PatternPart]): SemanticCheck =
    for {
      declared <- parts.foldSemanticCheck(declareVariables(ctx))
      sibling <- SemanticCheck.setState(declared.state.newSiblingScope)
      _ <- SemanticCheck.setState(sibling.state.importValuesFromScope(
        declared.state.currentScope.scope,
        sibling.state.currentScope.parent.get.symbolNames.intersect(
          declared.state.currentScope.symbolNames
        )
      ))
      recordedScopes <- parts.folder.findAllByClass[FullSubqueryExpression].foldSemanticCheck(recordCurrentScope(_))
    } yield recordedScopes

  def check(ctx: SemanticContext, pattern: RelationshipsPattern): SemanticCheck =
    check(ctx, pattern, MatchMode.default())

  def check(
    ctx: SemanticContext,
    pattern: RelationshipsPattern,
    matchMode: MatchMode
  ): SemanticCheck = {
    val checkPipeline =
      declareVariables(ctx, pattern.element) chain
        check(ctx, pattern.element)

    matchMode match {
      case _: DifferentRelationships =>
        checkPipeline chain
          ensureNoRepeatedRelationships(pattern) chain
          ensureNoRepeatedVarLengthRelationships(pattern)
      case _ =>
        checkPipeline
    }
  }

  def declareVariables(ctx: SemanticContext)(part: PatternPart): SemanticCheck =
    part match {
      case PrefixedPatternPart(_, _, part) =>
        declareVariables(ctx)(part)

      case x: NamedPatternPart =>
        declareVariables(ctx)(x.patternPart) chain
          declareVariable(x.variable, CTPath)

      case x: PathPatternPart =>
        (x.element, ctx) match {
          case (_: NodePattern, SemanticContext.Match) =>
            declareVariables(ctx, x.element)
          case (n: NodePattern, _) =>
            n.variable.foldSemanticCheck(declareVariable(_, CTNode)) chain
              declareVariables(ctx, n)
          case _ =>
            declareVariables(ctx, x.element)
        }

      case x: ShortestPathsPatternPart =>
        declareVariables(ctx, x.element)
    }

  def check(ctx: SemanticContext)(part: PatternPart): SemanticCheck =
    part match {
      case x: PrefixedPatternPart =>
        checkSelectorCount(x.selector) ifOkChain {
          val normalised = x.modifyElement {
            // sub-path assignment is fair game in selective path patterns, we can check it as if it was anonymous
            case parenthesizedPath @ ParenthesizedPath(NamedPatternPart(_, _), _)
              if x.isSelective => normalizeParenthesizedPath(parenthesizedPath)
            case element => element
          }
          val selector = normalised.selector.prettified
          check(ctx)(normalised.part) chain
            when(normalised.isSelective) {
              checkContext(
                ctx,
                selector,
                s"Path selectors such as `$selector`",
                normalised.selector.position
              )
            } chain
            check(normalised.selector)
        }

      case x: NamedPatternPart =>
        check(ctx)(x.patternPart)

      case x: PathPatternPart =>
        check(ctx, x.element)

      case x: ShortestPathsPatternPart =>
        def checkContainsSingle: SemanticCheck =
          x.element match {
            case RelationshipChain(_: NodePattern, r, _: NodePattern) =>
              r.properties.map {
                props => SemanticError.unsupportedUseOfProperties(props, x.name, x.position)
              }
            case _ => SemanticError.singleRelationshipPatternRequired(x.name, x.position)
          }

        def checkKnownEnds: SemanticCheck =
          (ctx, x.element) match {
            case (Match, _) => None
            case (_, RelationshipChain(l: NodePattern, _, r: NodePattern)) =>
              if (l.variable.isEmpty || r.variable.isEmpty)
                SemanticError.nodeVariableNotBound(x.name, x.position)
              else
                None
            case (_, _) =>
              None
          }

        def checkLength: SemanticCheck =
          x.element match {
            case RelationshipChain(_, rel, _) =>
              rel.length match {
                case Some(Some(Range(Some(min), _))) if min.value < 0 || min.value > 1 =>
                  error(SemanticError.invalidLowerBound(x.name, min.position))

                case Some(None) =>
                  val expressionStringifier = ExpressionStringifier(preferSingleQuotes = true)
                  val patternStringifier = PatternStringifier(expressionStringifier)
                  val pattern = patternStringifier(x.element)
                  warn(UnboundedShortestPathNotification(x.element.position, pattern))
                case _ => success
              }
            case _ => success
          }

        def checkNoQuantifiedPatterns: SemanticCheck = {
          x.element.folder.treeCollect {
            case qp: QuantifiedPath => SemanticError.qppInShortestPath(x.name, qp.position)
          }
        }

        val patternStringifier = PatternStringifier(stringifier)
        checkContext(ctx, patternStringifier.apply(x), s"${x.name}(...)", x.position) chain
          checkNoQuantifiedPatterns chain
          checkContainsSingle chain
          checkKnownEnds chain
          checkLength chain
          check(ctx, x.element)
    }

  private def checkContext(ctx: SemanticContext, expr: String, name: String, pos: InputPosition): SemanticCheck =
    ctx match {
      case SemanticContext.Merge | SemanticContext.Create =>
        SemanticError.expressionCanOnlyBeUsedInMatch(name, expr, ctx.name, pos)
      case _ => success
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
        FixedQuantifier(PathLengthQuantifier("0")(InputPosition.NONE))(InputPosition.NONE)
      val minimalPatternPart = x.element.endoRewrite {
        topDown(Rewriter.lift {
          case q: QuantifiedPath => q.copy(quantifier = fixedZeroQuantifier)(InputPosition.NONE)
        })
      }
      val stringifiedMinimalPatternPart = stringifier.patterns(minimalPatternPart)
      error(SemanticError.pathPatternNeedsAtLeastOnePattern(stringifiedMinimalPatternPart, x.position))
    }
  }

  def check(selector: PatternPart.Selector): SemanticCheck = selector match {
    case ShortestGroups(Left(countOfGroups)) if countOfGroups.value <= 0 =>
      SemanticAnalysisToolingErrorWithGqlInfo.specifiedNumberOutOfRangeError(
        "group count",
        "INTEGER",
        1,
        Long.MaxValue,
        String.valueOf(countOfGroups.value),
        "The group count needs to be greater than 0.",
        countOfGroups.position
      )
    case sel: CountedSelector => sel.count match {
        case Left(count) if count.value <= 0 =>
          SemanticAnalysisToolingErrorWithGqlInfo.specifiedNumberOutOfRangeError(
            "path count",
            "INTEGER",
            1,
            Long.MaxValue,
            String.valueOf(count.value),
            "The path count needs to be greater than 0.",
            count.position
          )
        case _ => success
      }
    case _ => success
  }

  private def checkSelectorCount(selector: PatternPart.Selector): SemanticCheck =
    selector match {
      case sel: CountedSelector => sel.count match {
          case Left(count)       => SemanticExpressionCheck.simple(count)
          case Right(countParam) => SemanticExpressionCheck.simple(countParam)
        }
      case _ => success
    }

  def check(ctx: SemanticContext, element: PatternElement): SemanticCheck =
    element match {
      case x: RelationshipChain =>
        check(ctx, x.element) chain
          check(ctx, x.relationship) chain
          checkDynamicLabels(ctx, x.relationship.labelExpression, isLabels = false) chain
          check(ctx, x.rightNode)

      case x: NodePattern =>
        checkNodeProperties(ctx, x.properties) chain
          checkLabelExpressions(ctx, x.labelExpression) chain
          checkDynamicLabels(ctx, x.labelExpression, isLabels = true) chain
          checkPredicate(ctx, x)

      case PathConcatenation(factors) =>
        factors.map(check(ctx, _)).reduce(_ chain _) chain
          checkValidJuxtaposition(factors)

      case q @ QuantifiedPath(pattern, quantifier, _, _) =>
        def checkContainedPatterns: SemanticCheck =
          pattern.folder.treeFold(SemanticCheck.success) {
            case quant: QuantifiedPath => acc =>
                SkipChildren(acc chain SemanticError.nestedQPP(
                  quant.position
                ))
            case shortestPaths: ShortestPathsPatternPart => acc =>
                SkipChildren(acc chain SemanticError.shortestPathInsideQPP(shortestPaths.position))
            case rel @ RelationshipPattern(_, _, Some(_), _, _, _) => acc =>
                SkipChildren(acc chain SemanticError.invalidUseOfVariableLengthRelationship(
                  "a quantified path pattern",
                  "Variable length relationships cannot be part of a quantified path pattern.",
                  rel.position
                ))
            case _: FullSubqueryExpression => acc => SkipChildren(acc)
          }

        def checkRelCount: SemanticCheck =
          when(pattern.folder.treeFindByClass[RelationshipPattern].isEmpty) {
            val patternStringified = stringifier.patterns(q)
            val nodeCount = pattern.folder.findAllByClass[NodePattern].size
            val nodeCountDescription = nodeCount match {
              case 1 => "one node"
              case _ => s"nodes"
            }
            error(SemanticError.qppNeedsAtLeastOneRelationship(patternStringified, nodeCountDescription, q.position))
          }
        val patternStringifier = PatternStringifier(stringifier)
        checkContext(ctx, patternStringifier.apply(q), "Quantified path patterns", element.position) chain
          checkContainedPatterns chain
          checkRelCount chain
          checkQuantifier(quantifier) chain
          withScopedStateWithVariablesFromRecordedScope(q) {
            // Here we import the variables from the previously recorded scope when we did all declarations.
            check(ctx)(pattern) chain
              q.optionalWhereExpression.foldSemanticCheck(Where.checkExpression) chain
              recordCurrentScope(q) // We need to overwrite the recorded scope of q for later checks.
          }

      case ParenthesizedPath(NamedPatternPart(variable, _), _) =>
        error(SemanticError.subPathAssignmentNotSupported(variable.position))
      case p @ ParenthesizedPath(patternPart, where) =>
        def checkContainedPatterns: SemanticCheck =
          // patternPart at this point is known to be an AnonymousPatternPart, as we have matched NamedPatternPart above
          // An AnonymousPatternPart can currently only be a ShortestPathsPatternPart or a PrefixedPatternPart.
          patternPart match {
            case shortestPaths: ShortestPathsPatternPart =>
              SemanticError.shortestPathInsideParenthesizedPathPattern(shortestPaths.name, shortestPaths.position)
            case _ => success
          }

        withScopedStateWithVariablesFromRecordedScope(p) {
          // Here we import the variables from the previously recorded scope when we did all declarations.
          check(ctx)(patternPart) chain
            checkContainedPatterns chain
            where.foldSemanticCheck(Where.checkExpression) chain
            recordCurrentScope(p) // We need to overwrite the recorded scope of p for later checks.
        }
    }

  private def getTypeString(factor: PathFactor) = factor match {
    case _: ParenthesizedPath => "(non-quantified) parenthesized path pattern"
    case _: QuantifiedPath    => "quantified path pattern"
    case _: RelationshipChain => "simple path pattern"
    case _: NodePattern       => "single node"
  }

  private def checkValidJuxtaposition(factors: Seq[PathFactor]) = {
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
        error(SemanticError.invalidNodePatternPair(inThisCase, b.position))
      case _ => SemanticCheck.success // we could get here with only one element in factors
    }.reduce(_ chain _)
  }

  private def checkQuantifier(quantifier: GraphPatternQuantifier): SemanticCheck =
    checkQuantifierValue(quantifier) ifOkChain {
      quantifier match {
        case FixedQuantifier(PathLengthQuantifier("0")) =>
          SemanticAnalysisToolingErrorWithGqlInfo.specifiedNumberOutOfRangeError(
            "quantifier for a path pattern",
            "INTEGER",
            1,
            Long.MaxValue,
            "0",
            "A quantifier for a path pattern must not be limited by 0.",
            quantifier.position
          )
        case IntervalQuantifier(Some(lower), Some(upper)) if upper.value < lower.value =>
          error(SemanticError.invalidQuantifier(lower.value, upper.value, quantifier.position))
        case IntervalQuantifier(_, Some(PathLengthQuantifier("0"))) =>
          SemanticAnalysisToolingErrorWithGqlInfo.specifiedNumberOutOfRangeError(
            "quantifier upperbound for a path pattern",
            "INTEGER",
            1,
            Long.MaxValue,
            "0",
            "A quantifier for a path pattern must not be limited by 0.",
            quantifier.position
          )
        case _ => SemanticCheck.success
      }
    }

  private def checkQuantifierValue(quantifier: GraphPatternQuantifier): SemanticCheck =
    quantifier match {
      case FixedQuantifier(value) => SemanticExpressionCheck.simple(value)
      case IntervalQuantifier(lower, upper) =>
        SemanticExpressionCheck.simple(lower) chain SemanticExpressionCheck.simple(upper)
      case _ => SemanticCheck.success
    }

  private def check(ctx: SemanticContext, x: RelationshipPattern): SemanticCheck = {
    def checkNotUndirectedWhenCreating: SemanticCheck = {
      ctx match {
        case SemanticContext.Create | SemanticContext.Insert if x.direction == SemanticDirection.BOTH =>
          SemanticError.onlyDirectedRelationshipAllowed(name(ctx), x.position)
        case _ =>
          SemanticCheck.success
      }
    }

    def checkNoVarLengthWhenUpdating: SemanticCheck =
      when(!x.isSingleLength) {
        ctx match {
          case SemanticContext.Merge | SemanticContext.Create =>
            SemanticAnalysisToolingErrorWithGqlInfo.invalidUseOfVariableLengthRelationshipError(
              name(ctx),
              s"Variable length relationships cannot be used in ${name(ctx)}",
              x.position
            )
          case _ =>
            None
        }
      }

    def checkVarLengthBounds: SemanticCheck =
      x.length match {
        case Some(Some(Range(lower, upper))) =>
          SemanticExpressionCheck.simple(lower) chain SemanticExpressionCheck.simple(upper)
        case _ => SemanticCheck.success
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
        SemanticCheck.error(SemanticError.legacyDisjunction(
          sanitizedLabelExpression,
          maybeLabelExpression.get.containsIs,
          isNode = false,
          illegalColonDisjunction.position
        ))
      }
    }

    def checkForQuantifiedLabelExpression: SemanticCheck = {
      x match {
        case RelationshipPattern(_, Some(labelExpression), Some(_), _, _, _)
          if labelExpression.containsGpmSpecificRelTypeExpression =>
          SemanticAnalysisToolingErrorWithGqlInfo.invalidUseOfVariableLengthRelationshipError(
            "combination with relationship type expressions",
            """Variable length relationships must not use relationship type expressions.""".stripMargin,
            labelExpression.position
          )
        case _ => SemanticCheck.success
      }
    }

    def unsatisfiableRelTypeExpression(labelExpression: LabelExpression): SemanticCheck = {
      val allowsForOneRelationship = SolvableLabelExpression.from(labelExpression).containsSolutionsForRelationship
      when(!allowsForOneRelationship && !labelExpression.containsDynamicLabelOrTypeExpression) {
        warn(UnsatisfiableRelationshipTypeExpression(
          labelExpression.position,
          stringifier.stringifyLabelExpression(labelExpression)
        ))
      }
    }

    def checkLabelExpressions(ctx: SemanticContext, labelExpression: Option[LabelExpression]): SemanticCheck =
      labelExpression.foldSemanticCheck { labelExpression =>
        when(
          (ctx == SemanticContext.Merge || ctx == SemanticContext.Create) && labelExpression.containsGpmSpecificRelTypeExpression
        ) {
          error(SemanticError.invalidRelTypeExpression(ctx.description, labelExpression.position))
        } chain
          unsatisfiableRelTypeExpression(labelExpression) chain
          SemanticExpressionCheck.checkLabelExpression(Some(RELATIONSHIP_TYPE), labelExpression)
      }

    def checkPredicate(ctx: SemanticContext, relationshipPattern: RelationshipPattern): SemanticCheck =
      relationshipPattern.predicate.foldSemanticCheck { predicate =>
        when(ctx != SemanticContext.Match) {
          error(SemanticError.invalidPatternPredicate("Relationship", ctx.description, predicate.position))
        } chain relationshipPattern.length.foldSemanticCheck { _ =>
          error(SemanticError.patternPredicateInVarLengthRel(predicate.position))
        } ifOkChain withScopedState {
          Where.checkExpression(predicate)
        }
      }

    checkNoVarLengthWhenUpdating chain
      checkVarLengthBounds chain
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
    element: PatternElement
  ): SemanticCheck =
    element match {
      case x: RelationshipChain =>
        declareVariables(ctx, x.element) chain
          declareVariables(ctx, x.relationship) chain
          declareVariables(ctx, x.rightNode)

      case x: NodePattern =>
        x.variable.foldSemanticCheck {
          variable =>
            ctx match {
              case SemanticContext.Expression =>
                ensureDefined(variable) chain
                  expectType(CTNode.covariant, variable)
              case _ =>
                implicitVariable(variable, CTNode)
            }
        }
      case PathConcatenation(factors) =>
        factors.map(declareVariables(ctx, _)).reduce(_ chain _)

      case q @ QuantifiedPath(pattern, _, _, entityBindings) =>
        withScopedState {
          declareVariables(ctx, pattern.element) chain
            ensureNoPathVariable(pattern) ifOkChain
            entityBindings.foldSemanticCheck { entityBinding =>
              ensureDefined(entityBinding.singleton)
            } chain
            recordCurrentScope(q) // We need to record the inner scope of q to import the variables for later checks.
        } chain entityBindings.foldSemanticCheck { entityBinding =>
          declareVariable(entityBinding.group, _.expressionType(entityBinding.singleton).actual.wrapInList)
        }

      case p @ ParenthesizedPath(pattern, _) =>
        // During later checks, we only have access to a normalized path, thus we use it here as well to be able to lookup recorded scopes later.
        val normalized = normalizeParenthesizedPath(p)

        // We will declare the path variable first so that we can verify if the variable declaration clashes with the parent scope.
        // If it does not, with scoped state will stash the parent scope and run semantic analysis on a fresh child scope and finally merge it with the parent scope.
        declarePathVariable(pattern) chain
          withScopedState {
            // Variables from parenthesized path are exported into the parent scope, because of that we can't tell whether a variable was declared inside
            // or outside of a given parenthesized path. By recording scopes before and after declaring variables, we can then later compute a diff to
            // get this information back.
            recordCurrentScope(ScopeBeforeParenthesizedPath(normalized)) chain
              declareVariables(ctx, pattern.element) chain
              recordCurrentScope(normalized) chain
              // Record the same scope again, because the current `normalized` scope
              // will be overwritten and lost after we check WHERE predicates at a later time.
              recordCurrentScope(ScopeAfterParenthesizedPath(normalized))
          } chain
          importValuesFromRecordedScope(normalized)

    }

  private def declarePathVariable(pattern: PatternPart): SemanticCheck =
    pattern match {
      case n: NamedPatternPart => declareVariable(n.variable, CTPath)
      case _                   => SemanticCheck.success
    }

  private def ensureNoPathVariable(pattern: PatternPart): SemanticCheck =
    pattern match {
      case n: NamedPatternPart =>
        error(SemanticError.pathBoundInQPP(n.position))
      case _ => SemanticCheck.success
    }

  private def declareVariables(
    ctx: SemanticContext,
    x: RelationshipPattern
  ): SemanticCheck =
    x.variable.foldSemanticCheck {
      variable =>
        val possibleType = if (x.length.isEmpty) CTRelationship else CTList(CTRelationship)

        ctx match {
          case SemanticContext.Match =>
            implicitVariable(variable, possibleType)
          case SemanticContext.Expression =>
            ensureDefined(variable) chain
              expectType(possibleType.covariant, variable)
          case _ =>
            declareVariable(variable, possibleType)
        }
    }

  private def ensureNoOutOfScopePathReferences(pattern: Pattern): SemanticCheck = {
    val elements: Seq[PatternElement] = pattern.patternParts.flatMap { patternPart =>
      patternPart.element.folder.treeCollect {
        case q: QuantifiedPath    => q
        case p: ParenthesizedPath => p
      }
    }
    elements.foldSemanticCheck {
      case q: QuantifiedPath    => ensureNoPathReferencesFromQuantifiedPath(pattern, q)
      case p: ParenthesizedPath => ensureNoPathReferencesFromParenthesizedPath(pattern, normalizeParenthesizedPath(p))
      case x =>
        throw new IllegalArgumentException(s"Expected QuantifiedPath or ParenthesizedPath, but was ${x.getClass}.")
    }
  }

  private def ensureNoPathReferencesFromQuantifiedPath(
    pattern: Pattern,
    quantifiedPath: QuantifiedPath
  ): SemanticCheck = {
    SemanticCheck.fromState { (state: SemanticState) =>
      val scope = state.recordedScopes(quantifiedPath)
      val dependencies = scope.declarationsAndDependencies.dependencies

      ensureNoReferencesToLocalPathVariable(
        pattern,
        quantifiedPath,
        dependencies,
        patternElementErrorMessageDescription = "quantified path pattern"
      )
    }
  }

  private def ensureNoPathReferencesFromParenthesizedPath(
    pattern: Pattern,
    parenthesizedPath: ParenthesizedPath
  ): SemanticCheck = {
    SemanticCheck.fromState { (state: SemanticState) =>
      val beforeScope = state.recordedScopes(ScopeBeforeParenthesizedPath(parenthesizedPath))
      val afterScope = state.recordedScopes(ScopeAfterParenthesizedPath(parenthesizedPath))
      val introducedDeclarations =
        afterScope.declarationsAndDependencies.declarations -- beforeScope.declarationsAndDependencies.declarations

      val finalScope = state.recordedScopes(parenthesizedPath)
      val dependencies = finalScope.declarationsAndDependencies.dependencies -- introducedDeclarations

      ensureNoReferencesToLocalPathVariable(
        pattern,
        parenthesizedPath,
        dependencies,
        patternElementErrorMessageDescription = "parenthesized path pattern"
      )
    }
  }

  /**
   * Verifies that the pattern does not reference a path variable defined in the same pattern.
   *
   * It does so by checking where path variables are defined.
   */
  private def ensureNoReferencesToLocalPathVariable(
    pattern: Pattern,
    patternElement: PatternElement,
    dependencies: Set[SymbolUse],
    patternElementErrorMessageDescription: String
  ): SemanticCheck = {
    { (state: SemanticState) =>
      // Since we don't open a new scope for a new MATCH clause,
      // this may contain declarations from previous MATCH clauses.
      val declarationsInCurrentScope = state.currentScope.declarationsAndDependencies.declarations

      val pathVariablesUsedInPattern = pattern.patternParts.flatMap(_.pathVariable)
      val pathVariablesDeclaredInPattern =
        pathVariablesUsedInPattern
          .map(SymbolUse(_))
          .filter(declarationsInCurrentScope)

      val referencesFromPatternElementToPattern = dependencies.intersect(pathVariablesDeclaredInPattern.toSet)
      val errors = referencesFromPatternElementToPattern.map { symbolUse =>
        val stringifiedPatternElement = stringifier.patterns(patternElement)
        SemanticError.invalidReferenceInParenthesizedPathPatternPredicate(
          stringifiedPatternElement,
          Set(symbolUse.name),
          symbolUse.asVariable.position,
          s"""From within a $patternElementErrorMessageDescription, one may only reference variables, that are already bound in a previous `MATCH` clause.
             |In this case, `${symbolUse.name}` is defined in the same `MATCH` clause as $stringifiedPatternElement.""".stripMargin
        )
      }
      SemanticCheckResult(state, errors.toSeq)
    }
  }

  private def ensureDifferentRelationships(pattern: Pattern, matchMode: MatchMode): SemanticCheck =
    matchMode match {
      case _: DifferentRelationships =>
        ensureNoRepeatedRelationships(pattern) chain
          ensureNoRepeatedVarLengthRelationships(pattern)
      case _ =>
        success
    }

  /**
   * Traverse the sub-tree at astNode. If any repeated relationships are found in that sub-tree, warn on the first occurrence.
   *
   * @param astNode the sub-tree to traverse.
   */
  private def ensureNoRepeatedRelationships(astNode: ASTNode): SemanticCheck = {
    findRepeatedRelationships(astNode, varLength = false).foldSemanticCheck {
      repeated =>
        warn(RepeatedRelationshipReference(repeated.position, repeated.name, extractPattern(astNode)))
    }
  }

  /**
   * Traverse the sub-tree at astNode. Warn if any repeated var length relationships are found in that sub-tree.
   *
   * @param astNode the sub-tree to traverse.
   */
  private def ensureNoRepeatedVarLengthRelationships(astNode: ASTNode): SemanticCheck = {
    findRepeatedRelationships(astNode, varLength = true).foldSemanticCheck { repeated =>
      warn(RepeatedVarLengthRelationshipReference(repeated.position, repeated.name, extractPattern(astNode)))
    }
  }

  private def extractPattern(astNode: ASTNode) = {
    val expressionStringifier = ExpressionStringifier(preferSingleQuotes = true)
    val patternStringifier = PatternStringifier(expressionStringifier)
    val pattern =
      astNode match {
        case p: Pattern              => patternStringifier(p)
        case r: RelationshipsPattern => patternStringifier(r.element)
        case x =>
          throw new IllegalArgumentException(s"Expected Pattern or RelationshipsPattern, but was ${x.getClass}.")
      }
    pattern
  }

  /**
   * This method will traverse into any ASTNode and find repeated relationship variables inside of RelationshipChains.
   *
   * For each rel variable that is repeated, return the first/second occurrence of that variable.
   */
  def findRepeatedRelationships(treeNode: ASTNode, varLength: Boolean): Seq[LogicalVariable] = {
    val relVariables = treeNode.folder.fold(Map[String, List[LogicalVariable]]().withDefaultValue(Nil)) {
      case RelationshipChain(_, RelationshipPattern(Some(rel), _, None, _, _, _), _) if !varLength =>
        map =>
          map.updated(rel.name, rel :: map(rel.name))
      case RelationshipChain(_, RelationshipPattern(Some(rel), _, Some(_), _, _, _), _) if varLength =>
        map =>
          map.updated(rel.name, rel :: map(rel.name))
      case _ =>
        identity
    }
    val repetitions = relVariables.values.filter(_.size > 1)
    repetitions.map(_.minBy(_.position)).toSeq
  }

  private def checkNodeProperties(ctx: SemanticContext, properties: Option[Expression]): SemanticCheck =
    checkNoParamMapsWhenMatching(properties, ctx) chain
      checkValidPropertyKeyNamesInPattern(properties) chain
      SemanticExpressionCheck.simple(properties) chain
      expectType(CTMap.covariant, properties)

  private def checkPredicate(ctx: SemanticContext, pattern: NodePattern): SemanticCheck =
    pattern.predicate.foldSemanticCheck { predicate =>
      when(ctx != SemanticContext.Match) {
        error(SemanticError.invalidPatternPredicate("Node", ctx.description, predicate.position))
      } ifOkChain withScopedState {
        Where.checkExpression(predicate)
      }
    }

  private def checkLabelExpressions(
    ctx: SemanticContext,
    labelExpression: Option[LabelExpression]
  ): SemanticCheck =
    labelExpression.foldSemanticCheck { labelExpression =>
      when(
        labelExpression.containsMatchSpecificLabelExpression && (ctx != SemanticContext.Match && ctx != SemanticContext.Expression)
      ) {
        error(
          SemanticError.invalidLabelExpressionInPattern(ctx.description, labelExpression.position)
        )
      } chain
        SemanticExpressionCheck.checkLabelExpression(Some(NODE_TYPE), labelExpression)
    }

  private def checkDynamicLabels(
    ctx: SemanticContext,
    labelExpression: Option[LabelExpression],
    isLabels: Boolean
  ): SemanticCheck = {
    labelExpression.foldSemanticCheck { labelExpression =>
      val dynamicLabelExpressions = labelExpression.folder.findAllByClass[LabelExpressionDynamicLeafExpression]
      val dynamicLabels = dynamicLabelExpressions.map(_.expression)
      when(
        ctx != SemanticContext.Match && ctx != SemanticContext.Expression
      ) {
        { (state: SemanticState) =>
          val errors = dynamicLabelExpressions.filter(!_.all).map { dynamicLabel =>
            SemanticError.invalidUseOfDynamicLabelOrType(
              if (isLabels) "labels" else "types",
              ctx.name,
              dynamicLabel.position
            )
          }
          SemanticCheckResult(state, errors)
        }
      } chain
        SemanticExpressionCheck.simple(dynamicLabels) chain
        SemanticPatternCheck.checkValidDynamicLabels(
          if (isLabels) TokenType.NodeLabel else TokenType.RelationshipType,
          dynamicLabels,
          labelExpression.position
        ) chain
        SemanticExpressionCheck.expectType(
          CTString.covariant | CTList(CTString).covariant,
          dynamicLabels,
          if (isLabels) TypeMismatchContext.DYNAMIC_LABEL else TypeMismatchContext.DYNAMIC_TYPE
        )
    }
  }

  def checkValidPropertyKeyNamesInReturnItems(returnItems: ReturnItems): SemanticCheck = {
    val propertyKeys = returnItems.items.collect { case item =>
      item.expression.folder.findAllByClass[Property] map (prop => prop.propertyKey)
    }.flatten
    SemanticPatternCheck.checkValidPropertyKeyNames(propertyKeys)
  }

  trait TokenType {
    def tokenType: String
  }

  object TokenType {
    case object PropertyName extends TokenType { override val tokenType: String = "property key" }
    case object NodeLabel extends TokenType { override val tokenType: String = "label" }
    case object RelationshipType extends TokenType { override val tokenType: String = "relationship type" }
  }

  def checkValidPropertyKeyNames(propertyKeys: Seq[PropertyKeyName]): SemanticCheck = {
    propertyKeys.collectFirst(Function.unlift(key =>
      checkValidTokenName(TokenType.PropertyName.tokenType, key.name, key.position)
    ))
  }

  def checkValidLabels(tokenType: TokenType, labelNames: Seq[SymbolicName], pos: InputPosition): SemanticCheck = {
    labelNames.view.collectFirst(Function.unlift {
      case LabelName(name)   => checkValidTokenName(tokenType.tokenType, name, pos)
      case RelTypeName(name) => checkValidTokenName(tokenType.tokenType, name, pos)

      case LabelOrRelTypeName(name) => checkValidTokenName(tokenType.tokenType, name, pos)
      case _                        => None
    })
  }

  def checkValidDynamicLabels(tokenType: TokenType, labelNames: Seq[Expression], pos: InputPosition): SemanticCheck = {
    labelNames.view.collectFirst(Function.unlift {
      case StringLiteral(name) => checkValidTokenName(tokenType.tokenType, name, pos)
      case ListLiteral(expressions) =>
        expressions.collectFirst(Function.unlift {
          case StringLiteral(name) => checkValidTokenName(tokenType.tokenType, name, pos)
          case _: Null             => checkValidTokenName(tokenType.tokenType, null, pos)
          case _                   => None
        })
      case _: Null => checkValidTokenName(tokenType.tokenType, null, pos)
      case _       => None
    })
  }

  private def checkValidTokenName(tokenType: String, name: String, pos: InputPosition): Option[SemanticError] = {
    Option.when(name == null || name.isEmpty || name.contains("\u0000"))(SemanticError.invalidToken(
      tokenType,
      name,
      pos
    ))
  }

  private def normalizeParenthesizedPath(ppp: ParenthesizedPath): ParenthesizedPath = {
    ppp match {
      case parenthesizedPath @ ParenthesizedPath(NamedPatternPart(_, patternPart), optionalWhereClause) =>
        ParenthesizedPath(patternPart, optionalWhereClause)(parenthesizedPath.position)
      case element => element
    }
  }

  // These are fake AST nodes that are introduced to recorded multiple scopes for the same parenthesized path object.
  // Normally we can only record a single scope per AST node.

  private case class ScopeBeforeParenthesizedPath(p: ParenthesizedPath) extends ASTNode {
    override def position: InputPosition = p.position
  }

  private case class ScopeAfterParenthesizedPath(p: ParenthesizedPath) extends ASTNode {
    override def position: InputPosition = p.position
  }
}

object checkNoParamMapsWhenMatching {

  def apply(properties: Option[Expression], ctx: SemanticContext): SemanticCheck = (properties, ctx) match {
    case (Some(e: Parameter), ctx) if ctx == Match || ctx == Merge =>
      SemanticError.invalidUseOfParameterMap(ctx.name, e.name, e.position)
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
