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

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.ASTSlicingPhrase.checkExpressionIsStaticInt
import org.neo4j.cypher.internal.ast.ASTSlicingPhrase.checkExpressionIsStaticNumber
import org.neo4j.cypher.internal.ast.Match.hintPrettifier
import org.neo4j.cypher.internal.ast.ReturnItems.ReturnVariables
import org.neo4j.cypher.internal.ast.ShowDatabase.ACCESS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.ADDRESS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.ALIASES_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CONSTITUENTS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CREATION_TIME_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CURRENT_PRIMARIES_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CURRENT_PROPERTY_SHARD_REPLICA_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CURRENT_SECONDARIES_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.CURRENT_STATUS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.DATABASE_ID_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.DEFAULT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.DEFAULT_LANGUAGE_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.GRAPH_SHARDS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.HOME_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.LAST_COMMITTED_TX_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.LAST_START_TIME_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.LAST_STOP_TIME_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.NAME_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.OPTIONS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.PROPERTY_SHARDS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.REPLICATION_LAG_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.REQUESTED_PRIMARIES_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.REQUESTED_PROPERTY_SHARDS_REPLICA_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.REQUESTED_SECONDARIES_COUNT_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.REQUESTED_STATUS_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.ROLE_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.SERVER_ID_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.SHARD_TX_LAG_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.STATUS_MSG_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.STORE_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.TYPE_COL
import org.neo4j.cypher.internal.ast.ShowDatabase.WRITER_COL
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorRetryThenFail
import org.neo4j.cypher.internal.ast.connectedComponents.RichConnectedComponent
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.ast.prettifier.PatternStringifier
import org.neo4j.cypher.internal.ast.prettifier.Prettifier
import org.neo4j.cypher.internal.ast.semantics.*
import org.neo4j.cypher.internal.ast.semantics.MapExtendedType
import org.neo4j.cypher.internal.ast.semantics.Scope
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisToolingErrorWithGqlInfo
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisToolingErrorWithGqlInfo.variableAlreadyDeclaredError
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.fromState
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.success
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck.when
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckContext
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.AllowClauseWithMixedLabelSyntax
import org.neo4j.cypher.internal.ast.semantics.SemanticPatternCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticPatternCheck.error
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.SemanticState.ScopeLocation
import org.neo4j.cypher.internal.ast.semantics.SymbolUse
import org.neo4j.cypher.internal.ast.semantics.TypeGenerator
import org.neo4j.cypher.internal.ast.semantics.iterableOnceSemanticChecking
import org.neo4j.cypher.internal.ast.semantics.optionSemanticChecking
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionScope
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionExpressionContext
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.DynamicRelTypeExpression
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasMappableExpressions
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.MatchMode
import org.neo4j.cypher.internal.expressions.MatchMode.DifferentRelationships
import org.neo4j.cypher.internal.expressions.MatchMode.MatchMode
import org.neo4j.cypher.internal.expressions.MatchMode.RepeatableElements
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.NonPrefixedPatternPart
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.PathMode
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPart.Selector
import org.neo4j.cypher.internal.expressions.PrefixedPatternPart
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.ScopeExpression
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.expressions.SimplePattern
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.SubqueryExpression
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.containsAggregate
import org.neo4j.cypher.internal.expressions.functions.Function.isIdFunction
import org.neo4j.cypher.internal.expressions.functions.GraphByElementId
import org.neo4j.cypher.internal.expressions.functions.GraphByName
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Disjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.DynamicLeaf
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.label_expressions.LabelExpressionPredicate
import org.neo4j.cypher.internal.notification.CartesianProductNotification
import org.neo4j.cypher.internal.notification.IdentifierShadowsVariableNotification
import org.neo4j.cypher.internal.notification.RedundantOptionalSubquery
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.DeprecatedFeature
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Namespace
import org.neo4j.cypher.internal.util.ProcedureName
import org.neo4j.cypher.internal.util.Rewritable.IteratorEq
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.helpers.LazyVal
import org.neo4j.cypher.internal.util.helpers.StringHelper.RichString
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTDateTime
import org.neo4j.cypher.internal.util.symbols.CTDuration
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTGraphRef
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.neo4j.cypher.internal.util.symbols.invariantTypeSpec
import org.neo4j.exceptions.InternalException
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlHelper
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.kernel.database.DatabaseReference
import org.neo4j.kernel.database.NormalizedDatabaseName

import java.util.stream.Collectors

import scala.annotation.tailrec
import scala.jdk.CollectionConverters.IterableHasAsJava

sealed trait Clause extends ASTNode with SemanticCheckable with SemanticAnalysisTooling {
  def name: String

  def returnVariables: ReturnVariables = ReturnVariables.empty

  final override def semanticCheck: SemanticCheck =
    clauseSpecificSemanticCheck chain
      whenState(_.features.contains(AllowClauseWithMixedLabelSyntax))(
        thenBranch = SemanticCheck.success,
        elseBranch = fromState(checkIfMixingLabelExpressionWithOldSyntax)
      ) chain
      when(shouldRunQPPChecks) {
        checkIfMixingLegacyVarLengthWithQPPs chain
          checkIfMixingLegacyShortestWithGpmFeatures
      }

  protected def shouldRunQPPChecks: Boolean = true

  private val stringifier = ExpressionStringifier()

  private def checkIfMixingLabelExpressionWithOldSyntax(
    state: SemanticState
  ): SemanticCheck = {

    sealed trait UsageContext
    case object Read extends UsageContext
    case object Write extends UsageContext
    case object ReadWrite extends UsageContext

    case class LegacyLabelExpression(labelExpression: LabelExpression) {
      def labelExprAndReplacement: (String, String) = {
        val isOrColon = if (labelExpression.containsIs) "IS " else ":"
        val prettifiedLabelExpr = isOrColon + stringifier.stringifyLabelExpression(labelExpression)
        val replacement = isOrColon + stringifier.stringifyLabelExpression(labelExpression.replaceColonSyntax)
        (prettifiedLabelExpr, replacement)
      }

      def position: InputPosition = labelExpression.position
    }

    case class LabelExpressionsPartitions(
      legacy: ListSet[LegacyLabelExpression] = ListSet.empty,
      gpm: ListSet[LabelExpression] = ListSet.empty
    ) {
      def withLegacyExpression(labelExpression: LabelExpression): LabelExpressionsPartitions =
        copy(legacy = legacy + LegacyLabelExpression(labelExpression))

      def withGPMExpression(labelExpression: LabelExpression): LabelExpressionsPartitions =
        copy(gpm = gpm + labelExpression)

      def semanticCheck: SemanticCheck = when(legacy.nonEmpty && gpm.nonEmpty) {
        // we prefer the new way, so we will only error on the "legacy" expressions
        val detailsSet: Set[(String, String, InputPosition)] = legacy.map { ls =>
          (ls.labelExprAndReplacement._1, ls.labelExprAndReplacement._2, ls.position)
        }
        val maybeErrorDetails =
          if (detailsSet.isEmpty) None
          else {
            // we report all errors on the first position as we will later on throw away everything but the first error.
            Some((detailsSet.map(_._1), detailsSet.map(_._2), detailsSet.head._3))
          }
        maybeErrorDetails match {
          case Some((labelExpressions, replacements, pos)) =>
            // We may have multiple conflicts, both with IS and with label expression symbols.
            // We just look at the first GPM label expression and decide what conflict we report
            // based on whether it contains IS.
            val conflictWithIS = gpm.head.containsIs
            if (conflictWithIS) SemanticError.mixingColonAndIs(labelExpressions, replacements, pos)
            else SemanticError.invalidLabelExpression(replacements, pos)
          case None => SemanticCheck.success
        }
      }
    }

    case class Acc(
      readPartitions: LabelExpressionsPartitions = LabelExpressionsPartitions(),
      writePartitions: LabelExpressionsPartitions = LabelExpressionsPartitions(),
      usage: UsageContext = Read
    ) {

      def inReadContext(): Acc = copy(usage = Read)
      def inWriteContext(): Acc = copy(usage = Write)
      def inReadWriteContext(): Acc = copy(usage = ReadWrite)

      private def withLegacyExpression(labelExpression: LabelExpression): Acc = usage match {
        case Read  => copy(readPartitions = readPartitions.withLegacyExpression(labelExpression))
        case Write => copy(writePartitions = writePartitions.withLegacyExpression(labelExpression))
        case ReadWrite => copy(
            readPartitions = readPartitions.withLegacyExpression(labelExpression),
            writePartitions = writePartitions.withLegacyExpression(labelExpression)
          )
      }

      private def withGPMExpression(labelExpression: LabelExpression): Acc = usage match {
        case Read  => copy(readPartitions = readPartitions.withGPMExpression(labelExpression))
        case Write => copy(writePartitions = writePartitions.withGPMExpression(labelExpression))
        case ReadWrite => copy(
            readPartitions = readPartitions.withGPMExpression(labelExpression),
            writePartitions = writePartitions.withGPMExpression(labelExpression)
          )
      }

      def sortLabelExpressionIntoPartition(
        labelExpression: LabelExpression,
        entityType: TypeSpec
      ): Acc = {
        val acc = if (labelExpression.containsIs) {
          // Only allowed in GPM
          withGPMExpression(labelExpression)
        } else this

        acc.sortLabelExpressionIntoPartitionIgnoringIs(labelExpression, entityType)
      }

      private def sortLabelExpressionIntoPartitionIgnoringIs(
        labelExpression: LabelExpression,
        entityType: TypeSpec
      ): Acc = {
        labelExpression match {
          case _: Leaf =>
            // A leaf is both GPM and legacy syntax.
            // Thus not adding to any partition.
            this
          case _: DynamicLeaf =>
            // A dynamic leaf is neither GPM nor legacy syntax.
            // Thus not adding to any partition.
            this
          case Disjunctions(children, _)
            if entityType != CTNode.invariant && // We continue here with unknown types to play it safe
              children.forall(child => child.isInstanceOf[Leaf] || child.isInstanceOf[DynamicLeaf]) =>
            // The disjunction for relationships is both GPM and legacy syntax.
            // Or in case a children is a DynamicLeaf neither GPM nor legacy syntax.
            // Thus not adding to any partition.
            this
          case x =>
            val isDefinitelyNode = entityType == CTNode.invariant
            val isDefinitelyRel = entityType == CTRelationship.invariant
            if (!isDefinitelyRel && !isDefinitelyNode) this // We don't know the type so we ignore this expression
            else if (isDefinitelyNode && x.containsGpmSpecificLabelExpression) withGPMExpression(x)
            else if (isDefinitelyRel && x.containsGpmSpecificRelTypeExpression) withGPMExpression(x)
            else withLegacyExpression(x)
        }
      }
    }

    val Acc(readPartitions, writePartitions, _) = this.folder.treeFold(Acc()) {

      // Depending on the clause, update the usage context

      case _: Merge => acc =>
          val newAcc = acc.inReadWriteContext()
          TraverseChildren(newAcc)

      case _: UpdateClause => acc =>
          val newAcc = acc.inWriteContext()
          TraverseChildren(newAcc)

      case _: Clause => acc =>
          val newAcc = acc.inReadContext()
          TraverseChildren(newAcc)

      // Partition label expressions into legacy and gpm.

      case NodePattern(_, Some(le), _, _) => acc =>
          TraverseChildren(acc.sortLabelExpressionIntoPartition(le, CTNode))

      case LabelExpressionPredicate(entity, le) => acc =>
          SkipChildren(Function.chain[Acc](Seq(
            _.inReadContext(),
            _.sortLabelExpressionIntoPartition(le, state.expressionType(entity).specified)
          ))(acc))

      case RelationshipPattern(_, Some(le), _, _, _, _) => acc =>
          TraverseChildren(acc.sortLabelExpressionIntoPartition(le, CTRelationship))
    }

    readPartitions.semanticCheck chain
      writePartitions.semanticCheck
  }

  private def checkIfMixingLegacyVarLengthWithQPPs: SemanticCheck = {
    val legacyVarLengthRelationships = this.folder.treeFold(Seq.empty[RelationshipPattern]) {
      case r @ RelationshipPattern(_, _, Some(_), _, _, _) => acc =>
          TraverseChildren(acc :+ r)
      // We should traverse into subqeries to implement CIP-40 correctly.
      // We don't, because changing this would break backwards compatibility.
      // See the "GPM Sync Rolling Agenda" notes for Nov 23, 2023
      case _: SubqueryCall | _: FullSubqueryExpression => acc => SkipChildren(acc)
    }
    val hasQPP = this.folder.treeFold(false) {
      case _: QuantifiedPath => _ =>
          SkipChildren(true)
      // We should traverse into subqeries to implement CIP-40 correctly.
      // We don't, because changing this would break backwards compatibility.
      // See the "GPM Sync Rolling Agenda" notes for Nov 23, 2023
      case _: SubqueryCall | _: FullSubqueryExpression => acc => SkipChildren(acc)
      case _                                           => acc => if (acc) SkipChildren(acc) else TraverseChildren(acc)
    }

    when(hasQPP) {
      legacyVarLengthRelationships.foldSemanticCheck { legacyVarLengthRelationship =>
        SemanticAnalysisToolingErrorWithGqlInfo.invalidUseOfVariableLengthRelationshipError(
          "combination with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*')",
          "Mixing variable-length relationships ('-[*]-') with quantified relationships ('()-->*()') or quantified path patterns ('(()-->())*') is not allowed.",
          legacyVarLengthRelationship.position
        )
      }
    }
  }

  private def checkIfMixingLegacyShortestWithGpmFeatures: SemanticCheck = {
    val legacyShortest = this.folder.findAllByClass[ShortestPathsPatternPart]

    val hasClashingGpmFeature = this.folder.treeFold(false) {
      case s: Selector if s.isSelective => _ => SkipChildren(true)
      case DifferentRelationships(true) =>
        // Allow implicit match mode
        acc => if (acc) SkipChildren(acc) else TraverseChildren(acc)
      case _: MatchMode =>
        // Forbid explicit match mode
        _ => SkipChildren(true)
      case pathMode: PathMode if !pathMode.implicitlyCreated =>
        // Forbid explicit path mode
        _ => SkipChildren(true)
      // We should traverse into subqeries to implement CIP-40 correctly.
      // We don't, because changing this would break backwards compatibility.
      case _ => acc => if (acc) SkipChildren(acc) else TraverseChildren(acc)
    }

    when(hasClashingGpmFeature) {
      legacyShortest.foldSemanticCheck { legacyVarLengthRelationship =>
        val fun = if (legacyVarLengthRelationship.single) "shortestPath" else "allShortestPaths"
        error(SemanticError.invalidUseOfShortestPath(fun, legacyVarLengthRelationship.position))
      }
    }
  }

  def clauseSpecificSemanticCheck: SemanticCheck
}

sealed trait UpdateClause extends Clause with HasMappableExpressions[UpdateClause] {
  override def returnVariables: ReturnVariables = ReturnVariables.empty
}

sealed trait CreateOrInsert extends UpdateClause {
  def pattern: Pattern.ForUpdate
}

case class LoadCSV(
  withHeaders: Boolean,
  urlString: Expression,
  variable: Variable,
  fieldTerminator: Option[StringLiteral]
)(val position: InputPosition) extends Clause with SemanticAnalysisTooling {
  override def name: String = "LOAD CSV"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    SemanticExpressionCheck.simple(urlString) chain
      expectType(CTString.covariant, urlString) chain
      checkFieldTerminator chain
      typeCheck

  private def checkFieldTerminator: SemanticCheck = {
    fieldTerminator match {
      case Some(literal) if literal.value.length != 1 =>
        error(SemanticError.invalidFieldTerminator(literal.position))
      case _ => success
    }
  }

  private def typeCheck: SemanticCheck = {
    if (withHeaders) {
      declareVariable(variable, MapExtendedType.getTypeSpec(CTMap, CTString.covariant))
    } else
      declareVariable(variable, CTList(CTString))
  }
}

object LoadCSV {
  private val FtpUserPassConnectionStringRegex = """[\w]+://.+:.+@.+""".r

  def isSensitiveUrl(url: String): Boolean = {
    FtpUserPassConnectionStringRegex.matches(url)
  }

  def fromUrl(
    withHeaders: Boolean,
    source: Expression,
    variable: Variable,
    fieldTerminator: Option[StringLiteral]
  )(position: InputPosition): LoadCSV = {
    val sensitiveSource = source match {
      case x: StringLiteral if isSensitiveUrl(x.value) => x.asSensitiveLiteral
      case x                                           => x
    }
    LoadCSV(withHeaders, sensitiveSource, variable, fieldTerminator)(position)
  }
}

case class InputDataStream(variables: Seq[Variable])(val position: InputPosition) extends Clause
    with SemanticAnalysisTooling {

  override def name: String = "INPUT DATA STREAM"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    variables.foldSemanticCheck(v => declareVariable(v, types(v)))
}

sealed trait GraphSelection extends Clause with SemanticAnalysisTooling {
  def graphReference: GraphReference
}

final case class UseGraph(graphReference: GraphReference)(val position: InputPosition) extends GraphSelection
    with ClauseAllowedOnSystem {
  override def name = "USE GRAPH"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    whenState(_.features(SemanticFeature.UseAsMultipleGraphsSelector))(
      thenBranch = checkMultiGraphSelector,
      elseBranch = whenState(_.features(SemanticFeature.UseAsSingleGraphSelector))(
        // On clause level, this feature means that only static graph references are allowed
        thenBranch = checkSingleGraphSelector,
        elseBranch = unsupported()
      )
    )

  private def unsupported(): SemanticCheck = SemanticCheck.fromFunctionWithContext { (semanticState, context) =>
    SemanticCheckResult.unableToRouteUseClauseError(
      semanticState,
      context.errorMessageProvider.createUseClauseUnsupportedError(),
      position
    )
  }

  private def checkMultiGraphSelector: SemanticCheck = {
    checkFunctionArguments chain checkWorkingGraph
  }

  private def checkFunctionArguments: SemanticCheck = {
    graphReference match {
      case ref: GraphFunctionReference =>
        ref.checkFunctionCall chain checkExpressions(ref.functionInvocation.args)
      case _: GraphDirectReference =>
        success
    }
  }

  private def checkWorkingGraph: SemanticCheck = {
    SemanticCheck.fromFunctionWithContext { (state, context) =>
      state.workingGraph match {
        // The session database reference is not forwarded in all places perform a semantic check.
        // Only record the working graph for the nested check if we know the session database
        case None => context.sessionDatabaseReference match {
            case Some(dbRef) if !repeatsSessionDatabaseReference(dbRef) =>
              SemanticCheckResult.success(state.recordWorkingGraph(Some(graphReference)))
            case _ => SemanticCheckResult.success(state)
          }
        case Some(workingGraph) =>
          if (workingGraph.semanticallyEqual(graphReference)) SemanticCheckResult.success(state)
          else SemanticCheckResult.error(
            GqlHelper.getGql42001_42N74(
              graphReference.position.offset,
              graphReference.position.line,
              graphReference.position.column,
              graphReference.print,
              workingGraph.print
            ),
            state,
            "Nested subqueries must use the same graph as their parent query",
            graphReference.position
          )
      }
    }
  }

  /**
   * Checks if a use clause repeats the session database reference.
   * For example, when connected to comp, this is valid:
   *
   * USE comp // repeats session database
   * WITH "Pete" as pete
   * CALL(pete) {
   *   USE comp.constituent // does not repeat session database
   *   MATCH (n { name: pete })
   *   RETURN n
   * }
   * RETURN n
   *
   * @param sessionDatabaseReference
   * @return
   */
  private def repeatsSessionDatabaseReference(sessionDatabaseReference: DatabaseReference): Boolean = {
    graphReference match {
      case GraphDirectReference(catalogName) =>
        sessionDatabaseReference != null &&
        new NormalizedDatabaseName(catalogName.qualifiedNameString).equals(sessionDatabaseReference.fullName())
      case GraphFunctionReference(_, _) => false
    }
  }

  private def checkExpressions(expressions: Seq[Expression]): SemanticCheck =
    expressions.foldSemanticCheck(expr =>
      SemanticExpressionCheck.check(Expression.SemanticContext.Results, expr)
    )

  private def checkSingleGraphSelector: SemanticCheck = {
    graphReference match {
      case graphReference: GraphDirectReference => checkSingleTargetGraph(graphReference)
      case graphFunction: GraphFunctionReference =>
        graphFunction.functionInvocation.function match {
          case GraphByElementId
            if graphFunction.isConstantForQuery =>
            if (graphFunction.functionInvocation.arguments.size == 1) {
              checkSingleTargetGraph(graphFunction)
            } else {
              val signature = GraphByElementId.signatures.head.getSignatureAsString
              val givenArgs = graphFunction.functionInvocation.arguments.size
              val name = graphFunction.functionInvocation.name
              SemanticCheck.error(
                SemanticError.functionCallWrongNumberOfArguments(
                  1,
                  givenArgs,
                  name,
                  signature,
                  s"argument of type ${GraphByElementId.signatures.head.argumentTypes.head.normalizedCypherTypeString()}",
                  position,
                  Some(
                    s"The procedure or function call does not provide the required number of arguments; expected 1 but got $givenArgs. The procedure or function `$name` has the signature: `$signature`."
                  )
                )
              )
            }
          case _ =>
            SemanticCheck.fromFunctionWithContext { (semanticState, context) =>
              SemanticCheckResult.error(
                GqlHelper.getGql42001_42N72(position.offset, position.line, position.column),
                semanticState,
                context.errorMessageProvider.createDynamicGraphReferenceUnsupportedError(graphReference.print),
                position
              )
            }
        }
    }
  }

  /*
   * The graph-references used inside the query must target the same graph.
   *
   * This currently checks equality and will fail if you mix direct and function graph references.
   */
  private def checkSingleTargetGraph(graphReference: GraphReference): SemanticCheck = {
    SemanticCheck.fromFunctionWithContext { (semanticState, context) =>
      semanticState.targetGraph match {
        case Some(existingTarget) =>
          if (existingTarget.equals(graphReference)) {
            SemanticCheckResult.success(semanticState)
          } else {
            SemanticCheckResult.accessingMultipleGraphsError(
              semanticState,
              context.errorMessageProvider.createMultipleGraphReferencesError(graphReference.print),
              position
            )
          }
        case None =>
          val newState = semanticState.recordTargetGraph(graphReference)
          SemanticCheckResult.success(newState)
      }
    }
  }
}

sealed trait GraphReference extends Expression with SemanticCheckable {
  override def semanticCheck: SemanticCheck = success
  def semanticallyEqual(other: Any): Boolean
  def print: String
  def dependencies: Set[LogicalVariable]
}

final case class GraphDirectReference(catalogName: CatalogName)(val position: InputPosition) extends GraphReference {
  override def print: String = catalogName.qualifiedNameString

  override def dependencies: Set[LogicalVariable] = Set.empty
  override def isConstantForQuery: Boolean = true

  override def equals(other: Any): Boolean = other match {
    case GraphDirectReference(otherName) =>
      catalogName.equals(otherName)
    case _ => false
  }

  override def hashCode(): Int = {
    catalogName.hashCode()
  }

  override def semanticallyEqual(other: Any): Boolean = other match {
    case GraphDirectReference(otherName) =>
      catalogName.equals(otherName)
    case _ => false
  }
}

final case class GraphFunctionReference(functionInvocation: FunctionInvocation, resolveByDisplayName: Boolean)(
  val position: InputPosition
) extends GraphReference with SemanticAnalysisTooling {
  override def print: String = ExpressionStringifier(_.asCanonicalStringVal).apply(functionInvocation)

  override def dependencies: Set[LogicalVariable] = functionInvocation.dependencies

  def checkFunctionCall: SemanticCheck = {
    functionInvocation.function match {
      case GraphByName      => success
      case GraphByElementId => success
      case wrong =>
        SemanticCheck.error(SemanticError.invalidType(
          "`USE clause`",
          List(
            "name of a database",
            "alias of a database",
            "graph function `graph.byName`",
            "graph function `graph.byElementId`"
          ),
          wrong.name,
          s"Type mismatch: USE clause must be given a ${CTGraphRef.toString}. Use either the name or alias of a database or the graph functions `graph.byName` and `graph.byElementId`.",
          functionInvocation.position
        ))
    }
  }
  override def isConstantForQuery: Boolean = functionInvocation.arguments.forall(_.isConstantForQuery)

  override def semanticallyEqual(other: Any): Boolean = other match {
    case GraphFunctionReference(other, _) =>
      other.equals(functionInvocation) &&
      functionInvocation.arguments.forall(_.isConstantForQuery) &&
      other.arguments.forall(_.isConstantForQuery)
    case _ => false
  }
}

trait SingleRelTypeCheck {
  self: Clause =>

  protected def checkRelTypes(patternPart: PatternPart): SemanticCheck =
    patternPart match {
      case PathPatternPart(element) => checkRelTypes(element)
      case _                        => success
    }

  protected def checkRelTypes(pattern: Pattern): SemanticCheck =
    pattern.patternParts.foldSemanticCheck(checkRelTypes)

  private def checkRelTypes(patternElement: PatternElement): SemanticCheck = {
    patternElement match {
      case RelationshipChain(element, rel, _) =>
        checkRelTypes(rel) chain checkRelTypes(element)
      case _ => success
    }
  }

  private def exactlyOneRelErrorMessage(relName: String): String =
    s"Exactly one relationship type must be specified for $relName. Did you forget to prefix your relationship type with a ':'?"

  private def tooManyTypesRelErrorMessage(maybePlain: String, exampleString: String, relName: String): String =
    s"A single ${maybePlain}relationship type ${exampleString}must be specified for ${relName}"

  private def checkRelTypes(rel: RelationshipPattern): SemanticCheck =
    rel.labelExpression match {
      case None =>
        SemanticError.invalidNumberOfRelationshipTypes(self.name, rel.position, exactlyOneRelErrorMessage(self.name))
      case Some(Leaf(RelTypeName(_), _)) => success
      case Some(DynamicLeaf(DynamicRelTypeExpression(expr, _), _)) => expr match {
          case ListLiteral(list) if list.isEmpty =>
            SemanticError.invalidNumberOfRelationshipTypes(
              self.name,
              rel.position,
              exactlyOneRelErrorMessage(self.name)
            )
          case ListLiteral(list) if list.size != 1 =>
            SemanticError.invalidNumberOfRelationshipTypes(
              self.name,
              rel.position,
              tooManyTypesRelErrorMessage("", "", self.name)
            )
          case _ => success
        }
      case Some(other) =>
        val types = other.flatten.distinct
        val (maybePlain, exampleString) =
          if (types.size == 1) ("plain ", s"like `:${types.head.name}` ")
          else ("", "")
        SemanticError.invalidNumberOfRelationshipTypes(
          self.name,
          rel.position,
          tooManyTypesRelErrorMessage(maybePlain, exampleString, self.name)
        )
    }
}

object Match {
  protected val hintPrettifier: Prettifier = Prettifier(ExpressionStringifier())
}

case class Match(
  optional: Boolean,
  matchMode: MatchMode,
  pattern: Pattern.ForMatch,
  hints: Seq[AstHint],
  where: Option[Where],
  search: Option[Search]
)(val position: InputPosition) extends Clause with SemanticAnalysisTooling {
  override def name = "MATCH"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    noImplicitJoinsInQuantifiedPathPatterns chain
      checkPathModes chain
      SemanticPatternCheck.check(
        Pattern.SemanticContext.Match,
        pattern,
        matchMode
      ) ifOkChain {
        hints.semanticCheck chain
          uniqueHints chain
          search.semanticCheck chain
          (if (search.isDefined) search.get.patternChecks(pattern) else SemanticCheck.success) chain
          where.semanticCheck chain
          checkHints chain
          checkForCartesianProducts
      }

  /**
   * Ensure that the node and relationship variables defined inside the quantified path patterns contained in this MATCH clause do not form any implicit joins.
   * It must run before checking the pattern itself – as it relies on variables defined in previous clauses to preempt some of the errors.
   * It checks for three scenarios:
   *   - a variable is defined in two or more quantified path patterns inside this MATCH clause
   *   - a variable is defined in a quantified path pattern and in a non-quantified node or relationship pattern inside this MATCH clause
   *   - a variable is defined in a quantified path pattern inside this MATCH clause and also appears in a previous MATCH clause
   */
  private def noImplicitJoinsInQuantifiedPathPatterns: SemanticCheck =
    SemanticCheck.fromState { state =>
      val (quantifiedPaths, simplePatterns) = partitionPatternElements(pattern.patternParts.map(_.element).toList)

      val allVariablesInQuantifiedPaths: List[(LogicalVariable, QuantifiedPath)] =
        for {
          quantifiedPath <- quantifiedPaths
          variable <- quantifiedPath.allVariables
        } yield variable -> quantifiedPath

      val quantifiedPathsPerVariable: Map[LogicalVariable, List[QuantifiedPath]] =
        allVariablesInQuantifiedPaths.groupMap(_._1)(_._2)

      val allVariablesInSimplePatterns: Set[LogicalVariable] =
        simplePatterns.flatMap(_.allVariables).toSet

      // Restores the user facing position of the variable when throwing errors in QPPs
      def getActualPos(variable: LogicalVariable, paths: List[QuantifiedPath]): InputPosition = {
        paths.flatMap(_.part.allVariables).find(_.name == variable.name).getOrElse(variable).position
      }

      val semanticErrors =
        quantifiedPathsPerVariable.flatMap { case (variable, paths) =>
          List(
            Option.when(paths.size > 1) {
              SemanticError.variableAlreadyDeclared(
                variable.name,
                s"The variable `${variable.name}` occurs in multiple quantified path patterns and needs to be renamed.",
                getActualPos(variable, paths)
              )
            },
            Option.when(allVariablesInSimplePatterns.contains(variable)) {
              SemanticError.variableAlreadyDeclared(
                variable.name,
                s"The variable `${variable.name}` occurs both inside and outside a quantified path pattern and needs to be renamed.",
                getActualPos(variable, paths)
              )
            },
            Option.when(state.symbol(variable.name).isDefined) {
              // Because one cannot refer to a variable defined in a subsequent clause, if the variable exists in the semantic state, then it must have been defined in a previous clause.
              SemanticError.variableAlreadyDeclared(
                variable.name,
                s"The variable `${variable.name}` is already defined in a previous clause, it cannot be referenced as a node or as a relationship variable inside of a quantified path pattern.",
                getActualPos(variable, paths)
              )
            }
          ).flatten
        }

      SemanticCheck.error(semanticErrors)
    }

  /**
   * Recursively partition sub-elements into quantified paths and "simple" patterns (nodes and relationships).
   * @param patternElements the list of elements to break down and partition
   * @param quantifiedPaths accumulator for quantified paths
   * @param simplePatterns accumulator for simple patterns
   * @return the list of quantified paths and the list of simple patterns
   */
  @tailrec
  private def partitionPatternElements(
    patternElements: List[PatternElement],
    quantifiedPaths: List[QuantifiedPath] = Nil,
    simplePatterns: List[SimplePattern] = Nil
  ): (List[QuantifiedPath], List[SimplePattern]) =
    patternElements match {
      case Nil => (quantifiedPaths.reverse, simplePatterns.reverse)
      case element :: otherElements =>
        element match {
          case PathConcatenation(factors) =>
            partitionPatternElements(factors.toList ++ otherElements, quantifiedPaths, simplePatterns)
          case ParenthesizedPath(part, _) =>
            partitionPatternElements(part.element :: otherElements, quantifiedPaths, simplePatterns)
          case quantifiedPath: QuantifiedPath =>
            partitionPatternElements(otherElements, quantifiedPath :: quantifiedPaths, simplePatterns)
          case simplePattern: SimplePattern =>
            partitionPatternElements(otherElements, quantifiedPaths, simplePattern :: simplePatterns)
        }
    }

  private def uniqueHints: SemanticCheck = {
    val errors = hints.collect {
      case h: UsingJoinHint => h.variables.toIndexedSeq
    }.flatten
      .groupBy(identity)
      .collect {
        case (variable, identHints) if identHints.size > 1 =>
          SemanticError.multipleJoinHintsForSameVariable(variable.asCanonicalStringVal, variable.position)
      }.toVector

    (state: SemanticState) => semantics.SemanticCheckResult(state, errors)
  }

  private def checkForCartesianProducts: SemanticCheck = (state: SemanticState) => {
    val expressionStringifier = ExpressionStringifier(preferSingleQuotes = true)
    val patternStringifier = PatternStringifier(expressionStringifier)
    lazy val patternString =
      try {
        patternStringifier(pattern)
      } catch {
        case _: StackOverflowError => "<StackOverflowError>"
      }
    val cc = connectedComponents(pattern.patternParts)
    // if we have multiple connected components we will have
    // a cartesian product
    val newState = cc.drop(1).foldLeft(state) { (innerState, component) =>
      innerState.addNotification(CartesianProductNotification(position, component.variables.map(_.name), patternString))
    }

    semantics.SemanticCheckResult(newState, Seq.empty)
  }

  def checkMatchMode(state: SemanticState, cypherVersion: CypherVersion): Seq[SemanticError] = {
    val scopeLocation = state.recordedScopes.getOrElse(this, state.currentScope)

    (matchMode, cypherVersion) match {
      case (mode: DifferentRelationships, CypherVersion.Cypher5) if mode.implicitlyCreated =>
        checkDifferentRelationshipsSelectivePathPatternCount(false)
      case (_, CypherVersion.Cypher5) =>
        // explicit match mode but Cypher 5
        Seq(SemanticError.matchModesNotSupportedInCypher5(matchMode.prettified, matchMode.position))
      case (_: RepeatableElements, _) =>
        checkRepeatableElements(scopeLocation)
      case (_: DifferentRelationships, _) =>
        checkDifferentRelationshipsSelectivePathPatternCount(true)
    }
  }

  /**
   * Under the match mode REPEATABLE ELEMENTS, StatefulShortestPath can go into infinite
   * loops if there is a path pattern with unbounded quantifiers. As a result, we consider patterns
   * with unbounded quantifiers — with or without selective path search — to be unsafe under REPEATABLE ELEMENTS.
   */
  private def checkRepeatableElements(scopeLocation: ScopeLocation): Seq[SemanticError] = {
    val unboundedQuantifiersErrors = pattern.patternParts.collect {
      case part if !part.isBounded => SemanticError.unsafeUsageOfRepeatableElements(part.position)
    }

    val interiorVariableErrors = checkStrictInteriorVariableOverlap(scopeLocation)
    val selectivePathPatternPredicateErrors =
      checkSelectivePathPatternPredicates(scopeLocation)

    unboundedQuantifiersErrors ++ interiorVariableErrors ++ selectivePathPatternPredicateErrors
  }

  /**
   * a strict interior variable of a shortest path pattern may not overlap with any other part of the pattern.
   */
  private def checkStrictInteriorVariableOverlap(scope: ScopeLocation): Seq[SemanticError] = {
    val symbolDefinitions = scope.availableSymbolDefinitions
    val variablesInPattern = pattern.patternParts.flatMap(_.allVariables).toSet
    val variablesInPatternDeclaredInPreviousClause =
      variablesInPattern.filterNot(symbolDefinitions contains SymbolUse(_))

    /**
     * keeping track of which variables were found and which of these were interior.
     */
    case class VariablesAndErrors(
      // these are only the strict interior nodes from selective path patterns
      strictInteriorVariables: Set[LogicalVariable] = Set.empty,
      otherVariables: Set[LogicalVariable] = Set.empty,
      errors: Seq[SemanticError] = Seq.empty
    ) {
      def addVariables(
        newStrictInteriorVariables: Set[LogicalVariable],
        newOtherVariables: Set[LogicalVariable]
      ): VariablesAndErrors =
        evaluateVariables(newStrictInteriorVariables, newOtherVariables)
          .copy(
            strictInteriorVariables = strictInteriorVariables ++ newStrictInteriorVariables,
            otherVariables = otherVariables ++ newOtherVariables
          )

      private def evaluateVariables(
        newInteriorVariables: Set[LogicalVariable],
        newOtherVariables: Set[LogicalVariable]
      ): VariablesAndErrors = {
        // variables that are strict interior to an SPP (interior except the boundary nodes) and overlap with some other part of the pattern and were not defined already
        val overlap =
          (
            (newOtherVariables intersect strictInteriorVariables) union
              (newInteriorVariables intersect otherVariables) union
              (newInteriorVariables intersect strictInteriorVariables)
          ) -- variablesInPatternDeclaredInPreviousClause

        val newErrors = overlap.map { variable =>
          SemanticError.variableAlreadyDeclared(variable.name, variable.position)
        }

        copy(errors = errors ++ newErrors)
      }
    }

    pattern
      .patternParts
      .foldLeft(VariablesAndErrors()) {
        case (acc, patternPart) if patternPart.isSelective && !patternPart.element.isInstanceOf[QuantifiedPath] =>
          acc.addVariables(patternPart.strictInteriorVariables, patternPart.boundaryNodes)
        case (acc, patternPart) =>
          acc.addVariables(Set.empty, patternPart.allVariables)
      }
      .errors
  }

  private val expressionStringifier = ExpressionStringifier(preferSingleQuotes = true)
  private val patternStringifier = PatternStringifier(expressionStringifier)

  /**
   * A selective path pattern is not allowed to have predicates referencing element variables that are defined in another path pattern in the same graph pattern.
   * It is allowed when the referenced variable is already bound by a previous clause.
   */
  private def checkSelectivePathPatternPredicates(scope: ScopeLocation): Seq[SemanticError] = {
    case class PatternPartWithReferences(patternString: String, invalidReferences: Set[LogicalVariable])

    val symbolDefinitions = scope.availableSymbolDefinitions
    val variablesInPattern = pattern.patternParts.flatMap(_.allVariables).toSet

    val variablesDefinedInPreviousClauses = symbolDefinitions.filterNot(variablesInPattern.map(SymbolUse(_)))
    val selectivePatternPartWithReferences = pattern.patternParts.collect {
      case patternPart if patternPart.isSelective =>
        // `patternPart` is a selective path pattern therefore, we must make sure that all its dependencies are either internal or defined in previous clauses

        val variablesDefinedInsideThisPatternPart = {
          val singletons = patternPart.folder.treeCollect {
            case QuantifiedPath(_, _, _, groupings) => groupings.map(_.singleton)
          }

          // allVariables contains all variables defined in this path pattern that are accessible from outside the path pattern
          patternPart.allVariables ++
            // ... whereas dependencies of the path pattern could also stem from inside a QPP and might reference the QPP's singletons.
            // We therefore also include the singletons in the comparison.
            // While singletons look the same as the grouping variables when writing the query, the Namespacer separates the two.
            // Checks that the singletons are disjoint among each other and are not referenced from outside the QPP are done as part of other semantic checks.
            singletons.flatten
        }

        PatternPartWithReferences(
          patternStringifier(patternPart),
          invalidReferences =
            patternPart.dependencies
              // allowed: references to variables in current path pattern
              .filterNot(variablesDefinedInsideThisPatternPart)
              // allowed: references to previously bounded variables
              .filterNot(variablesDefinedInPreviousClauses.map(_.asVariable))
        )
    }

    selectivePatternPartWithReferences
      .filter(_.invalidReferences.nonEmpty)
      .map { errorDetails =>
        val invalidVars = errorDetails.invalidReferences.map(_.name)
        SemanticError.invalidReferenceInParenthesizedPathPatternPredicate(
          errorDetails.patternString,
          invalidVars,
          errorDetails.invalidReferences.head.position,
          s"""From within a selective path pattern, one may only reference variables, that are already bound in a previous `MATCH` clause.
             |In this case, `${invalidVars.head}` is defined in the same `MATCH` clause as ${errorDetails.patternString}.""".stripMargin
        )
      }
  }

  /**
   * Iff we are operating under a DIFFERENT RELATIONSHIPS match mode, then a selective selector
   * (any other selector than ALL) would imply an order of evaluation of the different path patterns.
   * Therefore, once there is at least one path pattern with a selective selector, then we need to make sure
   * that there is no other path pattern beside it.
   */
  private def checkDifferentRelationshipsSelectivePathPatternCount(explicitMatchModesSupported: Boolean)
    : Seq[SemanticError] = {
    if (pattern.patternParts.size > 1) {
      pattern.patternParts
        .find(_.isSelective)
        .map(selectivePattern =>
          SemanticError.invalidUseOfMultiplePathPatterns(selectivePattern.position, explicitMatchModesSupported)
        )
        .toSeq
    } else {
      Seq.empty
    }
  }

  private def checkPathModes: SemanticCheck =
    checkMatchModePathModeCompatibility chain
      checkNoPathModeMixing chain
      checkNoPathModeVarLength

  private def checkMatchModePathModeCompatibility: SemanticCheck =
    matchMode match {
      case _: MatchMode.RepeatableElements =>
        val semanticErrors = pattern.patternParts.flatMap {
          case PrefixedPatternPart(_, _: PathMode.Walk, _) =>
            None
          case PrefixedPatternPart(_, pathMode, _) =>
            Some(SemanticError.unsupportedMatchModePathModeCombination(pathMode.prettified, pathMode.position))
        }
        SemanticCheck.error(semanticErrors)
      case _ =>
        SemanticCheck.success
    }

  private def checkNoPathModeMixing: SemanticCheck = {
    val pathModes = pattern.patternParts.collect {
      case PrefixedPatternPart(_, pathMode, _) => pathMode.prettified
    }.toSet

    when(pathModes.size > 1) {
      SemanticCheck.error(SemanticError.unsupportedPathModeMixing(pathModes, pattern.position))
    }
  }

  private def checkNoPathModeVarLength: SemanticCheck = {
    val errors =
      pattern.patternParts.flatMap {
        case PrefixedPatternPart(_, pathMode, part) if !pathMode.implicitlyCreated =>
          part.folder.treeFind[RelationshipPattern] {
            case r: RelationshipPattern => !r.isSingleLength
          }.map { relPattern =>
            SemanticError.unsupportedPathModeWithVarLength(
              patternStringifier(relPattern),
              pathMode.prettified,
              relPattern.position
            )
          }
        case _ => None
      }

    SemanticCheck.error(errors)
  }

  private def checkHints: SemanticCheck = SemanticCheck.fromFunctionWithContext { (semanticState, context) =>
    def getMissingEntityKindError(variable: String, labelOrRelTypeName: String, hint: AstHint): String = {
      val isNode = semanticState.isNode(variable)
      val typeName = if (isNode) "label" else "relationship type"
      val functionName = if (isNode) "labels" else "type"
      val operatorDescription = hint match {
        case _: UsingIndexHint => "index"
        case _: UsingScanHint  => s"$typeName scan"
        case hint => throw InternalException.internalError(this.getClass.getSimpleName, s"unexpected hint type $hint")
      }
      val typePredicates = getLabelAndRelTypePredicates(variable).distinct
      val foundTypePredicatesDescription = typePredicates match {
        case Seq()              => s"no $typeName was"
        case Seq(typePredicate) => s"only the $typeName `$typePredicate` was"
        case typePredicates     => s"only the ${typeName}s `${typePredicates.mkString("`, `")}` were"
      }

      getHintErrorForVariable(
        operatorDescription,
        hint,
        s"$typeName `$labelOrRelTypeName`",
        foundTypePredicatesDescription,
        variable,
        s"""Predicates must include the $typeName literal `$labelOrRelTypeName`.
            | That is, the function `$functionName()` is not compatible with indexes.""".stripLinesAndMargins
      )
    }

    def getMissingPropertyError(hint: UsingIndexHint): String = {
      val variable = hint.variable.name
      val propertiesInHint = hint.properties
      val plural = propertiesInHint.size > 1
      val foundPropertiesDescription = getPropertyPredicates(variable) match {
        case Seq()         => "none was"
        case Seq(property) => s"only `$property` was"
        case properties    => s"only `${properties.mkString("`, `")}` were"
      }
      val missingPropertiesNames = propertiesInHint.map(prop => s"`${prop.name}`").mkString(", ")
      val missingPropertiesDescription = s"the ${if (plural) "properties" else "property"} $missingPropertiesNames"

      getHintErrorForVariable(
        "index",
        hint,
        missingPropertiesDescription,
        foundPropertiesDescription,
        variable,
        """Supported predicates are:
          | equality comparison, inequality (range) comparison, `STARTS WITH`,
          | `IN` condition or checking property existence.
          | The comparison cannot be performed between two property values.""".stripLinesAndMargins
      )
    }

    def getHintErrorForVariable(
      operatorDescription: String,
      hint: AstHint,
      missingThingDescription: String,
      foundThingsDescription: String,
      variable: String,
      additionalInfo: String
    ): String = {
      val isNode = semanticState.isNode(variable)
      val entityName = if (isNode) "node" else "relationship"

      getHintError(
        operatorDescription,
        hint,
        missingThingDescription,
        foundThingsDescription,
        s"the $entityName `$variable`",
        entityName,
        additionalInfo
      )
    }

    def getHintError(
      operatorDescription: String,
      hint: AstHint,
      missingThingDescription: String,
      foundThingsDescription: String,
      entityDescription: String,
      entityName: String,
      additionalInfo: String
    ): String = {
      context.errorMessageProvider.createMissingPropertyLabelHintError(
        operatorDescription,
        hintPrettifier.asString(hint),
        missingThingDescription,
        foundThingsDescription,
        entityDescription,
        entityName,
        additionalInfo
      )
    }

    val error: Option[SemanticErrorDef] = {
      if (isUnfulfillable(where)) {
        // If the query is unfulfillable, then it is rewritten in UnfulfillableQueryRewriter.
        // The rewritten version might not have the element anymore where the hint is referring to, for example a label.
        // In that case we should not throw an error based on this hint.
        Option.empty
      } else {
        hints.collectFirst {
          case hint @ UsingIndexHint(Variable(variable), LabelOrRelTypeName(labelOrRelTypeName), _, _, _)
            if !containsLabelOrRelTypePredicate(variable, labelOrRelTypeName) =>
            val prettyHint = hintPrettifier.asString(hint)
            val isNode = semanticState.isNode(variable)
            val entity = if (isNode) "NODE" else "RELATIONSHIP"
            val legacyMessage = getMissingEntityKindError(variable, labelOrRelTypeName, hint)
            SemanticError.missingHintPredicate(legacyMessage, prettyHint, entity, variable, hint.position)
          case hint @ UsingIndexHint(Variable(variable), LabelOrRelTypeName(_), properties, _, _)
            if !containsPropertyPredicates(variable, properties) =>
            val prettyHint = hintPrettifier.asString(hint)
            val isNode = semanticState.isNode(variable)
            val entity = if (isNode) "NODE" else "RELATIONSHIP"
            SemanticError.missingHintPredicate(
              getMissingPropertyError(hint),
              prettyHint,
              entity,
              variable,
              hint.position
            )
          case hint @ UsingScanHint(Variable(variable), LabelOrRelTypeName(labelOrRelTypeName))
            if !containsLabelOrRelTypePredicate(variable, labelOrRelTypeName) =>
            val prettyHint = hintPrettifier.asString(hint)
            val isNode = semanticState.isNode(variable)
            val entity = if (isNode) "NODE" else "RELATIONSHIP"
            val legacyMessage = getMissingEntityKindError(variable, labelOrRelTypeName, hint)
            SemanticError.missingHintPredicate(legacyMessage, prettyHint, entity, variable, hint.position)
          case hint @ UsingJoinHint(_) if pattern.length == 0 =>
            SemanticError.cannotUseJoinHint(hint, hintPrettifier.asString(hint))
        }
      }
    }
    SemanticCheckResult(semanticState, error.toSeq)
  }

  private def isUnfulfillable(maybeWhere: Option[Where]): Boolean = {
    if (maybeWhere.isEmpty) {
      false
    } else {
      isUnfulfillable(maybeWhere.get.expression)
    }
  }

  private def isUnfulfillable(expr: Expression): Boolean = {
    expr match {
      case _: False => true // UnfulfillableQueryRewriter should have removed all expressions and created only False
      case _        => false // It might be fulfillable
    }
  }

  private[ast] def containsPropertyPredicates(variable: String, propertiesInHint: Seq[PropertyKeyName]): Boolean = {
    val propertiesInPredicates: Seq[String] = getPropertyPredicates(variable)

    propertiesInHint.forall(p => propertiesInPredicates.contains(p.name))
  }

  private def getPropertyPredicates(variable: String): Seq[String] = {
    where.map(w => collectPropertiesInPredicates(variable, w.expression)).getOrElse(Seq.empty[String]) ++
      pattern.folder.treeFold(Seq.empty[String]) {
        case NodePattern(Some(Variable(id)), _, properties, predicate) if variable == id =>
          acc =>
            SkipChildren(acc ++ collectPropertiesInPropertyMap(properties) ++ predicate.map(
              collectPropertiesInPredicates(variable, _)
            ).getOrElse(Seq.empty[String]))
        case RelationshipPattern(Some(Variable(id)), _, _, properties, predicate, _) if variable == id =>
          acc =>
            SkipChildren(acc ++ collectPropertiesInPropertyMap(properties) ++ predicate.map(
              collectPropertiesInPredicates(variable, _)
            ).getOrElse(Seq.empty[String]))
      }
  }

  private def collectPropertiesInPropertyMap(properties: Option[Expression]): Seq[String] =
    properties match {
      case Some(MapExpression(prop)) => prop.map(_._1.name)
      case _                         => Seq.empty[String]
    }

  private def collectPropertiesInPredicates(variable: String, whereExpression: Expression): Seq[String] =
    whereExpression.folder.treeFold(Seq.empty[String]) {
      case Equals(Property(Variable(`variable`), PropertyKeyName(name)), other) if applicable(other) =>
        acc => SkipChildren(acc :+ name)
      case Equals(other, Property(Variable(`variable`), PropertyKeyName(name))) if applicable(other) =>
        acc => SkipChildren(acc :+ name)
      case In(Property(Variable(`variable`), PropertyKeyName(name)), _) =>
        acc => SkipChildren(acc :+ name)
      case IsNotNull(Property(Variable(`variable`), PropertyKeyName(name))) =>
        acc => SkipChildren(acc :+ name)
      case IsTyped(Property(Variable(`variable`), PropertyKeyName(name)), typeName) if !typeName.isNullable =>
        acc => SkipChildren(acc :+ name)
      case IsNormalized(Property(Variable(`variable`), PropertyKeyName(name)), _) =>
        acc => SkipChildren(acc :+ name)
      case StartsWith(Property(Variable(`variable`), PropertyKeyName(name)), _) =>
        acc => SkipChildren(acc :+ name)
      case EndsWith(Property(Variable(`variable`), PropertyKeyName(name)), _) =>
        acc => SkipChildren(acc :+ name)
      case Contains(Property(Variable(`variable`), PropertyKeyName(name)), _) =>
        acc => SkipChildren(acc :+ name)
      case FunctionInvocation(
          FunctionName(Namespace(List(namespace)), functionName),
          _,
          Seq(Property(Variable(`variable`), PropertyKeyName(name)), _, _),
          _,
          _,
          _,
          _
        ) if namespace.equalsIgnoreCase("point") && functionName.equalsIgnoreCase("withinBBox") =>
        acc => SkipChildren(acc :+ name)
      case expr: InequalityExpression =>
        acc =>
          val newAcc: Seq[String] = Seq(expr.lhs, expr.rhs).foldLeft(acc) { (acc, expr) =>
            expr match {
              case Property(Variable(`variable`), PropertyKeyName(name)) =>
                acc :+ name
              case FunctionInvocation(
                  FunctionName(Namespace(List(namespace)), functionName),
                  _,
                  Seq(Property(Variable(id), PropertyKeyName(name)), _),
                  _,
                  _,
                  _,
                  _
                )
                if id == variable && namespace.equalsIgnoreCase("point") && functionName.equalsIgnoreCase("distance") =>
                acc :+ name
              case _ =>
                acc
            }
          }
          SkipChildren(newAcc)
      case _: Where | _: And | _: Ands | _: Set[_] | _: Seq[_] | _: Or | _: Ors | _: Not =>
        acc => TraverseChildren(acc)
      case _ =>
        acc => SkipChildren(acc)
    }

  /**
   * Checks validity of the other side, X, of expressions such as
   * `USING INDEX ON n:Label(prop) WHERE n.prop = X (or X = n.prop)`
   *
   * Returns true if X is a valid expression in this context, otherwise false.
   */
  private def applicable(other: Expression): Boolean = {
    other match {
      case f: FunctionInvocation => !isIdFunction(f)
      case _                     => true
    }
  }

  private[ast] def containsLabelOrRelTypePredicate(variable: String, labelOrRelType: String): Boolean =
    getLabelAndRelTypePredicates(variable).contains(labelOrRelType)

  private def getLabelsFromLabelExpression(labelExpression: LabelExpression) = {
    labelExpression.flatten.map(_.name)
  }

  private def getLabelAndRelTypePredicates(variable: String): Seq[String] = {
    val inlinedRelTypes = pattern.folder.fold(Seq.empty[String]) {
      case RelationshipPattern(Some(Variable(id)), Some(labelExpression), _, _, _, _) if variable == id =>
        list => list ++ getLabelsFromLabelExpression(labelExpression)
    }

    val labelExpressionLabels: Seq[String] = pattern.folder.fold(Seq.empty[String]) {
      case NodePattern(Some(Variable(id)), Some(labelExpression), _, _) if variable == id =>
        list => list ++ getLabelsFromLabelExpression(labelExpression)
    }

    val (predicateLabels, predicateRelTypes) = where match {
      case Some(innerWhere) => innerWhere.folder.treeFold((Seq.empty[String], Seq.empty[String])) {
          // These are predicates from the match pattern that were rewritten
          case HasLabels(Variable(id), predicateLabels) if id == variable => {
            case (ls, rs) => SkipChildren((ls ++ predicateLabels.map(_.name), rs))
          }
          case HasTypes(Variable(id), predicateRelTypes) if id == variable => {
            case (ls, rs) => SkipChildren((ls, rs ++ predicateRelTypes.map(_.name)))
          }
          // These are predicates in the where clause that have not been rewritten yet.
          case LabelExpressionPredicate(Variable(id), labelExpression) if id == variable => {
            case (ls, rs) =>
              val labelOrRelTypes = getLabelsFromLabelExpression(labelExpression)
              SkipChildren((ls ++ labelOrRelTypes, rs ++ labelOrRelTypes))
          }
          case _: Where | _: And | _: Ands | _: Set[_] | _: Seq[_] | _: Or | _: Ors =>
            acc => TraverseChildren(acc)
          case _ =>
            acc => SkipChildren(acc)
        }
      case None => (Seq.empty, Seq.empty)
    }

    val allLabels = labelExpressionLabels ++ predicateLabels
    val allRelTypes = inlinedRelTypes ++ predicateRelTypes
    allLabels ++ allRelTypes
  }

  def allExportedVariables: Set[LogicalVariable] = {

    val patternVariables = pattern.patternParts.folder.treeFold(Set.empty[LogicalVariable]) {
      case _: ScopeExpression          => acc => SkipChildren(acc)
      case logicalVar: LogicalVariable => acc => TraverseChildren(acc ++ Set(logicalVar))
    }

    val maybeScoreVariable = search match {
      case Some(Search(_, Some(score), _, _, _, _)) => Set(score)
      case _                                        => Set.empty
    }

    patternVariables ++ maybeScoreVariable
  }
}

case class Merge(pattern: NonPrefixedPatternPart, actions: Seq[MergeAction], where: Option[Where] = None)(
  val position: InputPosition
) extends UpdateClause with SingleRelTypeCheck {

  override def name = "MERGE"

  override protected def shouldRunQPPChecks: Boolean = false

  override def mapExpressions(f: Expression => Expression): UpdateClause =
    copy(pattern.mapExpressions(f), actions.map(_.mapExpressions(f)), where.map(_.mapExpressions(f)))(this.position)

  private def checkNoSubqueryInMerge: SemanticCheck = {
    val hasSubqueryExpression = Seq(pattern, actions).folder.treeCollect {
      case e: FullSubqueryExpression => e
    }

    hasSubqueryExpression match {
      case subquery +: _ =>
        SemanticCheck.error(SemanticError.invalidSubqueryInMerge(subquery.position))
      case _ => success
    }
  }

  private def checkNoPatternComprehensionInMergeSubClause: SemanticCheck = {
    actions.folder.treeFindByClass[SubqueryExpression]
      .map(subquery => SemanticCheck.error(SemanticError.invalidSubqueryInMerge(subquery.position)))
      .getOrElse(success)
  }

  override def clauseSpecificSemanticCheck: SemanticCheck = {
    val updatePattern = Pattern.ForUpdate(Seq(pattern))(pattern.position)
    SemanticPatternCheck.check(Pattern.SemanticContext.Merge, updatePattern) chain
      actions.semanticCheck chain
      checkRelTypes(pattern) chain
      where.semanticCheck chain
      SemanticCheck.fromState { state =>
        // Only check checkNoSubqueryInMerge the first time.
        // Afterwards we can have rewritten PatternComprehensions to COLLECT subqueries which would now fail this check.
        if (state.semanticCheckHasRunOnce) success
        else checkNoSubqueryInMerge
      } chain
      checkNoPatternComprehensionInMergeSubClause chain
      SemanticState.recordCurrentScope(updatePattern)
  }
}

object Merge {
  // MERGE (a)-[r]->(b {prop: a.prop})
  //                          ^
  case object SelfReference extends DeprecatedFeature.DeprecatedIn5ErrorIn25
}

case class Create(pattern: Pattern.ForUpdate)(val position: InputPosition) extends CreateOrInsert
    with SingleRelTypeCheck {
  override def name = "CREATE"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    SemanticPatternCheck.check(Pattern.SemanticContext.Create, pattern) chain
      checkRelTypes(pattern) chain
      SemanticState.recordCurrentScope(pattern)

  override protected def shouldRunQPPChecks: Boolean = false

  override def mapExpressions(f: Expression => Expression): UpdateClause =
    copy(pattern.mapExpressions(f))(this.position)
}

object Create {
  // CREATE (a), (b {prop: a.prop})
  //                       ^
  case object SelfReferenceAcrossPatterns extends DeprecatedFeature.DeprecatedIn5ErrorIn25
}

case class Insert(pattern: Pattern.ForUpdate)(val position: InputPosition) extends CreateOrInsert
    with SingleRelTypeCheck {
  override def name = "INSERT"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    SemanticPatternCheck.check(Pattern.SemanticContext.Insert, pattern) chain
      SemanticState.recordCurrentScope(pattern)

  override protected def shouldRunQPPChecks: Boolean = false

  override def mapExpressions(f: Expression => Expression): UpdateClause =
    copy(pattern.mapExpressions(f))(this.position)
}

case class SetClause(items: Seq[SetItem])(val position: InputPosition) extends UpdateClause {
  override def name = "SET"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    items.semanticCheck chain fromState(checkIfMixingIsWithMultipleLabels)

  override def mapExpressions(f: Expression => Expression): UpdateClause =
    copy(items.map(_.mapExpressions(f)))(this.position)

  private def checkIfMixingIsWithMultipleLabels(state: SemanticState): SemanticCheck = {
    // Check for the IS keyword
    val containsIs = this.folder.treeExists {
      case _ @SetLabelItem(_, _, _, true) => true
    }

    // Check for multiple labels in the same item
    val multipleLabels = this.folder.treeExists {
      case _ @SetLabelItem(_, labels, dynamicLabels, _) if labels.size + dynamicLabels.size > 1 => true
    }

    // If both were present, throw error with improvement suggestion: n:A:B => n IS A, n IS B
    when(containsIs && multipleLabels) {
      val prettifier = Prettifier(ExpressionStringifier())
      val setItems: Seq[SetItem] = this.items.flatMap {
        case s @ SetLabelItem(variable, labels, dynamicLabels, _) =>
          (labels.map(label =>
            SetLabelItem(variable, Seq(label), Seq.empty, containsIs = true)(s.position)
          ) ++ dynamicLabels.map(dynamicLabel =>
            SetLabelItem(variable, Seq.empty, Seq(dynamicLabel), containsIs = true)(s.position)
          )).sortBy(setItem =>
            if (setItem.labels.nonEmpty) setItem.labels.head.position.column
            else setItem.dynamicLabels.head.position.column
          )
        case x => Seq(x)
      }
      val replacement = prettifier.prettifySetItems(setItems)
      SemanticError.mixingIsWithMultipleLabels(name, replacement, position)
    }
  }
}

case class Delete(expressions: Seq[Expression], forced: Boolean)(val position: InputPosition) extends UpdateClause {
  override def name = "DELETE"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    SemanticExpressionCheck.simple(expressions) chain
      warnAboutDeletingLabels chain
      expectType(CTNode.covariant | CTRelationship.covariant | CTPath.covariant, expressions)

  private def warnAboutDeletingLabels =
    expressions.filter(e => e.isInstanceOf[LabelExpressionPredicate]) map {
      e => SemanticError.invalidDelete(e.position)
    }

  override def mapExpressions(f: Expression => Expression): UpdateClause = copy(expressions.map(f))(this.position)
}

case class Remove(items: Seq[RemoveItem])(val position: InputPosition) extends UpdateClause {
  override def name = "REMOVE"

  override def clauseSpecificSemanticCheck: SemanticCheck =
    items.semanticCheck chain checkIfMixingIsWithMultipleLabels()

  override def mapExpressions(f: Expression => Expression): UpdateClause =
    copy(items.map(_.mapExpressions(f)))(this.position)

  private def checkIfMixingIsWithMultipleLabels(): SemanticCheck = {
    // Check for the IS keyword
    val containsIs = this.folder.treeExists {
      case _ @RemoveLabelItem(_, _, _, true) => true
    }

    // Check for multiple labels in the same item
    val multipleLabels = this.folder.treeExists {
      case _ @RemoveLabelItem(_, labels, dynamicLabels, _) if labels.size + dynamicLabels.size > 1 => true
    }

    // If both were present, throw error with improvement suggestion: n:A:B => n IS A, n IS B
    when(containsIs && multipleLabels) {
      val prettifier = Prettifier(ExpressionStringifier())
      val removeItems: Seq[RemoveItem] = this.items.flatMap {
        case s @ RemoveLabelItem(variable, labels, dynamicLabels, _) =>
          (labels.map(label =>
            RemoveLabelItem(variable, Seq(label), Seq.empty, containsIs = true)(s.position)
          ) ++ dynamicLabels.map(dynamicLabel =>
            RemoveLabelItem(variable, Seq.empty, Seq(dynamicLabel), containsIs = true)(s.position)
          )).sortBy(removeItem =>
            if (removeItem.labels.nonEmpty) removeItem.labels.head.position.column
            else removeItem.dynamicLabels.head.position.column
          )
        case x => Seq(x)
      }
      val replacement = prettifier.prettifyRemoveItems(removeItems)
      SemanticError.mixingIsWithMultipleLabels(name, replacement, position)
    }
  }
}

case class Foreach(
  variable: Variable,
  expression: Expression,
  updates: Seq[Clause]
)(val position: InputPosition) extends UpdateClause {
  override def name = "FOREACH"

  override def mapExpressions(f: Expression => Expression): UpdateClause = {
    val mappedUpdates = updates.map {
      case uc: UpdateClause => uc.mapExpressions(f)
      case _                => throw new IllegalStateException("Foreach is expected to only have updating sub-clauses.")
    }
    copy(
      f(variable).asInstanceOf[Variable],
      f(expression),
      mappedUpdates
    )(this.position)
  }

  override def clauseSpecificSemanticCheck: SemanticCheck =
    SemanticExpressionCheck.simple(expression) chain
      expectType(CTList(CTAny).covariant | CTAny.covariant, expression) chain
      updates.filter(!_.isInstanceOf[UpdateClause]).map(c => {
        SemanticError.invalidForeach(c.name, c.position)
      }) ifOkChain
      withScopedState {
        val possibleInnerTypes: TypeGenerator = types(expression)(_).unwrapPotentialLists
        declareVariable(variable, possibleInnerTypes) chain updates.semanticCheck
      }
}

case class Unwind(
  expression: Expression,
  variable: Variable
)(val position: InputPosition, val useForInSyntax: Boolean = false) extends Clause with SemanticAnalysisTooling {
  override def name: String = if (useForInSyntax) "FOR" else "UNWIND"

  override def dup(children: Seq[AnyRef]): Unwind.this.type =
    if (children.iterator eqElements this.treeChildren)
      this
    else {
      children match {
        case Seq(e: Expression, v: Variable) =>
          copy(expression = e, variable = v)(position, useForInSyntax).asInstanceOf[this.type]
        case _ =>
          throw new IllegalStateException(
            s"Failed rewriting $this\nTried using children: ${children.mkString(",")}"
          )
      }
    }

  override def clauseSpecificSemanticCheck: SemanticCheck =
    SemanticExpressionCheck.check(SemanticContext.Simple, expression) chain
      expectType(CTList(CTAny).covariant | CTAny.covariant, expression) ifOkChain {
        val possibleInnerTypes: TypeGenerator = types(expression)(_).unwrapPotentialLists
        declareVariable(variable, possibleInnerTypes)
      }
}

sealed trait OptionalState
case object NonOptional extends OptionalState
case object Optional extends OptionalState
case object RewrittenOptional extends OptionalState

abstract class CallClause extends Clause {
  override def name = "CALL"

  def procedureName: ProcedureName
  def containsNoUpdates: Boolean
  def yieldAll: Boolean
  def optionalState: OptionalState
  def optional: Boolean = optionalState == Optional

  def asUnresolvedCall: UnresolvedCall

  def argumentCheck: SemanticCheck
  def resultCheck: SemanticCheck
  def invalidAggregationCheck: SemanticCheck

  def checkArgumentForAggregation(expr: Expression): SemanticCheck =
    expr.findAggregate match {
      case Some(agg) =>
        val prettifier = ExpressionStringifier()
        SemanticCheck.error(
          SemanticError.aggregateExpressionsNotAllowedInProcedureCallArgument(
            prettifier(agg),
            agg.position
          )
        )
      case _ => success
    }

  override def clauseSpecificSemanticCheck: SemanticCheck =
    argumentCheck chain resultCheck chain invalidAggregationCheck
}

case class UnresolvedCall(
  override val procedureName: ProcedureName,
  // None: No arguments given
  declaredArguments: Option[Seq[Expression]] = None,
  // None: No results declared  (i.e. no "YIELD" part or "YIELD *")
  declaredResult: Option[ProcedureResult] = None,
  isStandalone: Boolean = false,
  // YIELD *
  override val yieldAll: Boolean = false,
  override val optionalState: OptionalState = NonOptional
)(val position: InputPosition) extends CallClause {

  def fullName: String = procedureName.fullName

  override def asUnresolvedCall: UnresolvedCall = this

  override def returnVariables: ReturnVariables =
    ReturnVariables(
      includeExisting = false,
      declaredResult.map(_.items.map(_.variable).toList).getOrElse(List.empty)
    )

  override val argumentCheck: SemanticCheck = declaredArguments.map(
    // could this be checked with SemanticContext.Simple to make the invalidExpressionsCheck obsolete?
    SemanticExpressionCheck.check(SemanticContext.Results, _)
  ).getOrElse(success)
  override val resultCheck: SemanticCheck = declaredResult.map(_.semanticCheck).getOrElse(success)

  override val invalidAggregationCheck: SemanticCheck =
    declaredArguments.getOrElse(Seq.empty).foldSemanticCheck(arg => checkArgumentForAggregation(arg))

  // At this stage we can't know this, so we assume the CALL is non updating,
  // it should be rechecked when the call is resolved
  override def containsNoUpdates = true

  // Used to throw the correct error message if the optional procedure call is wrapped in an outer subquery.
  // See wrapOptionalCallProcedure.scala for more information.
  def wrappedOptional(subqueryScope: ScopeLocation): SemanticCheck = {
    declaredResult.map(_.items.map(_.variable).toList).getOrElse(List.empty).foldSemanticCheck(result =>
      subqueryScope.localSymbol(result.name) match {
        case Some(_) => variableAlreadyDeclaredError(
            result.name,
            result.position
          )
        case _ => success
      }
    )
  }
}

sealed trait HorizonClause extends Clause with SemanticAnalysisTooling {
  override def clauseSpecificSemanticCheck: SemanticCheck = SemanticState.recordCurrentScope(this)

  def semanticCheckContinuation(previousScope: Scope, outerScope: Option[Scope] = None): SemanticCheck
}

object ProjectionClause {

  def unapply(arg: ProjectionClause)
    : Option[(Boolean, ReturnItems, Option[GroupBy], Option[OrderBy], Option[Skip], Option[Limit], Option[Where])] = {
    arg match {
      case With(distinct, ri, groupBy, orderBy, skip, limit, where, _) =>
        Some((distinct, ri, groupBy, orderBy, skip, limit, where))
      case Return(distinct, ri, groupBy, orderBy, skip, limit, _, _, _) =>
        Some((distinct, ri, groupBy, orderBy, skip, limit, None))
      case Yield(ri, orderBy, skip, limit, where, _) => Some((false, ri, None, orderBy, skip, limit, where))
    }
  }

  case class Subclauses(
    groupBy: Option[GroupBy],
    orderBy: Option[OrderBy],
    skip: Option[Skip],
    limit: Option[Limit],
    where: Option[Where]
  ) {

    def sortAndPredicateExpressions: Seq[Expression] = {

      val sortExpressions = orderBy.map(_.sortItems.map(_.expression)).toSeq.flatten
      val predicateExpressions = where.map(_.expression)

      (sortExpressions ++ predicateExpressions).toSeq
    }

    def hasSubclause: Boolean =
      groupBy.isDefined || orderBy.isDefined || skip.isDefined || limit.isDefined || where.isDefined
  }

  case class Elements(
    distinct: Boolean,
    items: Seq[ReturnItem],
    subclauses: Subclauses,
    clauseType: ClauseType,
    projectionType: ProjectionType
  )

  object Elements {

    def apply(arg: ProjectionClause): Elements = {
      arg match {
        case With(distinct, ReturnItems(projectionType, items, _), groupBy, orderBy, skip, limit, where, withType) =>
          Elements(distinct, items, Subclauses(groupBy, orderBy, skip, limit, where), withType, projectionType)
        case Return(distinct, ReturnItems(projectionType, items, _), groupBy, orderBy, skip, limit, _, returnType, _) =>
          Elements(distinct, items, Subclauses(groupBy, orderBy, skip, limit, None), returnType, projectionType)
        case Yield(ReturnItems(projectionType, items, _), orderBy, skip, limit, where, yieldType) =>
          Elements(distinct = false, items, Subclauses(None, orderBy, skip, limit, where), yieldType, projectionType)
      }
    }
  }

  def checkAliasedReturnItems(returnItems: ReturnItems, clauseName: String): SemanticState => Seq[SemanticError] =
    state =>
      returnItems.items.filter(item => item.alias.isEmpty).map(i => {
        SemanticError.unaliasedReturnItem(clauseName, i.position)
      })
}

sealed trait ProjectionClause extends HorizonClause {
  def distinct: Boolean

  def returnItems: ReturnItems

  private val isAggregatingLazy: LazyVal[Boolean] =
    LazyVal(returnItems.directlyContainsAggregate || distinct || groupBy.isDefined)
  def isAggregating: Boolean = isAggregatingLazy.value

  private val hasExpandableSubclauseLazy: LazyVal[Boolean] =
    LazyVal(groupBy.isDefined || orderBy.isDefined || where.isDefined)
  def hasExpandableSubclause: Boolean = hasExpandableSubclauseLazy.value

  def groupBy: Option[GroupBy]

  def orderBy: Option[OrderBy]

  def where: Option[Where]

  def skip: Option[Skip]

  def limit: Option[Limit]

  final def isWith: Boolean = !isReturn

  def isReturn: Boolean = false

  def name: String

  def copyProjection(
    distinct: Boolean = this.distinct,
    returnItems: ReturnItems = this.returnItems,
    groupBy: Option[GroupBy] = this.groupBy,
    orderBy: Option[OrderBy] = this.orderBy,
    skip: Option[Skip] = this.skip,
    limit: Option[Limit] = this.limit,
    where: Option[Where] = this.where
  ): ProjectionClause = {
    this match {
      case w: With   => w.copy(distinct, returnItems, groupBy, orderBy, skip, limit, where)(this.position)
      case r: Return => r.copy(distinct, returnItems, groupBy, orderBy, skip, limit, r.excludedNames)(this.position)
      case y: Yield  => y.copy(returnItems, orderBy, skip, limit, where)(this.position)
    }
  }

  def withRewrittenType: ProjectionClause = {
    this match {
      case w @ With(_, _, _, _, _, _, _, _: FlavouredWithType) =>
        w.copy(withType = AddedInRewriteGeneral(Some(w.name)))(this.position)
      case w @ With(_, _, _, _, _, _, _, _) => w
      case r: Return                        => r
      case y: Yield                         => y
    }
  }

  /**
   * @return copy of this ProjectionClause with new return items
   */
  def withReturnItems(items: Seq[ReturnItem]): ProjectionClause

  override def clauseSpecificSemanticCheck: SemanticCheck =
    returnItems.semanticCheck

  override def semanticCheckContinuation(previousScope: Scope, outerScope: Option[Scope] = None): SemanticCheck =
    SemanticCheck.fromState {
      (state: SemanticState) =>
        /**
         * scopeToImportVariablesFrom will provide the scope to bring over only the variables that are needed from the
         * previous scope
         */
        def runChecks(scopeToImportVariablesFrom: Scope): SemanticCheck = {
          returnItems.declareVariables(scopeToImportVariablesFrom) chain
            groupBy.semanticCheck chain
            checkOrderBy chain
            limit.semanticCheck chain
            skip.semanticCheck chain
            where.foldSemanticCheck(checkWhere)
        }

        // The two clauses ORDER BY and WHERE, following a WITH clause where there is no DISTINCT nor aggregation, have a special scope such that they
        // can see both variables from before the WITH and variables introduced by the WITH
        // (SKIP and LIMIT clauses are not allowed to access variables anyway, so they do not need to be included in this condition even when they are standalone)
        val specialScopeForSubClausesNeeded = orderBy.isDefined || where.isDefined
        val canSeePreviousScope =
          (!(returnItems.containsAggregate || distinct || isInstanceOf[Yield])) || returnItems.includeExisting

        val check: SemanticCheck =
          if (specialScopeForSubClausesNeeded && canSeePreviousScope) {
            /*
             * We have `WITH ... WHERE` or `WITH ... ORDER BY` with no aggregation nor distinct meaning we can
             *  see things from previous scopes when we are done here
             *  (incoming-scope)
             *        |      \
             *        |     (child scope) <-  semantic checking of `ORDER BY` and `WHERE` discarded, only used for errors
             *        |
             *  (outgoing-scope)
             *        |
             *       ...
             */

            for {
              // Special scope for ORDER BY and WHERE (SKIP and LIMIT are also checked in isolated scopes)
              _ <- SemanticCheck.setState(state.newChildScope)
              checksResult <- runChecks(previousScope)
              // New sibling scope for the WITH/RETURN clause itself and onwards.
              // Re-declare projected variables in the new scope since the sub-scope is discarded
              // (We do not need to check warnOnAccessToRestrictedVariableInOrderByOrWhere here since that only applies when we have distinct or aggregation)
              returnState <- SemanticCheck.setState(checksResult.state.popScope.newSiblingScope)
              finalResult <- returnItems.declareVariables(state.currentScope.scope)
            } yield {
              SemanticCheckResult(finalResult.state, checksResult.errors ++ finalResult.errors)
            }
          } else if (specialScopeForSubClausesNeeded) {
            /*
             *  We have `WITH ... WHERE` or `WITH ... ORDER BY` with an aggregation or a distinct meaning we cannot
             *  see things from previous scopes after the aggregation (or distinct).
             *
             *  (incoming-scope)
             *         |
             *  (outgoing-scope)
             *         |      \
             *         |      (child-scope) <- semantic checking of `ORDER BY` and `WHERE` discarded only used for errors
             *        ...
             */

            // Introduce a new sibling scope first, and then a new child scope from that one
            // this child scope is used for errors only and will later be discarded.
            val siblingState = state.newSiblingScope
            val stateForSubClauses = siblingState.newChildScope

            for {
              _ <- SemanticCheck.setState(stateForSubClauses)
              checksResult <- runChecks(previousScope)
              // By popping the scope we will discard the special scope used for subclauses
              returnResult <- SemanticCheck.setState(checksResult.state.popScope)
              // Re-declare projected variables in the new scope since the sub-scope is discarded
              finalResult <-
                returnItems.declareVariables(previousScope)
            } yield {
              // Re-declare projected variables in the new scope since the sub-scope is discarded
              val niceErrors = (checksResult.errors ++ finalResult.errors).map(
                warnOnAccessToRestrictedVariableInOrderByOrWhere(state.currentScope.symbolNames)
              )
              SemanticCheckResult(finalResult.state, niceErrors)
            }
          } else {
            for {
              _ <- SemanticCheck.setState(state.newSiblingScope)
              checksResult <- runChecks(previousScope)
            } yield {
              val niceErrors = checksResult.errors.map(
                warnOnAccessToRestrictedVariableInOrderByOrWhere(state.currentScope.symbolNames)
              )
              SemanticCheckResult(checksResult.state, niceErrors)
            }
          }

        (isReturn, outerScope) match {
          case (true, Some(outer)) => check.flatMap { result =>
              val inImportingWith = this match {
                case r: Return => r.context == ImportingWithSubqueryCall
                case _         => true
              }
              when(inImportingWith, check) { (_: SemanticState) =>
                val outerScopeSymbolNames = outer.symbolNames
                val outputSymbolNames = result.state.currentScope.scope.symbolNames
                val alreadyDeclaredNames = outputSymbolNames.intersect(outerScopeSymbolNames)
                val explicitReturnVariablesByName =
                  returnItems.returnVariables.explicitVariables.map(v => v.name -> v).toMap
                val errors = alreadyDeclaredNames.map { name =>
                  val position = explicitReturnVariablesByName.getOrElse(name, returnItems).position
                  SemanticError.variableAlreadyDeclaredInOuterScope(name, position)
                }

                SemanticCheckResult(result.state, result.errors ++ errors)
              }
            }

          case _ => check
        }
    }

  /**
   * If you access a previously defined variable in a WITH/RETURN with DISTINCT or aggregation, that is not OK. Example:
   * MATCH (a) RETURN sum(a.age) ORDER BY a.name
   *
   * This method takes the "Variable not defined" errors we get from the semantic analysis and provides a more helpful
   * error message
   * @param previousScopeVars all variables defined in the previous scope.
   * @param error the error
   * @return an error with a possibly better error message
   */
  private[ast] def warnOnAccessToRestrictedVariableInOrderByOrWhere(previousScopeVars: Set[String])(
    error: SemanticErrorDef
  ): SemanticErrorDef = {
    previousScopeVars.collectFirst {
      case name if error.msg.equals(s"Variable `$name` not defined") =>
        error.withMsg(SemanticError.inaccessibleVariable(name, this.name, error.position))
    }.getOrElse(error)
  }

  private def cypher25SubclauseExpressionCheck(
    expression: Expression,
    ctx: SemanticCheckContext
  ): SemanticCheck = {
    val standardCheck = SemanticExpressionCheck.check(SemanticContext.Results, expression)
    val alias = ctx.scopeState.flatMap { scopeState =>
      scopeState.scopeOfOpt(expression).collect {
        case ExpressionScope(_, pec: ProjectionExpressionContext, _, _, _) => pec.projectionSpecification
      }.flatMap(spec =>
        spec.substituteFullExpression(expression, useLegacySubstitution = !spec.hasGroupBy, scopeState)
      ).collect { case lv: LogicalVariable => lv }
    }
    alias match {
      case Some(a) =>
        standardCheck.map(result => SemanticCheckResult(result.state, Seq.empty)) chain
          specifyType(types(a), expression)
      case None =>
        standardCheck
    }
  }

  private def checkOrderBy: SemanticCheck =
    SemanticCheck.fromContext { ctx =>
      if (ctx.cypherVersion == CypherVersion.Cypher5) orderBy.semanticCheck
      else orderBy.foldSemanticCheck(_.sortItems.foldSemanticCheck { si =>
        cypher25SubclauseExpressionCheck(si.expression, ctx) chain
          SemanticPatternCheck.checkValidPropertyKeyNames(
            si.expression.folder.findAllByClass[Property].map(_.propertyKey)
          )
      })
    }

  private def checkWhere(wh: Where): SemanticCheck =
    SemanticCheck.fromContext { ctx =>
      if (ctx.cypherVersion == CypherVersion.Cypher5)
        Where.checkExpression(wh.expression)
      else
        cypher25SubclauseExpressionCheck(wh.expression, ctx) chain
          SemanticPatternCheck.checkValidPropertyKeyNames(
            wh.expression.folder.findAllByClass[Property].map(_.propertyKey)
          ) chain
          SemanticExpressionCheck.expectType(CTBoolean.covariant, wh.expression)
    }

  def verifyOrderByAggregationUse(fail: (String, InputPosition) => Nothing): Unit = {
    val aggregationInProjection = returnItems.containsAggregate
    val aggregationInOrderBy = orderBy.exists(_.sortItems.map(_.expression).exists(containsAggregate))
    if (!aggregationInProjection && aggregationInOrderBy)
      fail(name, position)
  }
}

// used for SHOW/TERMINATE commands (and procedure calls against system)
sealed trait ClauseType

sealed trait YieldType extends ClauseType
case object DefaultYield extends YieldType
case object YieldAddedInRewrite extends YieldType

sealed trait WithType extends ClauseType
sealed trait MayBeImportingWithType
sealed trait GenericWithType extends WithType
sealed trait FlavouredWithType extends WithType
sealed trait StarNotReferencing
sealed trait OrderByOrPaginationWithType extends FlavouredWithType with StarNotReferencing
case object DefaultWith extends GenericWithType with MayBeImportingWithType
case object ParsedAsOrderBy extends OrderByOrPaginationWithType
case object ParsedAsSkip extends OrderByOrPaginationWithType
case object ParsedAsLimit extends OrderByOrPaginationWithType
case object ParsedAsFilter extends FlavouredWithType with StarNotReferencing
case object ParsedAsLet extends FlavouredWithType with StarNotReferencing
case object ParsedAsYield extends WithType with YieldType
case object AddedInRewriteShowCommands extends GenericWithType
case object AddedInRewriteProcCall extends GenericWithType
case class AddedInRewriteGeneral(name: Option[String] = None) extends GenericWithType with MayBeImportingWithType

sealed trait ReturnType extends ClauseType
case object DefaultReturn extends ReturnType
case object ReturnAddedInRewrite extends ReturnType

object With {

  def apply(returnItems: ReturnItems)(pos: InputPosition): With =
    With(distinct = false, returnItems, None, None, None, None, None)(pos)

  def apply(returnItems: ReturnItems, withType: WithType)(pos: InputPosition): With =
    With(distinct = false, returnItems, None, None, None, None, None, withType)(pos)

  def apply(returnItems: ReturnItems, where: Where, withType: WithType)(pos: InputPosition): With =
    With(distinct = false, returnItems, None, None, None, None, Some(where), withType)(pos)
}

case class With(
  distinct: Boolean,
  returnItems: ReturnItems,
  groupBy: Option[GroupBy],
  orderBy: Option[OrderBy],
  skip: Option[Skip],
  limit: Option[Limit],
  where: Option[Where],
  withType: WithType = DefaultWith
)(val position: InputPosition) extends ProjectionClause {

  override def name: String = withType match {
    case AddedInRewriteGeneral(Some(name)) => name
    case ParsedAsOrderBy                   => "ORDER BY"
    case ParsedAsSkip                      => skip.get.name
    case ParsedAsLimit                     => limit.get.name
    case ParsedAsFilter                    => "FILTER"
    case ParsedAsLet                       => "LET"
    case ParsedAsYield                     => "YIELD"
    case _                                 => "WITH"
  }

  override def clauseSpecificSemanticCheck: SemanticCheck =
    super.clauseSpecificSemanticCheck chain checkProjectionItems(returnItems)

  override def withReturnItems(items: Seq[ReturnItem]): With =
    this.copy(returnItems = ReturnItems(returnItems.projectionType, items)(returnItems.position))(this.position)

  private def checkProjectionItems(returnItems: ReturnItems): SemanticCheck =
    withType match {
      // No user-provided projection items in these variants
      case ParsedAsFilter | ParsedAsOrderBy | ParsedAsSkip | ParsedAsLimit => SemanticCheck.success
      // In LET all projection items are aliased by definition of the syntax
      case ParsedAsLet => checkLetItems(returnItems) chain
          SemanticPatternCheck.checkValidPropertyKeyNamesInReturnItems(returnItems)
      // Else
      case _ => ProjectionClause.checkAliasedReturnItems(returnItems, name) chain
          SemanticPatternCheck.checkValidPropertyKeyNamesInReturnItems(returnItems)
    }

  private def checkLetItems(returnItems: ReturnItems): SemanticCheck =
    SemanticExpressionCheck.simple(returnItems.items.map(_.expression))
}

case class Finish()(val position: InputPosition) extends Clause with ClauseAllowedOnSystem {

  override def name: String = "FINISH"

  override def clauseSpecificSemanticCheck: SemanticCheck = SemanticCheck.success
}

trait UnaliasedNotAllowed { val msg: String }

object Return {

  def apply(returnItems: ReturnItems)(pos: InputPosition): Return =
    Return(distinct = false, returnItems, None, None, None, None)(pos)

  def apply(returnItems: ReturnItems, returnType: ReturnType)(pos: InputPosition): Return =
    Return(distinct = false, returnItems, None, None, None, None, returnType = returnType)(pos)

  // Unapply for RETURN * ...
  object WithStar {

    def unapply(ret: Return): Option[Return] = {
      ret match {
        case Return(_, ReturnItems(AdditiveProjection, _, _), _, _, _, _, _, _, _) => Some(ret)
        case _                                                                     => None
      }
    }
  }
}

case class Return(
  distinct: Boolean,
  returnItems: ReturnItems,
  groupBy: Option[GroupBy],
  orderBy: Option[OrderBy],
  skip: Option[Skip],
  limit: Option[Limit],
  excludedNames: Set[String] = Set.empty,
  returnType: ReturnType = DefaultReturn, // used for SHOW/TERMINATE commands
  context: UnaliasedNotAllowed = ImportingWithSubqueryCall
)(val position: InputPosition) extends ProjectionClause with ClauseAllowedOnSystem {

  override def name = "RETURN"

  override def isReturn: Boolean = true

  override def where: Option[Where] = None

  override def returnVariables: ReturnVariables = returnItems.returnVariables

  override def clauseSpecificSemanticCheck: SemanticCheck =
    super.clauseSpecificSemanticCheck chain
      ProjectionClause.checkAliasedReturnItems(
        returnItems,
        context.msg
      ) chain
      SemanticPatternCheck.checkValidPropertyKeyNamesInReturnItems(returnItems)

  override def withReturnItems(items: Seq[ReturnItem]): Return =
    this.copy(returnItems = ReturnItems(returnItems.projectionType, items)(returnItems.position))(this.position)

  def withReturnItems(returnItems: ReturnItems): Return =
    this.copy(returnItems = returnItems)(this.position)

  def convertToWith(context: Option[String] = Some("NEXT")): With =
    With(distinct, returnItems, groupBy, orderBy, skip, limit, None, AddedInRewriteGeneral(context))(position)
}

case object Yield {

  def apply(returnItems: ReturnItems)(position: InputPosition): Yield =
    Yield(returnItems, None, None, None, None)(position)
}

case class Yield(
  returnItems: ReturnItems,
  orderBy: Option[OrderBy],
  skip: Option[Skip],
  limit: Option[Limit],
  where: Option[Where],
  yieldType: YieldType = DefaultYield
)(val position: InputPosition) extends ProjectionClause with ClauseAllowedOnSystem {
  override def distinct: Boolean = false
  override def groupBy: Option[GroupBy] = None

  override def name: String = "YIELD"

  override def withReturnItems(items: Seq[ReturnItem]): Yield =
    this.copy(returnItems = ReturnItems(returnItems.projectionType, items)(returnItems.position))(this.position)

  def withReturnItems(returnItems: ReturnItems): Yield =
    this.copy(returnItems = returnItems)(this.position)

  def withYieldType(yieldType: YieldType): Yield =
    this.copy(yieldType = yieldType)(this.position)

  override def warnOnAccessToRestrictedVariableInOrderByOrWhere(previousScopeVars: Set[String])(error: SemanticErrorDef)
    : SemanticErrorDef = error
}

object SubqueryCall {

  final case class InTransactionsBatchParameters(batchSize: Expression)(val position: InputPosition) extends ASTNode
      with SemanticCheckable {

    override def semanticCheck: SemanticCheck =
      checkExpressionIsStaticInt(batchSize, "OF ... ROWS", acceptsZero = false)
  }

  final case class InTransactionsConcurrencyParameters(concurrency: Option[Expression])(val position: InputPosition)
      extends ASTNode
      with SemanticCheckable {

    override def semanticCheck: SemanticCheck = {
      if (concurrency.isEmpty) {
        return SemanticCheck.success
      }
      checkExpressionIsStaticInt(concurrency.get, "IN ... CONCURRENT", acceptsZero = false)
    }
  }

  final case class InTransactionsReportParameters(reportAs: LogicalVariable)(val position: InputPosition)
      extends ASTNode with SemanticCheckable with SemanticAnalysisTooling {

    override def semanticCheck: SemanticCheck =
      declareVariable(reportAs, MapExtendedType.getTypeSpec(CTMap, CTString.covariant)) chain
        specifyType(MapExtendedType.getTypeSpec(CTMap, CTString.covariant), reportAs)
  }

  final case class InTransactionsErrorParameters(
    behaviour: InTransactionsOnErrorBehaviour,
    retryParameters: Option[InTransactionsRetryParameters]
  )(
    val position: InputPosition
  ) extends ASTNode

  sealed trait InTransactionsOnErrorBehaviour

  object InTransactionsOnErrorBehaviour {
    case object OnErrorContinue extends InTransactionsOnErrorBehaviour
    case object OnErrorBreak extends InTransactionsOnErrorBehaviour
    case object OnErrorFail extends InTransactionsOnErrorBehaviour
    case object OnErrorRetryThenContinue extends InTransactionsOnErrorBehaviour
    case object OnErrorRetryThenBreak extends InTransactionsOnErrorBehaviour
    case object OnErrorRetryThenFail extends InTransactionsOnErrorBehaviour

    def hasRetry(behaviour: InTransactionsOnErrorBehaviour): Boolean = behaviour match {
      case OnErrorRetryThenContinue | OnErrorRetryThenBreak | OnErrorRetryThenFail => true
      case _                                                                       => false
    }
  }

  final case class InTransactionsRetryParameters(timeout: Option[Expression])(val position: InputPosition)
      extends ASTNode
      with SemanticCheckable {

    override def semanticCheck: SemanticCheck = {
      if (timeout.isEmpty) {
        return SemanticCheck.success
      }
      checkExpressionIsStaticNumber(timeout.get, "RETRY ... SECONDS", acceptsZero = true, acceptsNegative = false)
    }
  }

  sealed trait InTransactionsDisjointByMode

  object InTransactionsDisjointByMode {
    case object DisjointByAuto extends InTransactionsDisjointByMode
    case object DisjointByNone extends InTransactionsDisjointByMode
    final case class DisjointByExpressions(expressions: Seq[Expression]) extends InTransactionsDisjointByMode
  }

  final case class InTransactionsDisjointByParameters(mode: InTransactionsDisjointByMode)(val position: InputPosition)
      extends ASTNode
      with SemanticCheckable {

    override def semanticCheck: SemanticCheck = mode match {
      case InTransactionsDisjointByMode.DisjointByExpressions(expressions) =>
        expressions.foldSemanticCheck { expression =>
          val nonDeterministic =
            if (!expression.isDeterministic) {
              error(SemanticError.disjointByExpressionNotDeterministic(expression.position))
            } else {
              SemanticCheck.success
            }
          val containsSubquery =
            if (expression.folder.treeExists { case _: SubqueryExpression => true }) {
              error(SemanticError.disjointByExpressionContainsSubquery(expression.position))
            } else {
              SemanticCheck.success
            }
          nonDeterministic chain containsSubquery
        }
      case _ => SemanticCheck.success
    }
  }

  final case class InTransactionsParameters(
    batchParams: Option[InTransactionsBatchParameters],
    concurrencyParams: Option[InTransactionsConcurrencyParameters],
    errorParams: Option[InTransactionsErrorParameters],
    reportParams: Option[InTransactionsReportParameters],
    disjointByParams: Option[InTransactionsDisjointByParameters]
  )(val position: InputPosition) extends ASTNode with SemanticCheckable {

    override def semanticCheck: SemanticCheck = {
      val checkBatchParams = batchParams.foldSemanticCheck(_.semanticCheck)
      val checkConcurrencyParams = concurrencyParams.foldSemanticCheck(_.semanticCheck)
      val checkReportParams = reportParams.foldSemanticCheck(_.semanticCheck)
      val checkRetryParams = errorParams.flatMap(_.retryParameters).foldSemanticCheck(_.semanticCheck)
      val checkDisjointByParams = disjointByParams.foldSemanticCheck(_.semanticCheck)

      val checkErrorReportCombination: SemanticCheck = (errorParams, reportParams) match {
        case (None, Some(reportParams)) =>
          error(SemanticError.invalidReportStatus(reportParams.position))
        case (Some(InTransactionsErrorParameters(OnErrorFail | OnErrorRetryThenFail, None)), Some(reportParams)) =>
          error(SemanticError.invalidReportStatus(reportParams.position))
        case _ => SemanticCheck.success
      }

      val checkDisjointByRequiresConcurrent: SemanticCheck = (disjointByParams, concurrencyParams) match {
        case (Some(bb), None) =>
          error(SemanticError.disjointByRequiresConcurrent(bb.position))
        case _ => SemanticCheck.success
      }

      checkBatchParams chain checkConcurrencyParams chain checkReportParams chain checkRetryParams chain checkDisjointByParams chain checkErrorReportCombination chain checkDisjointByRequiresConcurrent
    }
  }

  def isTransactionalSubquery(clause: SubqueryCall): Boolean = clause.inTransactionsParameters.isDefined

  def findTransactionalSubquery(node: ASTNode): Option[SubqueryCall] =
    node.folder.treeFind[SubqueryCall] { case s if isTransactionalSubquery(s) => true }
}

sealed trait SubqueryCall extends HorizonClause with SemanticAnalysisTooling {
  def innerQuery: Query
  def inTransactionsParameters: Option[SubqueryCall.InTransactionsParameters]
  def optional: Boolean

  final override def name: String = "CALL"

  final override def clauseSpecificSemanticCheck: SemanticCheck = {
    wrappedCallProcedureCheck chain
      checkSubquery(optional) chain
      inTransactionsParameters.foldSemanticCheck {
        _.semanticCheck chain
          checkNoNestedCallInTransactions
      } chain
      redundantOptionalCheck chain
      checkNoCallInTransactionsInsideRegularCall
  }

  final def reportParams: Option[SubqueryCall.InTransactionsReportParameters] =
    inTransactionsParameters.flatMap(_.reportParams)

  def isCorrelated: Boolean

  def checkSubquery(optional: Boolean): SemanticCheck

  /**
   * Semantically check the DISJOINT BY expressions against the subquery's import scope.
   *
   * DISJOINT BY expressions are evaluated in the inner scope of the CALL subquery, so they must be
   * checked in a state where only the imported variables are visible. This complements the determinism
   * and subquery-expression checks in [[SubqueryCall.InTransactionsDisjointByParameters.semanticCheck]]
   * by validating the expression internals (function resolution, arity, types, no aggregations).
   *
   * Variable-resolution errors raised here are filtered out downstream (see
   * VariableChecker.isNotImplementedCode) since the scoping pass owns them, so this does not produce
   * duplicate "variable not defined" errors.
   */
  final protected def checkDisjointByExpressions(importScope: SemanticState): SemanticCheck =
    inTransactionsParameters.flatMap(_.disjointByParams).map(_.mode) match {
      case Some(SubqueryCall.InTransactionsDisjointByMode.DisjointByExpressions(expressions)) =>
        withState(importScope)(expressions.foldSemanticCheck(SemanticExpressionCheck.simple))
      case _ => SemanticCheck.success
    }

  final protected def returnToOuterScope(outerScopeLocation: SemanticState.ScopeLocation): SemanticCheck =
    SemanticCheck.fromFunction { innerState =>
      val innerCurrentScope = innerState.currentScope.scope

      // Keep working from the latest state
      val after: SemanticState = innerState
        // but jump back to scope tree of outerStateWithImports
        .copy(currentScope = outerScopeLocation)
        // Copy in the scope tree from inner query (needed for Namespacer)
        .insertSiblingScope(innerCurrentScope)
        // Import variables from scope before subquery
        .newSiblingScope
        .importValuesFromScope(outerScopeLocation.scope)

      SemanticCheckResult.success(after)
    }

  // Used to throw the correct error message if the subquery is used to wrap an optional procedure call
  // See wrapOptionalCallProcedure.scala for more information.
  final private def wrappedCallProcedureCheck: SemanticCheck = {
    innerQuery match {
      case q: SingleQuery if optional =>
        q.clauses.head match {
          case c: UnresolvedCall =>
            for {
              state <- SemanticCheck.getState
              result <- c.wrappedOptional(state.state.currentScope)
            } yield result
          case _ => SemanticCheck.success
        }
      case _ => SemanticCheck.success
    }
  }

  final private def redundantOptionalCheck: SemanticCheck = {
    if (optional && !innerQuery.isReturning) {
      warn(RedundantOptionalSubquery(position))
    } else SemanticCheck.success
  }

  final override def semanticCheckContinuation(
    previousScope: Scope,
    outerScope: Option[Scope] = None
  ): SemanticCheck = {
    (s: SemanticState) =>
      SemanticCheckResult(s.importValuesFromScope(previousScope), Vector())
  }

  private def checkNoNestedCallInTransactions: SemanticCheck = {
    val nestedCallInTransactions = SubqueryCall.findTransactionalSubquery(innerQuery)
    nestedCallInTransactions.foldSemanticCheck { nestedCallInTransactions =>
      error(SemanticError.unsupportedNestingCIT(nestedCallInTransactions.position))
    }
  }

  private def checkNoCallInTransactionsInsideRegularCall: SemanticCheck = {
    val nestedCallInTransactions =
      if (inTransactionsParameters.isEmpty) {
        SubqueryCall.findTransactionalSubquery(innerQuery)
      } else
        None

    nestedCallInTransactions.foldSemanticCheck { nestedCallInTransactions =>
      error(SemanticError.unsupportedNestingCITInCall(nestedCallInTransactions.position))
    }
  }
}

case object ImportingWithSubqueryCall extends UnaliasedNotAllowed { override val msg = "CALL { RETURN ... }" }

case class ImportingWithSubqueryCall(
  override val innerQuery: Query,
  override val inTransactionsParameters: Option[SubqueryCall.InTransactionsParameters],
  override val optional: Boolean
)(val position: InputPosition) extends SubqueryCall {

  override def isCorrelated: Boolean = {
    innerQuery.isCorrelated
  }

  override def checkSubquery(optional: Boolean): SemanticCheck = {
    for {
      outerStateWithImports <- innerQuery.checkImportingWith(optional)
      // Create empty scope under root
      _ <- SemanticCheck.setState(outerStateWithImports.state.newBaseScope)
      // Check inner query. Allow it to import from outer scope
      innerChecked <- innerQuery.semanticCheckImportingWithSubQueryContext(outerStateWithImports.state, optional)
      _ <- returnToOuterScope(outerStateWithImports.state.currentScope)
      // Declare variables that are in output from subquery
      merged <- declareOutputVariablesInOuterScope(innerChecked.state.currentScope.scope)
    } yield {
      val importingWithErrors = outerStateWithImports.errors

      // Avoid double errors if inner has errors
      val allErrors = importingWithErrors ++
        (if (innerChecked.errors.nonEmpty) innerChecked.errors else merged.errors)

      // Keep errors from inner check and from variable declarations
      SemanticCheckResult(merged.state, allErrors)
    }
  }

  def declareOutputVariablesInOuterScope(rootScope: Scope): SemanticCheck = {
    when(innerQuery.isReturning) {
      val scopeForDeclaringVariables = innerQuery.finalScope(rootScope)
      declareVariables(scopeForDeclaringVariables.symbolTable.values)
    }
  }
}

case object ScopeClauseSubqueryCall extends UnaliasedNotAllowed {
  override val msg = "CALL () { RETURN ... }"

  def apply(inner: Query, imports: Seq[LogicalVariable])(position: InputPosition): ScopeClauseSubqueryCall =
    ScopeClauseSubqueryCall(inner, isImportingAll = false, imports, None, optional = false)(position)

}

case class ScopeClauseSubqueryCall(
  override val innerQuery: Query,
  isImportingAll: Boolean,
  importedVariables: Seq[LogicalVariable],
  override val inTransactionsParameters: Option[SubqueryCall.InTransactionsParameters],
  override val optional: Boolean,
  addedInRewriteOptionalCall: Boolean = false
)(val position: InputPosition) extends SubqueryCall {

  override def isCorrelated: Boolean = {
    isImportingAll || importedVariables.nonEmpty
  }

  override def checkSubquery(optional: Boolean): SemanticCheck = {
    for {
      // Get current state
      current <- SemanticCheck.getState
      // Checks for errors in imported variables and import into new baseScope
      innerWithImports <- importVariables
      // Check DISJOINT BY expressions against the import scope (only imported variables are visible)
      disjointByChecked <- checkDisjointByExpressions(innerWithImports.state)
      // Check inner query
      innerChecked <- innerQuery.semanticCheckInSubqueryContext(innerWithImports.state, current.state, optional)
      _ <- recordCurrentScope(this)
      // Declare output variables from inner query in outer scope
      merged <- declareOutputVariablesInOuterScope(current.state)
    } yield {
      val importingScopeErrors = (innerWithImports.errors ++ innerChecked.errors).distinct

      // Avoid double errors if inner has errors
      val allErrors =
        (if (importingScopeErrors.nonEmpty) importingScopeErrors else merged.errors) ++ disjointByChecked.errors

      // Keep errors from inner check and from variable declarations
      SemanticCheckResult(merged.state, allErrors)
    }
  }

  private def declareOutputVariablesInOuterScope(
    outer: SemanticState
  ): SemanticCheck = fromState { inner =>
    val innerScope = inner.currentScope.scope
    val innerFinalScope = innerQuery.finalScope(innerScope)
    val importedSymbolNames = innerScope.symbolNames

    returnToOuterScope(outer.currentScope) chain
      when(innerQuery.isReturning) {
        val outerScopeSymbolNames = outer.currentScope.symbolNames
        val outputSymbolNames = innerFinalScope.symbolNames
        val intersection = outputSymbolNames.intersect(outerScopeSymbolNames)
        val difference =
          if (innerQuery.returnVariables.includeExisting) intersection.diff(importedSymbolNames) else intersection

        val filteredVariables =
          innerFinalScope.symbolTable.values.filter(x => !importedSymbolNames.contains(x.name))

        innerQuery.getReturns.flatMap(v => difference.map((v, _))).foldSemanticCheck {
          case (ret, name) if ret.returnType != ReturnAddedInRewrite =>
            ret.returnItems.items.find(_.name == name) match {
              case Some(AliasedReturnItem(_, variable)) =>
                SemanticError.variableAlreadyDeclaredInOuterScope(name, variable.position)
              case None if ret.returnItems.includeExisting =>
                SemanticError.variableAlreadyDeclaredInOuterScope(name, ret.position)
              case _ => SemanticCheck.success
            }
          case _ => SemanticCheck.success
        } ifOkChain declareVariables(filteredVariables)
      }
  }

  private def importVariables: SemanticCheck =
    SemanticExpressionCheck.simple(importedVariables) chain
      fromState(previousState => {
        SemanticCheck.setState(previousState.newBaseScope).flatMap { _ =>
          val previous = previousState.currentScope
          if (isImportingAll) {
            (s: SemanticState) =>
              val intermediate =
                s.importValuesFromScope(previous.parent.get.scope).importValuesFromScope(previous.scope)
              SemanticCheckResult.success(intermediate)
          } else {
            importedVariables.foldSemanticCheck(item =>
              declareVariable(item, previousState.expressionType(item).actual, previousState.symbol(item.name))
            )
          }
        }
      })
}

// Show and terminate command clauses

sealed trait CommandClause extends Clause with SemanticAnalysisTooling {
  def unfilteredColumns: DefaultOrAllShowColumns

  def getFilteredColumns(features: Set[SemanticFeature]): Seq[LogicalVariable] =
    unfilteredColumns.columns.map(_.variable)

  // Yielded columns or yield *
  def yieldItems: List[CommandResultItem]
  def yieldAll: Boolean

  // Original columns before potential rename or filtering in YIELD
  protected def originalColumns: List[ShowAndTerminateColumn]

  // Used for semantic check
  private val columnsAsMapLazy: LazyVal[Map[String, CypherType]] =
    LazyVal(originalColumns.map(column => column.name -> column.cypherType).toMap[String, CypherType])
  protected def columnsAsMap: Map[String, CypherType] = columnsAsMapLazy.value

  override def clauseSpecificSemanticCheck: SemanticCheck =
    if (yieldItems.nonEmpty) yieldItems.foldSemanticCheck(_.semanticCheck(columnsAsMap))
    else semanticCheckFold(unfilteredColumns.columns)(sc => declareVariable(sc.variable, sc.cypherType))

  def where: Option[Where]
  def moveWhereToProjection: CommandClause

  def yieldWith: Option[With]
  def moveOutWith: CommandClause
  def getClauseWithoutSubclauses: CommandClause
}

object CommandClause {

  def unapply(cc: CommandClause): Option[(List[ShowColumn], Option[Where])] =
    Some((cc.unfilteredColumns.columns, cc.where))

  // Update variables in ORDER BY and WHERE to alias if renamed in YIELD
  // To allow YIELD x AS y ORDER BY x WHERE x = 'something' (which is allowed in WITH)
  // No need to update SKIP and LIMIT as those can only take integer values (for commands)
  // This method lives here to not need to be duplicated in Neo4jASTFactory and AstGenerator
  def updateAliasedVariablesFromYieldInOrderByAndWhere(yieldClause: Yield): (Option[OrderBy], Option[Where]) = {
    val returnAliasesMap = yieldClause.returnItems.items.map(ri => (ri.expression, ri.alias)).toMap

    def updateExpression(e: Expression): Expression = {
      returnAliasesMap.filter {
        // They should all be variables as that is what we allow parsing
        // Only need replacing if there is a differing replacement value
        // (parsing seems to add replacement even for unaliased columns (`YIELD x`))
        // (also skips replacing `YIELD x AS x`)
        // Avoid renaming variables if another variable has been renamed to the old name
        // (`YIELD x AS y, z AS x`)
        case (ov: LogicalVariable, Some(nv)) =>
          !nv.equals(ov) && !returnAliasesMap.valuesIterator.contains(Some(ov))
        case _ => false
      }.map {
        // we know the key is a LogicalVariable and that the value exists based on previous filtering
        case (key, value) => (key.asInstanceOf[LogicalVariable], value.get)
      }.foldLeft(e) {
        case (acc, (oldV, newV)) =>
          // Computed dependencies aren't available before semantic analysis
          acc.replaceAllOccurrencesBy(oldV, newV.copyId, skipExpressionsWithComputedDependencies = true)
      }
    }

    val orderBy = yieldClause.orderBy.map(ob => {
      val updatedSortItems = ob.sortItems.map(si => si.mapExpression(updateExpression))
      ob.copy(updatedSortItems)(ob.position)
    })
    val where = yieldClause.where.map(w => w.mapExpressions(updateExpression))

    (orderBy, where)
  }

  // Check if the query should automatically be routed to the system database, for backwards compatibility reasons
  def shouldRouteToSystem(clauses: Seq[Clause]): Boolean = clauses match {
    case Seq(_: CommandClauseRouteToSystem, w: With, r: Return) =>
      w.withType == ParsedAsYield || (w.withType == AddedInRewriteShowCommands && r.returnType == ReturnAddedInRewrite)
    case Seq(_: CommandClauseRouteToSystem, r: Return) =>
      r.returnType == ReturnAddedInRewrite
    case _ => false
  }
}

// Yield columns: keeps track of the original name and the yield variable (either same name or renamed)
case class CommandResultItem(originalName: String, aliasedVariable: LogicalVariable)(val position: InputPosition)
    extends ASTNode with SemanticAnalysisTooling {

  def semanticCheck(columns: Map[String, CypherType]): SemanticCheck = {

    columns
      .get(originalName)
      .map { typ => declareVariable(aliasedVariable, typ): SemanticCheck }
      .getOrElse({
        SemanticCheck.error(SemanticError.yieldMissingColumn(
          originalName,
          columns.keys.toList.sorted.asJavaCollection.stream().collect(Collectors.toList()),
          position
        ))
      })
  }

  def toReturnItem: ReturnItem = {
    AliasedReturnItem(Variable(originalName)(position, isIsolated = false), aliasedVariable)(position)
  }
}

// Column name together with the column type
// Used to create the ShowColumns but without keeping variables
// (as having undeclared variables in the ast caused issues with namespacer)
case class ShowAndTerminateColumn(name: String, cypherType: CypherType = CTString)

// Command clauses which can take strings or string expressions
// For example, transaction ids or setting names
sealed trait CommandClauseWithNames extends CommandClause {
  // The potential strings or string expressions
  def names: CommandClauseNames
  // To anonymize the name
  def withNames(names: CommandClauseNames): CommandClauseWithNames

  // Semantic check:
  private def expressionCheck: SemanticCheck =
    names.maybeExpression.map(e => SemanticExpressionCheck.simple(e))
      .getOrElse(SemanticCheck.success)

  override def clauseSpecificSemanticCheck: SemanticCheck =
    expressionCheck chain super.clauseSpecificSemanticCheck
}

// The representation of the strings or string expressions for command clauses with names
// - NoNames: no given names
// - CommaSeparatedNames: a list of strings (stored as a ListLiteral with StringLiterals)
// - ExpressionNames: a single expression (should resolve to a single string or a list of strings)
sealed trait CommandClauseNames extends ASTNode {
  def maybeExpression: Option[Expression]
}

case object NoNames extends CommandClauseNames {
  val maybeExpression: Option[Expression] = None

  // This represents nothing given so we don't really have a position
  override def position: InputPosition = InputPosition.NONE
}

case class CommaSeparatedNames(names: ListLiteral) extends CommandClauseNames {
  val maybeExpression: Option[Expression] = Some(names)

  override def position: InputPosition = names.position
}

case class ExpressionNames(names: Expression) extends CommandClauseNames {
  val maybeExpression: Option[Expression] = Some(names)

  override def position: InputPosition = names.position
}

// For a query to be allowed to run on system it needs to consist of:
// - only ClauseAllowedOnSystem clauses (or the WITH that was parsed as YIELD/added in rewriter for commands)
// - at least one CommandClauseAllowedOnSystem clause
sealed trait ClauseAllowedOnSystem
sealed trait CommandClauseAllowedOnSystem extends ClauseAllowedOnSystem
sealed trait CommandClauseRouteToSystem extends CommandClauseAllowedOnSystem

case class ShowIndexesClause(
  briefIndexColumns: List[ShowAndTerminateColumn],
  allIndexColumns: List[ShowAndTerminateColumn],
  indexType: ShowIndexType,
  where: Option[Where],
  yieldItems: List[CommandResultItem],
  yieldAll: Boolean,
  yieldWith: Option[With]
)(val position: InputPosition) extends CommandClause {
  override def name: String = "SHOW INDEXES"

  private val useAllColumns = yieldItems.nonEmpty || yieldAll

  val originalColumns: List[ShowAndTerminateColumn] =
    if (useAllColumns) allIndexColumns else briefIndexColumns

  private val briefColumns = briefIndexColumns.map(c => ShowColumn(c.name, c.cypherType)(position))
  private val allColumns = allIndexColumns.map(c => ShowColumn(c.name, c.cypherType)(position))

  val unfilteredColumns: DefaultOrAllShowColumns =
    DefaultOrAllShowColumns(useAllColumns, briefColumns, allColumns)

  override def moveWhereToProjection: CommandClause = copy(where = None)(position)
  override def moveOutWith: CommandClause = copy(yieldWith = None)(position)

  override def getClauseWithoutSubclauses: CommandClause =
    copy(where = None, yieldItems = List.empty, yieldWith = None)(InputPosition.NONE)
}

object ShowIndexesClause {
  val idColumn = "id"
  val nameColumn = "name"
  val stateColumn = "state"
  val populationPercentColumn = "populationPercent"
  val typeColumn = "type"
  val entityTypeColumn = "entityType"
  val labelsOrTypesColumn = "labelsOrTypes"
  val propertiesColumn = "properties"
  val indexProviderColumn = "indexProvider"
  val owningConstraintColumn = "owningConstraint"
  val lastReadColumn = "lastRead"
  val readCountColumn = "readCount"
  val trackedSinceColumn = "trackedSince"
  val optionsColumn = "options"
  val failureMessageColumn = "failureMessage"
  val createStatementColumn = "createStatement"

  def apply(
    indexType: ShowIndexType,
    where: Option[Where],
    yieldItems: List[CommandResultItem],
    yieldAll: Boolean,
    yieldWith: Option[With]
  )(position: InputPosition): ShowIndexesClause = {
    val briefCols = List(
      ShowAndTerminateColumn(idColumn, CTInteger),
      ShowAndTerminateColumn(nameColumn),
      ShowAndTerminateColumn(stateColumn),
      ShowAndTerminateColumn(populationPercentColumn, CTFloat),
      ShowAndTerminateColumn(typeColumn),
      ShowAndTerminateColumn(entityTypeColumn),
      ShowAndTerminateColumn(labelsOrTypesColumn, CTList(CTString)),
      ShowAndTerminateColumn(propertiesColumn, CTList(CTString)),
      ShowAndTerminateColumn(indexProviderColumn),
      ShowAndTerminateColumn(owningConstraintColumn),
      ShowAndTerminateColumn(lastReadColumn, CTDateTime),
      ShowAndTerminateColumn(readCountColumn, CTInteger)
    )
    val verboseCols = List(
      ShowAndTerminateColumn(trackedSinceColumn, CTDateTime),
      ShowAndTerminateColumn(optionsColumn, CTMap),
      ShowAndTerminateColumn(failureMessageColumn),
      ShowAndTerminateColumn(createStatementColumn)
    )

    ShowIndexesClause(
      briefCols,
      briefCols ++ verboseCols,
      indexType,
      where,
      yieldItems,
      yieldAll,
      yieldWith
    )(position)
  }
}

case class ShowConstraintsClause(
  briefConstraintColumns: List[ShowAndTerminateColumn],
  allConstraintColumns: List[ShowAndTerminateColumn],
  constraintType: ShowConstraintType,
  where: Option[Where],
  yieldItems: List[CommandResultItem],
  yieldAll: Boolean,
  yieldWith: Option[With]
)(val position: InputPosition) extends CommandClause {
  override def name: String = "SHOW CONSTRAINTS"

  private val useAllColumns = yieldItems.nonEmpty || yieldAll

  val originalColumns: List[ShowAndTerminateColumn] =
    if (useAllColumns) allConstraintColumns else briefConstraintColumns

  private val briefColumns = briefConstraintColumns.map(c => ShowColumn(c.name, c.cypherType)(position))
  private val allColumns = allConstraintColumns.map(c => ShowColumn(c.name, c.cypherType)(position))

  val unfilteredColumns: DefaultOrAllShowColumns =
    DefaultOrAllShowColumns(useAllColumns, briefColumns, allColumns)

  override def moveWhereToProjection: CommandClause = copy(where = None)(position)
  override def moveOutWith: CommandClause = copy(yieldWith = None)(position)

  override def getClauseWithoutSubclauses: CommandClause =
    copy(where = None, yieldItems = List.empty, yieldWith = None)(InputPosition.NONE)

  override def getFilteredColumns(features: Set[SemanticFeature]): Seq[LogicalVariable] =
    if (!features(SemanticFeature.GraphTypes)) {
      def filterOutGraphTypeColumns(name: String) =
        Seq(ShowConstraintsClause.enforcedLabelColumn, ShowConstraintsClause.classificationColumn).contains(name)

      val filteredUnfilteredColumns =
        unfilteredColumns.columns.filterNot { (s: ShowColumn) => filterOutGraphTypeColumns(s.name) }

      filteredUnfilteredColumns.map(_.variable)
    } else {
      unfilteredColumns.columns.map(_.variable)
    }

  // Don't want to declare the graph type columns without the feature flag enabled
  override def clauseSpecificSemanticCheck: SemanticCheck = fromState { s =>
    val (updatedColumnsAsMap, updatedUnfilteredColumns) = if (!s.features(SemanticFeature.GraphTypes)) {
      def filterOutGraphTypeColumns(name: String) =
        Seq(ShowConstraintsClause.enforcedLabelColumn, ShowConstraintsClause.classificationColumn).contains(name)

      val filteredColumnsAsMap = columnsAsMap.filterNot { case (name, _) => filterOutGraphTypeColumns(name) }
      val filteredUnfilteredColumns =
        unfilteredColumns.columns.filterNot { (s: ShowColumn) => filterOutGraphTypeColumns(s.name) }

      (filteredColumnsAsMap, filteredUnfilteredColumns)
    } else {
      (columnsAsMap, unfilteredColumns.columns)
    }

    // This is the same things `super.clauseSpecificSemanticCheck` does (with the values of the else case directly)
    if (yieldItems.nonEmpty) yieldItems.foldSemanticCheck(_.semanticCheck(updatedColumnsAsMap))
    else semanticCheckFold(updatedUnfilteredColumns)(sc => declareVariable(sc.variable, sc.cypherType))
  }
}

object ShowConstraintsClause {
  val idColumn = "id"
  val nameColumn = "name"
  val typeColumn = "type"
  val entityTypeColumn = "entityType"
  val labelsOrTypesColumn = "labelsOrTypes"
  val propertiesColumn = "properties"
  val enforcedLabelColumn = "enforcedLabel"
  val classificationColumn = "classification"
  val ownedIndexColumn = "ownedIndex"
  val propertyTypeColumn = "propertyType"
  val optionsColumn = "options"
  val createStatementColumn = "createStatement"

  def apply(
    constraintType: ShowConstraintType,
    where: Option[Where],
    yieldItems: List[CommandResultItem],
    yieldAll: Boolean,
    yieldWith: Option[With],
    returnCypher5Columns: Boolean
  )(position: InputPosition): ShowConstraintsClause = {
    val columns = List(
      // (column, brief, availableInCypher5)
      (ShowAndTerminateColumn(idColumn, CTInteger), true, true),
      (ShowAndTerminateColumn(nameColumn), true, true),
      (ShowAndTerminateColumn(typeColumn), true, true),
      (ShowAndTerminateColumn(entityTypeColumn), true, true),
      (ShowAndTerminateColumn(labelsOrTypesColumn, CTList(CTString)), true, true),
      (ShowAndTerminateColumn(propertiesColumn, CTList(CTString)), true, true),
      (ShowAndTerminateColumn(enforcedLabelColumn), true, false),
      (ShowAndTerminateColumn(classificationColumn), false, false),
      (ShowAndTerminateColumn(ownedIndexColumn), true, true),
      (ShowAndTerminateColumn(propertyTypeColumn), true, true),
      (ShowAndTerminateColumn(optionsColumn, CTMap), false, true),
      (ShowAndTerminateColumn(createStatementColumn), false, true)
    )
    val briefColumns =
      columns.filter { case (_, brief, _) => brief }
        .filter { case (_, _, availableInCypher5) => !returnCypher5Columns || availableInCypher5 }
        .map { case (column, _, _) => column }
    val allColumns =
      columns.filter { case (_, _, availableInCypher5) => !returnCypher5Columns || availableInCypher5 }
        .map { case (column, _, _) => column }

    ShowConstraintsClause(
      briefColumns,
      allColumns,
      constraintType,
      where,
      yieldItems,
      yieldAll,
      yieldWith
    )(position)
  }
}

case class ShowCurrentGraphTypeClause(
  originalColumns: List[ShowAndTerminateColumn],
  asGraph: Boolean,
  where: Option[Where],
  yieldItems: List[CommandResultItem],
  yieldAll: Boolean,
  yieldWith: Option[With]
)(val position: InputPosition) extends CommandClause {
  override def name: String = if (asGraph) "SHOW CURRENT GRAPH TYPE AS GRAPH" else "SHOW CURRENT GRAPH TYPE"

  private val columns = originalColumns.map(c => ShowColumn(c.name, c.cypherType)(position))

  val unfilteredColumns: DefaultOrAllShowColumns =
    DefaultOrAllShowColumns(useAllColumns = yieldItems.nonEmpty || yieldAll, columns, columns)

  override def moveWhereToProjection: CommandClause = copy(where = None)(position)
  override def moveOutWith: CommandClause = copy(yieldWith = None)(position)

  override def getClauseWithoutSubclauses: CommandClause =
    copy(where = None, yieldItems = List.empty, yieldWith = None)(InputPosition.NONE)

  override def clauseSpecificSemanticCheck: SemanticCheck =
    requireFeatureSupport("`SHOW CURRENT GRAPH TYPE`", SemanticFeature.GraphTypes, position) chain
      super.clauseSpecificSemanticCheck
}

object ShowCurrentGraphTypeClause {
  val specificationColumn = "specification"
  val nodesColumn = "nodes"
  val relationshipsColumn = "relationships"

  def apply(
    asGraph: Boolean,
    where: Option[Where],
    yieldItems: List[CommandResultItem],
    yieldAll: Boolean,
    yieldWith: Option[With]
  )(position: InputPosition): ShowCurrentGraphTypeClause = {

    val columns =
      if (asGraph) List(
        ShowAndTerminateColumn(nodesColumn, CTList(CTNode)),
        ShowAndTerminateColumn(relationshipsColumn, CTList(CTRelationship))
      )
      else List(ShowAndTerminateColumn(specificationColumn))

    ShowCurrentGraphTypeClause(columns, asGraph, where, yieldItems, yieldAll, yieldWith)(position)
  }
}

case class ShowProceduresClause(
  briefProcedureColumns: List[ShowAndTerminateColumn],
  allProcedureColumns: List[ShowAndTerminateColumn],
  executable: Option[ExecutableBy],
  where: Option[Where],
  yieldItems: List[CommandResultItem],
  yieldAll: Boolean,
  yieldWith: Option[With]
)(val position: InputPosition) extends CommandClause with CommandClauseAllowedOnSystem {
  override def name: String = "SHOW PROCEDURES"

  private val useAllColumns = yieldItems.nonEmpty || yieldAll

  val originalColumns: List[ShowAndTerminateColumn] =
    if (useAllColumns) allProcedureColumns else briefProcedureColumns

  private val briefColumns = briefProcedureColumns.map(c => ShowColumn(c.name, c.cypherType)(position))
  private val allColumns = allProcedureColumns.map(c => ShowColumn(c.name, c.cypherType)(position))

  val unfilteredColumns: DefaultOrAllShowColumns =
    DefaultOrAllShowColumns(useAllColumns, briefColumns, allColumns)

  override def moveWhereToProjection: CommandClause = copy(where = None)(position)
  override def moveOutWith: CommandClause = copy(yieldWith = None)(position)

  override def getClauseWithoutSubclauses: CommandClause =
    copy(where = None, yieldItems = List.empty, yieldWith = None)(InputPosition.NONE)

  override def clauseSpecificSemanticCheck: SemanticCheck =
    executable.semanticCheck chain super.clauseSpecificSemanticCheck
}

object ShowProceduresClause {
  val nameColumn = "name"
  val descriptionColumn = "description"
  val modeColumn = "mode"
  val worksOnSystemColumn = "worksOnSystem"
  val signatureColumn = "signature"
  val argumentDescriptionColumn = "argumentDescription"
  val returnDescriptionColumn = "returnDescription"
  val adminColumn = "admin"
  val rolesExecutionColumn = "rolesExecution"
  val rolesBoostedExecutionColumn = "rolesBoostedExecution"
  val isDeprecatedColumn = "isDeprecated"
  val deprecatedByColumn = "deprecatedBy"
  val optionColumn = "option"

  def apply(
    executable: Option[ExecutableBy],
    where: Option[Where],
    yieldItems: List[CommandResultItem],
    yieldAll: Boolean,
    yieldWith: Option[With]
  )(position: InputPosition): ShowProceduresClause = {
    val briefCols = List(
      ShowAndTerminateColumn(nameColumn),
      ShowAndTerminateColumn(descriptionColumn),
      ShowAndTerminateColumn(modeColumn),
      ShowAndTerminateColumn(worksOnSystemColumn, CTBoolean)
    )
    val verboseCols = List(
      ShowAndTerminateColumn(signatureColumn),
      ShowAndTerminateColumn(argumentDescriptionColumn, CTList(CTMap)),
      ShowAndTerminateColumn(returnDescriptionColumn, CTList(CTMap)),
      ShowAndTerminateColumn(adminColumn, CTBoolean),
      ShowAndTerminateColumn(rolesExecutionColumn, CTList(CTString)),
      ShowAndTerminateColumn(rolesBoostedExecutionColumn, CTList(CTString)),
      ShowAndTerminateColumn(isDeprecatedColumn, CTBoolean),
      ShowAndTerminateColumn(deprecatedByColumn),
      ShowAndTerminateColumn(optionColumn, CTMap)
    )

    ShowProceduresClause(
      briefCols,
      briefCols ++ verboseCols,
      executable,
      where,
      yieldItems,
      yieldAll,
      yieldWith
    )(position)
  }
}

case class ShowFunctionsClause(
  briefFunctionColumns: List[ShowAndTerminateColumn],
  allFunctionColumns: List[ShowAndTerminateColumn],
  functionType: ShowFunctionType,
  executable: Option[ExecutableBy],
  where: Option[Where],
  yieldItems: List[CommandResultItem],
  yieldAll: Boolean,
  yieldWith: Option[With]
)(val position: InputPosition) extends CommandClause with CommandClauseAllowedOnSystem {
  override def name: String = "SHOW FUNCTIONS"

  private val useAllColumns = yieldItems.nonEmpty || yieldAll

  val originalColumns: List[ShowAndTerminateColumn] =
    if (useAllColumns) allFunctionColumns else briefFunctionColumns

  private val briefColumns = briefFunctionColumns.map(c => ShowColumn(c.name, c.cypherType)(position))
  private val allColumns = allFunctionColumns.map(c => ShowColumn(c.name, c.cypherType)(position))

  val unfilteredColumns: DefaultOrAllShowColumns =
    DefaultOrAllShowColumns(useAllColumns, briefColumns, allColumns)

  override def moveWhereToProjection: CommandClause = copy(where = None)(position)
  override def moveOutWith: CommandClause = copy(yieldWith = None)(position)

  override def getClauseWithoutSubclauses: CommandClause =
    copy(where = None, yieldItems = List.empty, yieldWith = None)(InputPosition.NONE)

  override def clauseSpecificSemanticCheck: SemanticCheck =
    executable.semanticCheck chain super.clauseSpecificSemanticCheck
}

object ShowFunctionsClause {
  val nameColumn = "name"
  val categoryColumn = "category"
  val descriptionColumn = "description"
  val signatureColumn = "signature"
  val isBuiltInColumn = "isBuiltIn"
  val argumentDescriptionColumn = "argumentDescription"
  val returnDescriptionColumn = "returnDescription"
  val aggregatingColumn = "aggregating"
  val rolesExecutionColumn = "rolesExecution"
  val rolesBoostedExecutionColumn = "rolesBoostedExecution"
  val isDeprecatedColumn = "isDeprecated"
  val deprecatedByColumn = "deprecatedBy"

  def apply(
    functionType: ShowFunctionType,
    executable: Option[ExecutableBy],
    where: Option[Where],
    yieldItems: List[CommandResultItem],
    yieldAll: Boolean,
    yieldWith: Option[With]
  )(position: InputPosition): ShowFunctionsClause = {
    val briefCols = List(
      ShowAndTerminateColumn(nameColumn),
      ShowAndTerminateColumn(categoryColumn),
      ShowAndTerminateColumn(descriptionColumn)
    )
    val verboseCols = List(
      ShowAndTerminateColumn(signatureColumn),
      ShowAndTerminateColumn(isBuiltInColumn, CTBoolean),
      ShowAndTerminateColumn(argumentDescriptionColumn, CTList(CTMap)),
      ShowAndTerminateColumn(returnDescriptionColumn),
      ShowAndTerminateColumn(aggregatingColumn, CTBoolean),
      ShowAndTerminateColumn(rolesExecutionColumn, CTList(CTString)),
      ShowAndTerminateColumn(rolesBoostedExecutionColumn, CTList(CTString)),
      ShowAndTerminateColumn(isDeprecatedColumn, CTBoolean),
      ShowAndTerminateColumn(deprecatedByColumn)
    )

    ShowFunctionsClause(
      briefCols,
      briefCols ++ verboseCols,
      functionType,
      executable,
      where,
      yieldItems,
      yieldAll,
      yieldWith
    )(position)
  }
}

sealed trait TransactionsCommandClause extends CommandClauseWithNames with CommandClauseAllowedOnSystem

case class ShowTransactionsClause(
  briefTransactionColumns: List[ShowAndTerminateColumn],
  allTransactionColumns: List[ShowAndTerminateColumn],
  names: CommandClauseNames,
  where: Option[Where],
  yieldItems: List[CommandResultItem],
  yieldAll: Boolean,
  yieldWith: Option[With]
)(val position: InputPosition) extends TransactionsCommandClause {

  override def name: String = "SHOW TRANSACTIONS"
  def withNames(names: CommandClauseNames): ShowTransactionsClause = copy(names = names)(position)

  private val useAllColumns = yieldItems.nonEmpty || yieldAll

  val originalColumns: List[ShowAndTerminateColumn] =
    if (useAllColumns) allTransactionColumns else briefTransactionColumns

  private val briefColumns = briefTransactionColumns.map(c => ShowColumn(c.name, c.cypherType)(position))
  private val allColumns = allTransactionColumns.map(c => ShowColumn(c.name, c.cypherType)(position))

  val unfilteredColumns: DefaultOrAllShowColumns =
    DefaultOrAllShowColumns(useAllColumns, briefColumns, allColumns)

  override def moveWhereToProjection: CommandClause = copy(where = None)(position)
  override def moveOutWith: CommandClause = copy(yieldWith = None)(position)

  override def getClauseWithoutSubclauses: CommandClause =
    copy(where = None, yieldItems = List.empty, yieldWith = None)(InputPosition.NONE)
}

object ShowTransactionsClause {
  val databaseColumn = "database"
  val transactionIdColumn = "transactionId"
  val currentQueryIdColumn = "currentQueryId"
  val outerTransactionIdColumn = "outerTransactionId"
  val connectionIdColumn = "connectionId"
  val clientAddressColumn = "clientAddress"
  val usernameColumn = "username"
  val metaDataColumn = "metaData"
  val currentQueryColumn = "currentQuery"
  val parametersColumn = "parameters"
  val plannerColumn = "planner"
  val runtimeColumn = "runtime"
  val indexesColumn = "indexes"
  val startTimeColumn = "startTime"
  val currentQueryStartTimeColumn = "currentQueryStartTime"
  val protocolColumn = "protocol"
  val requestUriColumn = "requestUri"
  val statusColumn = "status"
  val currentQueryStatusColumn = "currentQueryStatus"
  val statusDetailsColumn = "statusDetails"
  val resourceInformationColumn = "resourceInformation"
  val activeLockCountColumn = "activeLockCount"
  val currentQueryActiveLockCountColumn = "currentQueryActiveLockCount"
  val elapsedTimeColumn = "elapsedTime"
  val cpuTimeColumn = "cpuTime"
  val waitTimeColumn = "waitTime"
  val idleTimeColumn = "idleTime"
  val currentQueryElapsedTimeColumn = "currentQueryElapsedTime"
  val currentQueryCpuTimeColumn = "currentQueryCpuTime"
  val currentQueryWaitTimeColumn = "currentQueryWaitTime"
  val currentQueryIdleTimeColumn = "currentQueryIdleTime"
  val currentQueryAllocatedBytesColumn = "currentQueryAllocatedBytes"
  val allocatedDirectBytesColumn = "allocatedDirectBytes"
  val estimatedUsedHeapMemoryColumn = "estimatedUsedHeapMemory"
  val pageHitsColumn = "pageHits"
  val pageFaultsColumn = "pageFaults"
  val currentQueryPageHitsColumn = "currentQueryPageHits"
  val currentQueryPageFaultsColumn = "currentQueryPageFaults"
  val initializationStackTraceColumn = "initializationStackTrace"
  val currentQueryProgressColumn = "currentQueryProgress"

  def apply(
    ids: CommandClauseNames,
    where: Option[Where],
    yieldItems: List[CommandResultItem],
    yieldAll: Boolean,
    yieldWith: Option[With],
    returnCypher5Types: Boolean
  )(position: InputPosition): ShowTransactionsClause = {
    val columns = List(
      // (column, brief, includedInCypher5)
      (ShowAndTerminateColumn(databaseColumn), true, true),
      (ShowAndTerminateColumn(transactionIdColumn), true, true),
      (ShowAndTerminateColumn(currentQueryIdColumn), true, true),
      (ShowAndTerminateColumn(outerTransactionIdColumn), false, true),
      (ShowAndTerminateColumn(connectionIdColumn), true, true),
      (ShowAndTerminateColumn(clientAddressColumn), true, true),
      (ShowAndTerminateColumn(usernameColumn), true, true),
      (ShowAndTerminateColumn(metaDataColumn, CTMap), false, true),
      (ShowAndTerminateColumn(currentQueryColumn), true, true),
      (ShowAndTerminateColumn(parametersColumn, CTMap), false, true),
      (ShowAndTerminateColumn(plannerColumn), false, true),
      (ShowAndTerminateColumn(runtimeColumn), false, true),
      (ShowAndTerminateColumn(indexesColumn, CTList(CTMap)), false, true),
      (ShowAndTerminateColumn(startTimeColumn, if (returnCypher5Types) CTString else CTDateTime), true, true),
      (
        ShowAndTerminateColumn(currentQueryStartTimeColumn, if (returnCypher5Types) CTString else CTDateTime),
        false,
        true
      ),
      (ShowAndTerminateColumn(protocolColumn), false, true),
      (ShowAndTerminateColumn(requestUriColumn), false, true),
      (ShowAndTerminateColumn(statusColumn), true, true),
      (ShowAndTerminateColumn(currentQueryStatusColumn), false, true),
      (ShowAndTerminateColumn(statusDetailsColumn), false, true),
      (ShowAndTerminateColumn(resourceInformationColumn, CTMap), false, true),
      (ShowAndTerminateColumn(activeLockCountColumn, CTInteger), false, true),
      (ShowAndTerminateColumn(currentQueryActiveLockCountColumn, CTInteger), false, true),
      (ShowAndTerminateColumn(elapsedTimeColumn, CTDuration), true, true),
      (ShowAndTerminateColumn(cpuTimeColumn, CTDuration), false, true),
      (ShowAndTerminateColumn(waitTimeColumn, CTDuration), false, true),
      (ShowAndTerminateColumn(idleTimeColumn, CTDuration), false, true),
      (ShowAndTerminateColumn(currentQueryElapsedTimeColumn, CTDuration), false, true),
      (ShowAndTerminateColumn(currentQueryCpuTimeColumn, CTDuration), false, true),
      (ShowAndTerminateColumn(currentQueryWaitTimeColumn, CTDuration), false, true),
      (ShowAndTerminateColumn(currentQueryIdleTimeColumn, CTDuration), false, true),
      (ShowAndTerminateColumn(currentQueryAllocatedBytesColumn, CTInteger), false, true),
      (ShowAndTerminateColumn(allocatedDirectBytesColumn, CTInteger), false, true),
      (ShowAndTerminateColumn(estimatedUsedHeapMemoryColumn, CTInteger), false, true),
      (ShowAndTerminateColumn(pageHitsColumn, CTInteger), false, true),
      (ShowAndTerminateColumn(pageFaultsColumn, CTInteger), false, true),
      (ShowAndTerminateColumn(currentQueryPageHitsColumn, CTInteger), false, true),
      (ShowAndTerminateColumn(currentQueryPageFaultsColumn, CTInteger), false, true),
      (ShowAndTerminateColumn(initializationStackTraceColumn), false, true),
      (ShowAndTerminateColumn(currentQueryProgressColumn), false, false)
    )
    val showColumns = columns.filter { case (_, _, includedInCypher5) =>
      // rename or create separate parameter
      !returnCypher5Types || includedInCypher5
    }.map { case (showColumn, brief, _) => (showColumn, brief) }
    val briefColumns = showColumns.filter { case (_, brief) => brief }.map { case (column, _) => column }
    val allColumns = showColumns.map { case (column, _) => column }

    ShowTransactionsClause(
      briefColumns,
      allColumns,
      ids,
      where,
      yieldItems,
      yieldAll,
      yieldWith
    )(position)
  }
}

case class TerminateTransactionsClause(
  originalColumns: List[ShowAndTerminateColumn],
  names: CommandClauseNames,
  yieldItems: List[CommandResultItem],
  yieldAll: Boolean,
  yieldWith: Option[With],
  wherePos: Option[InputPosition]
)(val position: InputPosition) extends TransactionsCommandClause {

  override def name: String = "TERMINATE TRANSACTIONS"
  def withNames(names: CommandClauseNames): TerminateTransactionsClause = copy(names = names)(position)

  private val columns = originalColumns.map(c => ShowColumn(c.name, c.cypherType)(position))

  val unfilteredColumns: DefaultOrAllShowColumns =
    DefaultOrAllShowColumns(useAllColumns = yieldItems.nonEmpty || yieldAll, columns, columns)

  override def clauseSpecificSemanticCheck: SemanticCheck = when(wherePos.isDefined) {
    val gql = ErrorGqlStatusObjectImplementation.from(GqlStatusInfoCodes.STATUS_42N84)
      .atPosition(wherePos.get.offset, wherePos.get.line, wherePos.get.column)
      .build()
    error(
      gql,
      "`WHERE` is not allowed by itself, please use `TERMINATE TRANSACTION ... YIELD ... WHERE ...` instead",
      wherePos.get
    )
  } chain super.clauseSpecificSemanticCheck

  override def where: Option[Where] = None
  override def moveWhereToProjection: CommandClause = this

  override def moveOutWith: CommandClause = copy(yieldWith = None)(position)

  override def getClauseWithoutSubclauses: CommandClause =
    copy(yieldItems = List.empty, yieldWith = None)(InputPosition.NONE)
}

object TerminateTransactionsClause {
  val transactionIdColumn = "transactionId"
  val usernameColumn = "username"
  val messageColumn = "message"

  def apply(
    ids: CommandClauseNames,
    yieldItems: List[CommandResultItem],
    yieldAll: Boolean,
    yieldWith: Option[With],
    wherePos: Option[InputPosition]
  )(position: InputPosition): TerminateTransactionsClause = {
    // All columns are currently default
    val columns = List(
      ShowAndTerminateColumn(transactionIdColumn),
      ShowAndTerminateColumn(usernameColumn),
      ShowAndTerminateColumn(messageColumn)
    )

    TerminateTransactionsClause(
      columns,
      ids,
      yieldItems,
      yieldAll,
      yieldWith,
      wherePos
    )(position)
  }
}

case class ShowSettingsClause(
  briefSettingColumns: List[ShowAndTerminateColumn],
  allSettingColumns: List[ShowAndTerminateColumn],
  names: CommandClauseNames,
  where: Option[Where],
  yieldItems: List[CommandResultItem],
  yieldAll: Boolean,
  yieldWith: Option[With]
)(val position: InputPosition) extends CommandClauseWithNames with CommandClauseAllowedOnSystem {

  override def name: String = "SHOW SETTINGS"
  def withNames(names: CommandClauseNames): ShowSettingsClause = copy(names = names)(position)

  private val useAllColumns = yieldItems.nonEmpty || yieldAll

  val originalColumns: List[ShowAndTerminateColumn] =
    if (useAllColumns) allSettingColumns else briefSettingColumns

  private val briefColumns = briefSettingColumns.map(c => ShowColumn(c.name, c.cypherType)(position))
  private val allColumns = allSettingColumns.map(c => ShowColumn(c.name, c.cypherType)(position))

  val unfilteredColumns: DefaultOrAllShowColumns =
    DefaultOrAllShowColumns(useAllColumns, briefColumns, allColumns)

  override def moveWhereToProjection: CommandClause = copy(where = None)(position)
  override def moveOutWith: CommandClause = copy(yieldWith = None)(position)

  override def getClauseWithoutSubclauses: CommandClause =
    copy(where = None, yieldItems = List.empty, yieldWith = None)(InputPosition.NONE)

  override def clauseSpecificSemanticCheck: SemanticCheck = {
    requireFeatureSupport(
      s"The `$name` clause",
      SemanticFeature.ShowSetting,
      position
    ) chain super.clauseSpecificSemanticCheck
  }
}

object ShowSettingsClause {
  val nameColumn = "name"
  val valueColumn = "value"
  val isDynamicColumn = "isDynamic"
  val defaultValueColumn = "defaultValue"
  val descriptionColumn = "description"
  val startupValueColumn = "startupValue"
  val isExplicitlySetColumn = "isExplicitlySet"
  val validValuesColumn = "validValues"
  val isDeprecatedColumn = "isDeprecated"

  def apply(
    names: CommandClauseNames,
    where: Option[Where],
    yieldItems: List[CommandResultItem],
    yieldAll: Boolean,
    yieldWith: Option[With]
  )(position: InputPosition): ShowSettingsClause = {
    val defaultCols = List(
      ShowAndTerminateColumn(nameColumn),
      ShowAndTerminateColumn(valueColumn),
      ShowAndTerminateColumn(isDynamicColumn, CTBoolean),
      ShowAndTerminateColumn(defaultValueColumn),
      ShowAndTerminateColumn(descriptionColumn)
    )
    val verboseCols = List(
      ShowAndTerminateColumn(startupValueColumn),
      ShowAndTerminateColumn(isExplicitlySetColumn, CTBoolean),
      ShowAndTerminateColumn(validValuesColumn),
      ShowAndTerminateColumn(isDeprecatedColumn, CTBoolean)
    )

    ShowSettingsClause(
      defaultCols,
      defaultCols ++ verboseCols,
      names,
      where,
      yieldItems,
      yieldAll,
      yieldWith
    )(position)
  }
}

// System graph only commands

case class ShowDatabasesClause(
  dbScope: DatabaseScope,
  briefDatabaseColumns: List[ShowAndTerminateColumn],
  allDatabaseColumns: List[ShowAndTerminateColumn],
  where: Option[Where],
  yieldItems: List[CommandResultItem],
  yieldAll: Boolean,
  yieldWith: Option[With]
)(val position: InputPosition) extends CommandClause with CommandClauseRouteToSystem {

  override def name: String = dbScope match {
    case _: SingleNamedDatabaseScope                   => "SHOW DATABASE"
    case _: AllDatabasesScope | _: NamedDatabasesScope => "SHOW DATABASES"
    case _: DefaultDatabaseScope                       => "SHOW DEFAULT DATABASE"
    case _: HomeDatabaseScope                          => "SHOW HOME DATABASE"
  }

  private val useAllColumns = yieldItems.nonEmpty || yieldAll

  val originalColumns: List[ShowAndTerminateColumn] =
    if (useAllColumns) allDatabaseColumns else briefDatabaseColumns

  private val briefColumns = briefDatabaseColumns.map(c => ShowColumn(c.name, c.cypherType)(position))
  private val allColumns = allDatabaseColumns.map(c => ShowColumn(c.name, c.cypherType)(position))

  val unfilteredColumns: DefaultOrAllShowColumns =
    DefaultOrAllShowColumns(useAllColumns, briefColumns, allColumns)

  override def moveWhereToProjection: CommandClause = copy(where = None)(position)
  override def moveOutWith: CommandClause = copy(yieldWith = None)(position)

  override def getClauseWithoutSubclauses: CommandClause =
    copy(where = None, yieldItems = List.empty, yieldWith = None)(position)

  // Semantic check needs to be split to handle typing
  override def clauseSpecificSemanticCheck: SemanticCheck = checkDbName() chain super.clauseSpecificSemanticCheck

  private def checkDbName(): SemanticState => Either[SemanticError, SemanticState] = (s: SemanticState) => {
    dbScope match {
      case SingleNamedDatabaseScope(dbName: NamespacedName) =>
        s.symbol(dbName.toString) match {
          case None => Right(s)
          case Some(_) =>
            Right(s.addNotification(IdentifierShadowsVariableNotification(position, dbName.toString, name)))
        }
      case _ => Right(s)
    }
  }
}

object ShowDatabasesClause {

  def apply(
    dbScope: DatabaseScope,
    where: Option[Where],
    yieldItems: List[CommandResultItem],
    yieldAll: Boolean,
    yieldWith: Option[With],
    cypher5ColumnsOnly: Boolean
  )(position: InputPosition): ShowDatabasesClause = {

    // (column, brief, showCypher5)
    val cols: List[(ShowAndTerminateColumn, Boolean, Boolean)] = List(
      (ShowAndTerminateColumn(NAME_COL), true, true),
      (ShowAndTerminateColumn(TYPE_COL), true, true),
      (ShowAndTerminateColumn(ALIASES_COL, CTList(CTString)), true, true),
      (ShowAndTerminateColumn(ACCESS_COL), true, true),
      (ShowAndTerminateColumn(DATABASE_ID_COL), false, true),
      (ShowAndTerminateColumn(SERVER_ID_COL), false, true),
      (ShowAndTerminateColumn(ADDRESS_COL), true, true),
      (ShowAndTerminateColumn(ROLE_COL), true, true),
      (ShowAndTerminateColumn(WRITER_COL, CTBoolean), true, true),
      (ShowAndTerminateColumn(REQUESTED_STATUS_COL), true, true),
      (ShowAndTerminateColumn(CURRENT_STATUS_COL), true, true),
      (ShowAndTerminateColumn(STATUS_MSG_COL), true, true)
    ) ++ (dbScope match {
      case _: DefaultDatabaseScope => List.empty
      case _: HomeDatabaseScope    => List.empty
      case _ =>
        List(
          (ShowAndTerminateColumn(DEFAULT_COL, CTBoolean), true, true),
          (ShowAndTerminateColumn(HOME_COL, CTBoolean), true, true)
        )
    }) ++ List(
      (ShowAndTerminateColumn(CURRENT_PRIMARIES_COUNT_COL, CTInteger), false, true),
      (ShowAndTerminateColumn(CURRENT_SECONDARIES_COUNT_COL, CTInteger), false, true),
      (ShowAndTerminateColumn(CURRENT_PROPERTY_SHARD_REPLICA_COUNT_COL, CTInteger), false, false),
      (ShowAndTerminateColumn(REQUESTED_PRIMARIES_COUNT_COL, CTInteger), false, true),
      (ShowAndTerminateColumn(REQUESTED_SECONDARIES_COUNT_COL, CTInteger), false, true),
      (ShowAndTerminateColumn(REQUESTED_PROPERTY_SHARDS_REPLICA_COUNT_COL, CTInteger), false, false),
      (ShowAndTerminateColumn(CREATION_TIME_COL, CTDateTime), false, true),
      (ShowAndTerminateColumn(LAST_START_TIME_COL, CTDateTime), false, true),
      (ShowAndTerminateColumn(LAST_STOP_TIME_COL, CTDateTime), false, true),
      (ShowAndTerminateColumn(STORE_COL), false, true),
      (ShowAndTerminateColumn(LAST_COMMITTED_TX_COL, CTInteger), false, true),
      (ShowAndTerminateColumn(REPLICATION_LAG_COL, CTInteger), false, true),
      (ShowAndTerminateColumn(SHARD_TX_LAG_COL, CTInteger), false, false),
      (ShowAndTerminateColumn(CONSTITUENTS_COL, CTList(CTString)), true, true),
      (ShowAndTerminateColumn(GRAPH_SHARDS_COL, CTList(CTString)), false, false),
      (ShowAndTerminateColumn(PROPERTY_SHARDS_COL, CTList(CTString)), false, false),
      (ShowAndTerminateColumn(DEFAULT_LANGUAGE_COL), false, true),
      (ShowAndTerminateColumn(OPTIONS_COL, CTMap), false, true)
    )

    val briefCols = cols.collect { case (col, true, showInCypher5) if !cypher5ColumnsOnly || showInCypher5 => col }
    val allCols = cols.collect { case (col, _, showInCypher5) if !cypher5ColumnsOnly || showInCypher5 => col }

    ShowDatabasesClause(
      dbScope,
      briefCols,
      allCols,
      where,
      yieldItems,
      yieldAll,
      yieldWith
    )(position)
  }
}

object ShowDatabase {

  // Must be the same for all rows of a database
  val ALIASES_COL = "aliases"
  val REQUESTED_STATUS_COL = "requestedStatus"
  val DEFAULT_COL = "default"
  val HOME_COL = "home"
  val REQUESTED_PRIMARIES_COUNT_COL = "requestedPrimariesCount"
  val REQUESTED_SECONDARIES_COUNT_COL = "requestedSecondariesCount"
  val REQUESTED_PROPERTY_SHARDS_REPLICA_COUNT_COL = "requestedPropertyShardReplicas"
  val CREATION_TIME_COL = "creationTime"
  val LAST_START_TIME_COL = "lastStartTime"
  val LAST_STOP_TIME_COL = "lastStopTime"
  val CONSTITUENTS_COL = "constituents"
  val GRAPH_SHARDS_COL = "graphShards"
  val PROPERTY_SHARDS_COL = "propertyShards"
  val DEFAULT_LANGUAGE_COL = "defaultLanguage"
  val NAME_COL = "name"
  val TYPE_COL = "type"
  val CURRENT_PRIMARIES_COUNT_COL = "currentPrimariesCount"
  val CURRENT_SECONDARIES_COUNT_COL = "currentSecondariesCount"
  val CURRENT_PROPERTY_SHARD_REPLICA_COUNT_COL = "currentPropertyShardReplicas"
  val OPTIONS_COL = "options"

  // If present, must be the same for every row for a database
  val DATABASE_ID_COL = "databaseID"
  val STORE_COL = "store"

  // Can/will/must be different for every row for a database
  val ACCESS_COL = "access"
  val ROLE_COL = "role"
  val PROPERTY_SHARD_REPLICA_ROLE = "property shard replica"
  val WRITER_COL = "writer"
  val CURRENT_STATUS_COL = "currentStatus"
  val STATUS_MSG_COL = "statusMessage"
  val LAST_COMMITTED_TX_COL = "lastCommittedTxn"
  val REPLICATION_LAG_COL = "replicationLag"
  val SHARD_TX_LAG_COL = "shardTxnLag"
  val SERVER_ID_COL = "serverID"
  val ADDRESS_COL = "address"

}
