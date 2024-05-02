/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour
import org.neo4j.cypher.internal.ast.SubqueryCall.InTransactionsOnErrorBehaviour.OnErrorFail
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.AndsReorderable
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.ListComprehension
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.ResolvedCall
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.cypher.internal.ir.CSVFormat
import org.neo4j.cypher.internal.ir.CreateCommand
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.CreatePattern
import org.neo4j.cypher.internal.ir.CreateRelationship
import org.neo4j.cypher.internal.ir.EagernessReason
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.RemoveLabelPattern
import org.neo4j.cypher.internal.ir.SetLabelPattern
import org.neo4j.cypher.internal.ir.SetMutatingPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetNodePropertiesPattern
import org.neo4j.cypher.internal.ir.SetNodePropertyPattern
import org.neo4j.cypher.internal.ir.SetPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetPropertiesPattern
import org.neo4j.cypher.internal.ir.SetPropertyPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesFromMapPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertiesPattern
import org.neo4j.cypher.internal.ir.SetRelationshipPropertyPattern
import org.neo4j.cypher.internal.ir.ShortestRelationshipPattern
import org.neo4j.cypher.internal.ir.SimpleMutatingPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.label_expressions.LabelExpression.disjoinRelTypesToLabelExpression
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.TrailParameters
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.pos
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Anti
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.ArgumentTracker
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
import org.neo4j.cypher.internal.logical.plans.AssertSameRelationship
import org.neo4j.cypher.internal.logical.plans.BFSPruningVarExpand
import org.neo4j.cypher.internal.logical.plans.BidirectionalRepeatTrail
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.DeleteExpression
import org.neo4j.cypher.internal.logical.plans.DeleteNode
import org.neo4j.cypher.internal.logical.plans.DeletePath
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.DetachDeleteExpression
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.DetachDeletePath
import org.neo4j.cypher.internal.logical.plans.DirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.DirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandAll
import org.neo4j.cypher.internal.logical.plans.Expand.ExpandInto
import org.neo4j.cypher.internal.logical.plans.Expand.ExpansionMode
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.DisallowSameNode
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths.SameNodeMode
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexSeek
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.InjectCompilationError
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.IntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.LetAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.LetSemiApply
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LoadCSV
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.Merge
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NFA
import org.neo4j.cypher.internal.logical.plans.NestedPlanCollectExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanExistsExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanGetByNameExpression
import org.neo4j.cypher.internal.logical.plans.NodeByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeekLeafPlan
import org.neo4j.cypher.internal.logical.plans.NonFuseable
import org.neo4j.cypher.internal.logical.plans.NonPipelined
import org.neo4j.cypher.internal.logical.plans.NonPipelinedStreaming
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.PartitionedAllNodesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.PartitionedDirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedIntersectionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedNodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.PartitionedSubtractionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.PartitionedUnwindCollection
import org.neo4j.cypher.internal.logical.plans.PathPropagatingBFS
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxRange
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PointDistanceRange
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RelationshipIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.RemoteBatchProperties
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.RepeatOptions
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.RunQueryAt
import org.neo4j.cypher.internal.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.logical.plans.SetLabels
import org.neo4j.cypher.internal.logical.plans.SetNodeProperties
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.SetProperties
import org.neo4j.cypher.internal.logical.plans.SetPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetProperty
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperties
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.SimulatedExpand
import org.neo4j.cypher.internal.logical.plans.SimulatedNodeScan
import org.neo4j.cypher.internal.logical.plans.SimulatedSelection
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.LengthBounds
import org.neo4j.cypher.internal.logical.plans.StatefulShortestPath.Mapping
import org.neo4j.cypher.internal.logical.plans.SubqueryForeach
import org.neo4j.cypher.internal.logical.plans.SubtractionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.Top1WithTies
import org.neo4j.cypher.internal.logical.plans.Trail
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionConcurrency
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.TriadicBuild
import org.neo4j.cypher.internal.logical.plans.TriadicFilter
import org.neo4j.cypher.internal.logical.plans.TriadicSelection
import org.neo4j.cypher.internal.logical.plans.UndirectedAllRelationshipsScan
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByElementIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.UndirectedUnionRelationshipTypesScan
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnionNodeByLabelsScan
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.rewriting.rewriters.HasLabelsAndHasTypeNormalizer
import org.neo4j.cypher.internal.rewriting.rewriters.combineHasLabels
import org.neo4j.cypher.internal.rewriting.rewriters.desugarMapProjection
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InputPosition.NONE
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.Rewritable.RewritableAny
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.collection.immutable.ListSet
import org.neo4j.cypher.internal.util.inSequence
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.TypeSpec
import org.neo4j.cypher.internal.util.topDown
import org.neo4j.graphdb.schema.IndexType

import scala.collection.mutable.ArrayBuffer

/**
 * Used by [[AbstractLogicalPlanBuilder]] to resolve tokens and procedures
 */
trait Resolver {

  /**
   * Obtain the token of a label by name.
   */
  def getLabelId(label: String): Int

  def getRelTypeId(label: String): Int

  def getPropertyKeyId(prop: String): Int

  def procedureSignature(name: QualifiedName): ProcedureSignature

  def functionSignature(name: QualifiedName): Option[UserFunctionSignature]
}

/**
 * Test help utility for hand-writing objects needing logical plans.
 * @param wholePlan Validate that we are creating a whole plan
 */
abstract class AbstractLogicalPlanBuilder[T, IMPL <: AbstractLogicalPlanBuilder[T, IMPL]](
  protected val resolver: Resolver,
  wholePlan: Boolean = true,
  initialId: Int = 0
) {

  self: IMPL =>

  val patternParser = new PatternParser
  protected var semanticTable = new SemanticTable()

  sealed protected trait OperatorBuilder

  protected case class LeafOperator(planToIdConstructor: IdGen => LogicalPlan) extends OperatorBuilder {
    private val id = idGen.id()
    _idOfLastPlan = id
    def planConstructor(): LogicalPlan = planToIdConstructor(SameId(id))
  }

  protected case class UnaryOperator(planToIdConstructor: LogicalPlan => IdGen => LogicalPlan) extends OperatorBuilder {
    private val id = idGen.id()
    _idOfLastPlan = id
    def planConstructor: LogicalPlan => LogicalPlan = planToIdConstructor(_)(SameId(id))
  }

  protected case class BinaryOperator(planToIdConstructor: (LogicalPlan, LogicalPlan) => IdGen => LogicalPlan)
      extends OperatorBuilder {
    private val id = idGen.id()
    _idOfLastPlan = id
    def planConstructor: (LogicalPlan, LogicalPlan) => LogicalPlan = planToIdConstructor(_, _)(SameId(id))
  }

  protected class Tree(operator: OperatorBuilder) {
    private var _left: Option[Tree] = None
    private var _right: Option[Tree] = None

    def left: Option[Tree] = _left

    def left_=(newVal: Option[Tree]): Unit = {
      operator match {
        case _: LeafOperator =>
          throw new IllegalArgumentException(s"Cannot attach a LHS to a leaf plan.")
        case _ =>
      }
      _left = newVal
    }

    def right: Option[Tree] = _right

    def right_=(newVal: Option[Tree]): Unit = {
      operator match {
        case _: LeafOperator =>
          throw new IllegalArgumentException(s"Cannot attach a RHS to a leaf plan.")
        case _: UnaryOperator =>
          throw new IllegalArgumentException(s"Cannot attach a RHS to a unary plan.")
        case _ =>
      }
      _right = newVal
    }

    def build(): LogicalPlan = {
      operator match {
        case o: LeafOperator =>
          o.planConstructor()
        case o: UnaryOperator =>
          o.planConstructor(left.get.build())
        case o: BinaryOperator =>
          (left, right) match {
            case (Some(leftVal), Some(rightVal)) => o.planConstructor(leftVal.build(), rightVal.build())
            case (None, _) =>
              val fakePlan = o.planConstructor(Argument()(idGen), Argument()(idGen)).productPrefix
              throw new IllegalStateException(s"Tried building plan '$fakePlan' but left operator is missing.")
            case (_, None) =>
              val fakePlan = o.planConstructor(Argument()(idGen), Argument()(idGen)).productPrefix
              throw new IllegalStateException(s"Tried building plan '$fakePlan' but right operator is missing.")
          }
      }
    }
  }

  val idGen: IdGen = new SequentialIdGen(initialId)

  private var tree: Tree = _
  private val looseEnds = new ArrayBuffer[Tree]
  private var indent = 0
  protected var resultColumns: Array[String] = _

  private var _idOfLastPlan = Id.INVALID_ID

  protected def idOfLastPlan: Id = _idOfLastPlan

  /**
   * Increase indent. The indent determines where the next
   * logical plan will be appended to the tree.
   */
  def | : IMPL = {
    indent += 1
    self
  }

  def resetIndent(): IMPL = {
    indent = 0
    self
  }

  def planIf(condition: Boolean)(builder: IMPL => IMPL): IMPL = {
    if (condition) {
      builder(self)
    } else {
      self
    }
  }

  // OPERATORS

  def produceResults(vars: String*): IMPL = {
    val resultColumnsSeq = vars.map(VariableParser.unescaped)
    resultColumns = resultColumnsSeq.toArray
    tree = new Tree(UnaryOperator(lp => ProduceResult(lp, resultColumnsSeq.map(varFor))(_)))
    looseEnds += tree
    self
  }

  def procedureCall(call: String, withFakedFullDeclarations: Boolean = false): IMPL = {
    val unresolvedCall = Parser.parseProcedureCall(call)
    appendAtCurrentIndent(UnaryOperator(lp => {
      val resolvedCall =
        ResolvedCall(resolver.procedureSignature)(unresolvedCall)
          .coerceArguments
      val rewrittenResolvedCall =
        if (withFakedFullDeclarations) resolvedCall.withFakedFullDeclarations else resolvedCall
      ProcedureCall(lp, rewrittenResolvedCall)(_)
    }))
    self
  }

  def optional(protectedSymbols: String*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Optional(lp, protectedSymbols.map(varFor).toSet)(_)))
    self
  }

  def anti(): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Anti(lp)(_)))
    self
  }

  def limit(count: Long): IMPL =
    limit(literalInt(count))

  def limit(countExpr: Expression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Limit(lp, countExpr)(_)))
    self
  }

  def exhaustiveLimit(count: Long): IMPL =
    exhaustiveLimit(literalInt(count))

  def exhaustiveLimit(countExpr: Expression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => ExhaustiveLimit(lp, countExpr)(_)))
    self
  }

  def skip(count: Long): IMPL =
    skip(literalInt(count))

  def skip(countExpr: Expression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Skip(lp, countExpr)(_)))
    self
  }

  def argumentTracker(): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => ArgumentTracker(lp)(_)))
    self
  }

  def expand(
    pattern: String,
    expandMode: ExpansionMode = ExpandAll,
    projectedDir: SemanticDirection = OUTGOING,
    nodePredicates: Seq[Predicate] = Seq.empty,
    relationshipPredicates: Seq[Predicate] = Seq.empty
  ): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    if (expandMode == ExpandAll) {
      newNode(varFor(p.to))
    }

    p.length match {
      case SimplePatternLength =>
        appendAtCurrentIndent(UnaryOperator(lp =>
          Expand(lp, varFor(p.from), p.dir, p.relTypes, varFor(p.to), varFor(p.relName), expandMode)(_)
        ))
      case varPatternLength: VarPatternLength =>
        appendAtCurrentIndent(UnaryOperator(lp =>
          VarExpand(
            lp,
            varFor(p.from),
            p.dir,
            projectedDir,
            p.relTypes,
            varFor(p.to),
            varFor(p.relName),
            varPatternLength,
            expandMode,
            nodePredicates.map(_.asVariablePredicate),
            relationshipPredicates.map(_.asVariablePredicate)
          )(_)
        ))
    }
    self
  }

  def simulatedExpand(fromNode: String, rel: String, toNode: String, factor: Double): IMPL = {
    val from = VariableParser.unescaped(fromNode)
    val urel = VariableParser.unescaped(rel)
    val to = VariableParser.unescaped(toNode)
    newNode(varFor(from))
    newRelationship(varFor(urel))
    newNode(varFor(to))
    appendAtCurrentIndent(UnaryOperator(lp =>
      SimulatedExpand(lp, varFor(from), varFor(urel), varFor(to), factor)(_)
    ))
    self
  }

  def shortestPath(
    pattern: String,
    pathName: Option[String] = None,
    all: Boolean = false,
    nodePredicates: Seq[Predicate] = Seq.empty,
    relationshipPredicates: Seq[Predicate] = Seq.empty,
    pathPredicates: Seq[String] = Seq.empty,
    withFallback: Boolean = false,
    sameNodeMode: SameNodeMode = DisallowSameNode
  ): IMPL =
    shortestPathSolver(
      pattern,
      pathName,
      all,
      nodePredicates.map(_.asVariablePredicate),
      relationshipPredicates.map(_.asVariablePredicate),
      pathPredicates.map(parseExpression),
      withFallback,
      sameNodeMode
    )

  def shortestPathExpr(
    pattern: String,
    pathName: Option[String] = None,
    all: Boolean = false,
    nodePredicates: Seq[VariablePredicate] = Seq.empty,
    relationshipPredicates: Seq[VariablePredicate] = Seq.empty,
    pathPredicates: Seq[Expression] = Seq.empty,
    withFallback: Boolean = false,
    sameNodeMode: SameNodeMode = DisallowSameNode
  ): IMPL =
    shortestPathSolver(
      pattern,
      pathName,
      all,
      nodePredicates,
      relationshipPredicates,
      pathPredicates,
      withFallback,
      sameNodeMode
    )

  def statefulShortestPathExpr(
    sourceNode: String,
    targetNode: String,
    solvedExpressionString: String,
    nonInlinedPreFilters: Option[Expression],
    groupNodes: Set[(String, String)],
    groupRelationships: Set[(String, String)],
    singletonNodeVariables: Set[(String, String)],
    singletonRelationshipVariables: Set[(String, String)],
    selector: StatefulShortestPath.Selector,
    nfa: NFA,
    mode: ExpansionMode,
    reverseGroupVariableProjections: Boolean = false,
    minLength: Int = 0,
    maxLength: Option[Int] = None
  ): IMPL = {
    val nodeVariableGroupings = groupNodes.map { case (x, y) => VariableGrouping(varFor(x), varFor(y))(pos) }
    val relationshipVariableGroupings = groupRelationships.map { case (x, y) =>
      VariableGrouping(varFor(x), varFor(y))(pos)
    }
    val singletonNodeMappings = singletonNodeVariables.map { case (x, y) => Mapping(varFor(x), varFor(y)) }
    val singletonRelMappings = singletonRelationshipVariables.map { case (x, y) => Mapping(varFor(x), varFor(y)) }

    // Assign types to group variables
    nodeVariableGroupings.map(_.group.asInstanceOf[Variable])
      .foreach(newVariable(_, CTList(CTNode)))
    relationshipVariableGroupings.map(_.group.asInstanceOf[Variable])
      .foreach(newVariable(_, CTList(CTRelationship)))

    // Assign types to singleton variables.
    // Be aware that this will override the types from the group variables if they share the same name with
    // the singleton variables. This will for instance happen if you use .enableDeduplicateNames(true),
    // which is the default setting in any LogicalPlanning integration test.
    // Overriding the type of the group variable is usually fine, except for Eagerness analysis on logical plans.
    // For these tests, you might want to consider setting .enableDeduplicateNames(false).
    newNodes(nfa.nodes.map(_.name) + targetNode)
    newRelationships(nfa.relationships.map(_.name))
    singletonNodeMappings.map(_.rowVar.asInstanceOf[Variable])
      .foreach(newVariable(_, CTNode))
    singletonRelMappings.map(_.rowVar.asInstanceOf[Variable])
      .foreach(newVariable(_, CTRelationship))

    appendAtCurrentIndent(UnaryOperator(lp =>
      StatefulShortestPath(
        lp,
        varFor(sourceNode),
        varFor(targetNode),
        nfa.endoRewrite(expressionRewriter),
        mode,
        nonInlinedPreFilters.endoRewrite(expressionRewriter),
        nodeVariableGroupings,
        relationshipVariableGroupings,
        singletonNodeMappings,
        singletonRelMappings,
        selector,
        solvedExpressionString,
        reverseGroupVariableProjections,
        LengthBounds(minLength, maxLength)
      )(_)
    ))
    self
  }

  def statefulShortestPath(
    sourceNode: String,
    targetNode: String,
    solvedExpressionString: String,
    nonInlinedPreFilters: Option[String],
    groupNodes: Set[(String, String)],
    groupRelationships: Set[(String, String)],
    singletonNodeVariables: Set[(String, String)],
    singletonRelationshipVariables: Set[(String, String)],
    selector: StatefulShortestPath.Selector,
    nfa: NFA,
    mode: ExpansionMode,
    reverseGroupVariableProjections: Boolean = false,
    minLength: Int = 0,
    maxLength: Option[Int] = None
  ): IMPL = {
    val predicates = nonInlinedPreFilters.map(parseExpression)
    statefulShortestPathExpr(
      sourceNode,
      targetNode,
      solvedExpressionString,
      predicates,
      groupNodes,
      groupRelationships,
      singletonNodeVariables,
      singletonRelationshipVariables,
      selector,
      nfa,
      mode,
      reverseGroupVariableProjections,
      minLength,
      maxLength
    )
  }

  private def shortestPathSolver(
    pattern: String,
    pathName: Option[String],
    all: Boolean,
    nodePredicates: Seq[VariablePredicate],
    relationshipPredicates: Seq[VariablePredicate],
    pathPredicates: Seq[Expression],
    withFallback: Boolean,
    sameNodeMode: SameNodeMode
  ): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))

    val length = p.length match {
      case SimplePatternLength => None
      case VarPatternLength(min, max) => Some(Some(Range(
          Some(UnsignedDecimalIntegerLiteral(min.toString)(pos)),
          max.map(i => UnsignedDecimalIntegerLiteral(i.toString)(pos))
        )(pos)))
    }

    appendAtCurrentIndent(UnaryOperator(lp =>
      FindShortestPaths(
        lp,
        ShortestRelationshipPattern(
          pathName.map(varFor),
          PatternRelationship(varFor(p.relName), (varFor(p.from), varFor(p.to)), p.dir, p.relTypes, p.length),
          !all
        )(ShortestPathsPatternPart(
          RelationshipChain(
            NodePattern(Some(varFor(p.from)), None, None, None)(
              pos
            ), // labels, properties and predicates are not used at runtime
            RelationshipPattern(
              Some(varFor(p.relName)),
              disjoinRelTypesToLabelExpression(p.relTypes),
              length,
              None, // properties are not used at runtime
              None,
              p.dir
            )(pos),
            NodePattern(Some(varFor(p.to)), None, None, None)(
              pos
            ) // labels, properties and predicates are not used at runtime
          )(pos),
          !all
        )(pos)),
        nodePredicates,
        relationshipPredicates,
        pathPredicates,
        withFallback,
        sameNodeMode
      )(_)
    ))
  }

  def pruningVarExpand(
    pattern: String,
    nodePredicates: Seq[Predicate] = Seq.empty,
    relationshipPredicates: Seq[Predicate] = Seq.empty
  ): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    newNode(varFor(p.to))
    p.length match {
      case VarPatternLength(min, Some(max)) =>
        appendAtCurrentIndent(UnaryOperator(lp =>
          PruningVarExpand(
            lp,
            varFor(p.from),
            p.dir,
            p.relTypes,
            varFor(p.to),
            min,
            max,
            nodePredicates.map(_.asVariablePredicate),
            relationshipPredicates.map(_.asVariablePredicate)
          )(_)
        ))
      case _ =>
        throw new IllegalArgumentException("This pattern is not compatible with pruning var expand")
    }
    self
  }

  def bfsPruningVarExpand(
    pattern: String,
    depthName: Option[String] = None,
    nodePredicates: Seq[Predicate] = Seq.empty,
    relationshipPredicates: Seq[Predicate] = Seq.empty,
    mode: ExpansionMode = ExpandAll
  ): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    if (mode == ExpandAll) {
      newNode(varFor(p.to))
    }
    p.length match {
      case VarPatternLength(min, maybeMax) if min <= 1 =>
        appendAtCurrentIndent(UnaryOperator(lp =>
          BFSPruningVarExpand(
            lp,
            varFor(p.from),
            p.dir,
            p.relTypes,
            varFor(p.to),
            min == 0,
            maxLength = maybeMax.getOrElse(Int.MaxValue),
            depthName.map(varFor),
            mode,
            nodePredicates.map(_.asVariablePredicate),
            relationshipPredicates.map(_.asVariablePredicate)
          )(_)
        ))
      case _ =>
        throw new IllegalArgumentException("This pattern is not compatible with a bfs pruning var expand")
    }
    self
  }

  def pathPropagatingBFS(
    pattern: String,
    projectedDir: SemanticDirection = OUTGOING,
    nodePredicates: Seq[Predicate] = Seq.empty,
    relationshipPredicates: Seq[Predicate] = Seq.empty
  ): IMPL = {
    val p = patternParser.parse(pattern)
    val varPatternLength = p.length.asInstanceOf[VarPatternLength]

    newRelationship(varFor(p.relName))
    newNode(varFor(p.to))

    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      PathPropagatingBFS(
        lhs,
        rhs,
        varFor(p.from),
        p.dir,
        projectedDir,
        p.relTypes,
        varFor(p.to),
        varFor(p.relName),
        varPatternLength,
        nodePredicates.map(_.asVariablePredicate),
        relationshipPredicates.map(_.asVariablePredicate)
      )(_)
    ))
  }

  def expandInto(pattern: String): IMPL = expand(pattern, ExpandInto)

  def optionalExpandAll(pattern: String, predicate: Option[String] = None): IMPL =
    optionalExpandAll(
      patternParser.parse(pattern),
      predicate.map(parseExpression).map(p => Ands(ListSet(p))(p.position))
    )

  private def optionalExpandAll(pattern: PatternParser.Pattern, predicate: Option[Expression]): IMPL = {
    val nodes = Set(pattern.from, pattern.to)
    val rewrittenPredicate = predicate.map(_.endoRewrite(topDown(Rewriter.lift {
      case p @ HasLabelsOrTypes(v: Variable, labelsOrTypes) if nodes.contains(v.name) =>
        HasLabels(v, labelsOrTypes.map(l => LabelName(l.name)(l.position)))(p.position)
      case p @ HasLabelsOrTypes(v: Variable, labelsOrTypes) if pattern.relName == v.name =>
        HasTypes(v, labelsOrTypes.map(l => RelTypeName(l.name)(l.position)))(p.position)
    })))
    pattern.length match {
      case SimplePatternLength =>
        appendAtCurrentIndent(UnaryOperator(lp =>
          OptionalExpand(
            lp,
            varFor(pattern.from),
            pattern.dir,
            pattern.relTypes,
            varFor(pattern.to),
            varFor(pattern.relName),
            ExpandAll,
            rewrittenPredicate
          )(_)
        ))
      case _ =>
        throw new IllegalArgumentException("Cannot have optional expand with variable length pattern")
    }
    self
  }

  def optionalExpandInto(pattern: String, predicate: Option[String] = None): IMPL = {
    val p = patternParser.parse(pattern)
    p.length match {
      case SimplePatternLength =>
        val pred = predicate.map(parseExpression).map(p => Ands(ListSet(p))(p.position))
        appendAtCurrentIndent(UnaryOperator(lp =>
          OptionalExpand(lp, varFor(p.from), p.dir, p.relTypes, varFor(p.to), varFor(p.relName), ExpandInto, pred)(_)
        ))
      case _ =>
        throw new IllegalArgumentException("Cannot have optional expand with variable length pattern")
    }
    self
  }

  def projectEndpoints(pattern: String, startInScope: Boolean, endInScope: Boolean): IMPL = {
    val p = patternParser.parse(pattern)
    newNode(varFor(p.from))
    newNode(varFor(p.to))
    appendAtCurrentIndent(UnaryOperator(lp =>
      ProjectEndpoints(
        lp,
        varFor(p.relName),
        varFor(p.from),
        startInScope,
        varFor(p.to),
        endInScope,
        p.relTypes,
        p.dir,
        p.length
      )(_)
    ))
  }

  def partialSort(alreadySortedPrefix: Seq[String], stillToSortSuffix: Seq[String]): IMPL =
    partialSortColumns(Parser.parseSort(alreadySortedPrefix), Parser.parseSort(stillToSortSuffix))

  def partialSortColumns(alreadySortedPrefix: Seq[ColumnOrder], stillToSortSuffix: Seq[ColumnOrder]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => PartialSort(lp, alreadySortedPrefix, stillToSortSuffix, None)(_)))
    self
  }

  def partialSortColumns(
    alreadySortedPrefix: Seq[ColumnOrder],
    stillToSortSuffix: Seq[ColumnOrder],
    skipSortingPrefixLength: Long
  ): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp =>
      PartialSort(lp, alreadySortedPrefix, stillToSortSuffix, Some(literalInt(skipSortingPrefixLength)))(_)
    ))
    self
  }

  def partialSort(
    alreadySortedPrefix: Seq[String],
    stillToSortSuffix: Seq[String],
    skipSortingPrefixLength: Long = 0
  ): IMPL = {
    val skipSort = if (skipSortingPrefixLength == 0) None else Some(literalInt(skipSortingPrefixLength))
    appendAtCurrentIndent(UnaryOperator(lp =>
      PartialSort(lp, Parser.parseSort(alreadySortedPrefix), Parser.parseSort(stillToSortSuffix), skipSort)(_)
    ))
    self
  }

  def sortColumns(sortItems: Seq[ColumnOrder]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Sort(lp, sortItems)(_)))
    self
  }

  def sort(sortItems: String*): IMPL = sortColumns(Parser.parseSort(sortItems))

  def top(limit: Long, sortItems: String*): IMPL = top(Parser.parseSort(sortItems), limit)

  def top(sortItems: Seq[ColumnOrder], limit: Long): IMPL =
    top(sortItems, literalInt(limit))

  def top(sortItems: Seq[ColumnOrder], limitExpr: Expression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Top(lp, sortItems, limitExpr)(_)))
    self
  }

  def top1WithTies(sortItems: String*): IMPL = top1WithTiesColumns(Parser.parseSort(sortItems))

  def top1WithTiesColumns(sortItems: Seq[ColumnOrder]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Top1WithTies(lp, sortItems)(_)))
    self
  }

  def partialTop(alreadySortedPrefix: Seq[ColumnOrder], stillToSortSuffix: Seq[ColumnOrder], limit: Long): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp =>
      PartialTop(lp, alreadySortedPrefix, stillToSortSuffix, literalInt(limit), None)(_)
    ))
    self
  }

  def partialTop(limit: Long, alreadySortedPrefix: Seq[String], stillToSortSuffix: Seq[String]): IMPL = {
    partialTop(Parser.parseSort(alreadySortedPrefix), Parser.parseSort(stillToSortSuffix), limit)
  }

  def partialTop(
    alreadySortedPrefix: Seq[ColumnOrder],
    stillToSortSuffix: Seq[ColumnOrder],
    limit: Long,
    skipSortingPrefixLength: Long
  ): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp =>
      PartialTop(
        lp,
        alreadySortedPrefix,
        stillToSortSuffix,
        literalInt(limit),
        Some(literalInt(skipSortingPrefixLength))
      )(_)
    ))
    self
  }

  def partialTop(
    limit: Long,
    skipSortingPrefixLength: Long,
    alreadySortedPrefix: Seq[String],
    stillToSortSuffix: Seq[String]
  ): IMPL = {
    partialTop(
      Parser.parseSort(alreadySortedPrefix),
      Parser.parseSort(stillToSortSuffix),
      limit,
      skipSortingPrefixLength
    )
  }

  def eager(reasons: ListSet[EagernessReason] = ListSet(EagernessReason.Unknown)): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Eager(lp, reasons)(_)))
    self
  }

  def emptyResult(): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => EmptyResult(lp)(_)))
    self
  }

  def deleteNode(node: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DeleteNode(lp, parseExpression(node))(_)))
    self
  }

  def deleteNode(node: Expression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DeleteNode(lp, node)(_)))
    self
  }

  def detachDeleteNode(node: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DetachDeleteNode(lp, parseExpression(node))(_)))
    self
  }

  def deleteRelationship(rel: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DeleteRelationship(lp, parseExpression(rel))(_)))
    self
  }

  def deleteRelationship(rel: Expression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DeleteRelationship(lp, rel)(_)))
    self
  }

  def deletePath(path: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DeletePath(lp, parseExpression(path))(_)))
    self
  }

  def detachDeletePath(path: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DetachDeletePath(lp, parseExpression(path))(_)))
    self
  }

  def deleteExpression(expression: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DeleteExpression(lp, parseExpression(expression))(_)))
    self
  }

  def deleteExpression(expression: Expression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DeleteExpression(lp, expression)(_)))
    self
  }

  def detachDeleteExpression(expression: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DetachDeleteExpression(lp, parseExpression(expression))(_)))
    self
  }

  def setLabels(nodeVariable: String, labels: String*): IMPL = {
    val labelNames = labels.map(l => LabelName(l)(InputPosition.NONE)).toSet
    appendAtCurrentIndent(UnaryOperator(lp => SetLabels(lp, varFor(nodeVariable), labelNames)(_)))
  }

  def removeLabels(nodeVariable: String, labels: String*): IMPL = {
    val labelNames = labels.map(l => LabelName(l)(InputPosition.NONE)).toSet
    appendAtCurrentIndent(UnaryOperator(lp => RemoveLabels(lp, varFor(nodeVariable), labelNames)(_)))
  }

  def unwind(projectionString: String): IMPL = {
    val (name, expression) = toVarMap(Parser.parseProjections(projectionString)).head
    appendAtCurrentIndent(UnaryOperator(lp => UnwindCollection(lp, name, expression)(_)))
    self
  }

  def partitionedUnwind(projectionString: String): IMPL = {
    val (name, expression) = toVarMap(Parser.parseProjections(projectionString)).head
    appendAtCurrentIndent(UnaryOperator(lp => PartitionedUnwindCollection(lp, name, expression)(_)))
    self
  }

  def runQueryAt(
    query: String,
    graphReference: String,
    parameters: Set[String] = Set.empty,
    importsAsParameters: Map[String, String] = Map.empty,
    columns: Set[String] = Set.empty
  ): IMPL = {
    val properParameters = parameters.map(asParameter)
    val properImports = importsAsParameters.map[Parameter, LogicalVariable] {
      case (parameterName, variableName) => asParameter(parameterName) -> varFor(variableName)
    }
    appendAtCurrentIndent(UnaryOperator(source =>
      RunQueryAt(
        source,
        query,
        Parser.parseGraphReference(graphReference),
        properParameters,
        properImports,
        columns.map(varFor)
      )(_)
    ))
  }

  private def asParameter(parameterName: String): Parameter =
    ExplicitParameter(parameterName.stripPrefix("$"), CTAny)(pos)

  def projection(projectionStrings: String*): IMPL = {
    doProjection(parseProjections(projectionStrings: _*))
  }

  def projection(projectExpressions: Map[String, Expression]): IMPL = {
    doProjection(toVarMap(projectExpressions))
  }

  private def doProjection(projectExpressions: Map[LogicalVariable, Expression]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => {
      val rewrittenProjections = projectExpressions.map { case (name, expr) =>
        name -> expr.endoRewrite(expressionRewriter)
      }
      projectExpressions.foreach { case (name, expr) => newAlias(name, expr) }
      Projection(lp, rewrittenProjections)(_)
    }))
    self
  }

  private def toVarMap(map: Map[String, Expression]): Map[LogicalVariable, Expression] = map.map {
    case (key, value) => varFor(key) -> value
  }

  def distinct(projectionStrings: String*): IMPL = {
    val projections = Parser.parseProjections(projectionStrings: _*)
    appendAtCurrentIndent(UnaryOperator(lp => Distinct(lp, toVarMap(projections))(_)))
    self
  }

  def orderedDistinct(orderToLeverage: Seq[String], projectionStrings: String*): IMPL = {
    val order = orderToLeverage.map(parseExpression)
    val projections = Parser.parseProjections(projectionStrings: _*)
    appendAtCurrentIndent(UnaryOperator(lp => OrderedDistinct(lp, toVarMap(projections), order)(_)))
    self
  }

  def allNodeScan(node: String, args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(AllNodesScan(
      varFor(n),
      args.map(a => varFor(VariableParser.unescaped(a))).toSet
    )(_)))
  }

  def partitionedAllNodeScan(node: String, args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(PartitionedAllNodesScan(
      varFor(n),
      args.map(a => varFor(VariableParser.unescaped(a))).toSet
    )(_)))
  }

  def simulatedNodeScan(node: String, numberOfRows: Long): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(SimulatedNodeScan(varFor(n), numberOfRows)(_)))
  }

  def argument(args: String*): IMPL = {
    appendAtCurrentIndent(LeafOperator(Argument(args.map(varFor).toSet)(_)))
  }

  def nodeByLabelScan(node: String, label: String, args: String*): IMPL =
    nodeByLabelScan(node, label, IndexOrderNone, args: _*)

  def nodeByLabelScan(node: String, label: String, indexOrder: IndexOrder, args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(NodeByLabelScan(
      varFor(n),
      labelName(label),
      args.map(a => varFor(VariableParser.unescaped(a))).toSet,
      indexOrder
    )(_)))
  }

  def partitionedNodeByLabelScan(node: String, label: String, args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(PartitionedNodeByLabelScan(
      varFor(n),
      labelName(label),
      args.map(a => varFor(VariableParser.unescaped(a))).toSet
    )(_)))
  }

  def unionNodeByLabelsScan(node: String, labels: Seq[String], args: String*): IMPL = {
    unionNodeByLabelsScan(node, labels, IndexOrderNone, args: _*)
  }

  def unionNodeByLabelsScan(node: String, labels: Seq[String], indexOrder: IndexOrder, args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(UnionNodeByLabelsScan(
      varFor(n),
      labels.map(labelName),
      args.map(a => varFor(VariableParser.unescaped(a))).toSet,
      indexOrder
    )(_)))
  }

  def partitionedUnionNodeByLabelsScan(node: String, labels: Seq[String], args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(PartitionedUnionNodeByLabelsScan(
      varFor(n),
      labels.map(labelName),
      args.map(a => varFor(VariableParser.unescaped(a))).toSet
    )(_)))
  }

  def intersectionNodeByLabelsScan(node: String, labels: Seq[String], args: String*): IMPL = {
    intersectionNodeByLabelsScan(node, labels, IndexOrderNone, args: _*)
  }

  def intersectionNodeByLabelsScan(node: String, labels: Seq[String], indexOrder: IndexOrder, args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(IntersectionNodeByLabelsScan(
      varFor(n),
      labels.map(labelName),
      args.map(a => varFor(VariableParser.unescaped(a))).toSet,
      indexOrder
    )(_)))
  }

  def partitionedIntersectionNodeByLabelsScan(node: String, labels: Seq[String], args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(PartitionedIntersectionNodeByLabelsScan(
      varFor(n),
      labels.map(labelName),
      args.map(a => varFor(VariableParser.unescaped(a))).toSet
    )(_)))
  }

  def subtractionNodeByLabelsScan(
    node: String,
    positiveLabel: String,
    negativeLabel: String,
    args: String*
  ): IMPL = {
    subtractionNodeByLabelsScan(node, Seq(positiveLabel), Seq(negativeLabel), args: _*)
  }

  def subtractionNodeByLabelsScan(
    node: String,
    positiveLabel: String,
    negativeLabel: String,
    indexOrder: IndexOrder,
    args: String*
  ): IMPL = {
    subtractionNodeByLabelsScan(node, Seq(positiveLabel), Seq(negativeLabel), indexOrder, args: _*)
  }

  def subtractionNodeByLabelsScan(
    node: String,
    positiveLabels: Seq[String],
    negativeLabels: Seq[String],
    args: String*
  ): IMPL = {
    subtractionNodeByLabelsScan(node, positiveLabels, negativeLabels, IndexOrderNone, args: _*)
  }

  def subtractionNodeByLabelsScan(
    node: String,
    positiveLabels: Seq[String],
    negativeLabels: Seq[String],
    indexOrder: IndexOrder,
    args: String*
  ): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(SubtractionNodeByLabelsScan(
      varFor(n),
      positiveLabels.map(labelName),
      negativeLabels.map(labelName),
      args.map(a => varFor(VariableParser.unescaped(a))).toSet,
      indexOrder
    )(_)))
  }

  def partitionedSubtractionNodeByLabelsScan(
    node: String,
    positiveLabels: Seq[String],
    negativeLabels: Seq[String],
    args: String*
  ): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(PartitionedSubtractionNodeByLabelsScan(
      varFor(n),
      positiveLabels.map(labelName),
      negativeLabels.map(labelName),
      args.map(a => varFor(VariableParser.unescaped(a))).toSet
    )(_)))
  }

  def unionRelationshipTypesScan(pattern: String, args: String*): IMPL = {
    unionRelationshipTypesScan(pattern, IndexOrderNone, args: _*)
  }

  def unionRelationshipTypesScan(pattern: String, indexOrder: IndexOrder, args: String*): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    newNode(varFor(p.from))
    newNode(varFor(p.to))
    if (!p.length.isSimple) throw new UnsupportedOperationException("Cannot do a scan from a variable pattern")

    p.dir match {
      case SemanticDirection.OUTGOING =>
        appendAtCurrentIndent(LeafOperator(DirectedUnionRelationshipTypesScan(
          varFor(p.relName),
          varFor(p.from),
          p.relTypes,
          varFor(p.to),
          args.map(varFor).toSet,
          indexOrder
        )(_)))
      case SemanticDirection.INCOMING =>
        appendAtCurrentIndent(LeafOperator(DirectedUnionRelationshipTypesScan(
          varFor(p.relName),
          varFor(p.to),
          p.relTypes,
          varFor(p.from),
          args.map(varFor).toSet,
          indexOrder
        )(_)))
      case SemanticDirection.BOTH =>
        appendAtCurrentIndent(LeafOperator(UndirectedUnionRelationshipTypesScan(
          varFor(p.relName),
          varFor(p.from),
          p.relTypes,
          varFor(p.to),
          args.map(varFor).toSet,
          indexOrder
        )(_)))
    }
  }

  def partitionedUnionRelationshipTypesScan(pattern: String, args: String*): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    newNode(varFor(p.from))
    newNode(varFor(p.to))
    if (!p.length.isSimple) throw new UnsupportedOperationException("Cannot do a scan from a variable pattern")

    p.dir match {
      case SemanticDirection.OUTGOING =>
        appendAtCurrentIndent(LeafOperator(PartitionedDirectedUnionRelationshipTypesScan(
          varFor(p.relName),
          varFor(p.from),
          p.relTypes,
          varFor(p.to),
          args.map(varFor).toSet
        )(_)))
      case SemanticDirection.INCOMING =>
        appendAtCurrentIndent(LeafOperator(PartitionedDirectedUnionRelationshipTypesScan(
          varFor(p.relName),
          varFor(p.to),
          p.relTypes,
          varFor(p.from),
          args.map(varFor).toSet
        )(_)))
      case SemanticDirection.BOTH =>
        appendAtCurrentIndent(LeafOperator(PartitionedUndirectedUnionRelationshipTypesScan(
          varFor(p.relName),
          varFor(p.from),
          p.relTypes,
          varFor(p.to),
          args.map(varFor).toSet
        )(_)))
    }
  }

  def nodeByIdSeek(node: String, args: Set[String], ids: Any*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))

    val input = idSeekInput(ids)
    appendAtCurrentIndent(LeafOperator(NodeByIdSeek(varFor(n), input, args.map(varFor))(_)))
  }

  private def directedRelationshipByIdSeekSolver(
    relationship: String,
    from: String,
    to: String,
    args: Set[String],
    expr: Seq[Expression]
  ): IMPL = {
    newRelationship(varFor(relationship))
    newNode(varFor(from))
    newNode(varFor(to))
    val input = ManySeekableArgs(ListLiteral(expr)(pos))

    appendAtCurrentIndent(LeafOperator(DirectedRelationshipByIdSeek(
      varFor(relationship),
      input,
      varFor(from),
      varFor(to),
      args.map(varFor)
    )(_)))
  }

  def directedRelationshipByIdSeek(
    relationship: String,
    from: String,
    to: String,
    args: Set[String],
    ids: AnyVal*
  ): IMPL = {
    val idExpressions: Seq[Expression] = ids.map {
      case x @ (_: Long | _: Int)     => SignedDecimalIntegerLiteral(x.toString)(pos)
      case x @ (_: Float | _: Double) => DecimalDoubleLiteral(x.toString)(pos)
      case x                          => throw new IllegalArgumentException(s"$x is not a supported value for ID")
    }

    directedRelationshipByIdSeekSolver(relationship, from, to, args, idExpressions)
  }

  def directedRelationshipByIdSeekExpr(
    relationship: String,
    from: String,
    to: String,
    args: Set[String],
    expr: Expression*
  ): IMPL = {
    directedRelationshipByIdSeekSolver(relationship, from, to, args, expr)
  }

  private def undirectedRelationshipByIdSeekSolver(
    relationship: String,
    from: String,
    to: String,
    args: Set[String],
    expr: Seq[Expression]
  ): IMPL = {
    newRelationship(varFor(relationship))
    newNode(varFor(from))
    newNode(varFor(to))
    val input = ManySeekableArgs(ListLiteral(expr)(pos))

    appendAtCurrentIndent(LeafOperator(UndirectedRelationshipByIdSeek(
      varFor(relationship),
      input,
      varFor(from),
      varFor(to),
      args.map(varFor)
    )(_)))
  }

  def undirectedRelationshipByIdSeek(
    relationship: String,
    from: String,
    to: String,
    args: Set[String],
    ids: AnyVal*
  ): IMPL = {
    newRelationship(varFor(relationship))
    newNode(varFor(from))
    newNode(varFor(to))
    val idExpressions = ids.map {
      case x @ (_: Long | _: Int)     => SignedDecimalIntegerLiteral(x.toString)(pos)
      case x @ (_: Float | _: Double) => DecimalDoubleLiteral(x.toString)(pos)
      case x                          => throw new IllegalArgumentException(s"$x is not a supported value for ID")
    }

    undirectedRelationshipByIdSeekSolver(relationship, from, to, args, idExpressions)
  }

  def undirectedRelationshipByIdSeekExpr(
    relationship: String,
    from: String,
    to: String,
    args: Set[String],
    expr: Expression*
  ): IMPL = {
    undirectedRelationshipByIdSeekSolver(relationship, from, to, args, expr)
  }

  def nodeByElementIdSeek(node: String, args: Set[String], ids: Any*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))

    val input = idSeekInput(ids)
    appendAtCurrentIndent(LeafOperator(NodeByElementIdSeek(varFor(n), input, args.map(varFor))(_)))
  }

  def directedRelationshipByElementIdSeek(
    relationship: String,
    from: String,
    to: String,
    args: Set[String],
    ids: Any*
  ): IMPL = {
    newRelationship(varFor(relationship))
    newNode(varFor(from))
    newNode(varFor(to))

    val input = idSeekInput(ids)
    appendAtCurrentIndent(LeafOperator(DirectedRelationshipByElementIdSeek(
      varFor(relationship),
      input,
      varFor(from),
      varFor(to),
      args.map(varFor)
    )(_)))
  }

  def undirectedRelationshipByElementIdSeek(
    relationship: String,
    from: String,
    to: String,
    args: Set[String],
    ids: Any*
  ): IMPL = {
    newRelationship(varFor(relationship))
    newNode(varFor(from))
    newNode(varFor(to))

    val input = idSeekInput(ids)
    appendAtCurrentIndent(LeafOperator(UndirectedRelationshipByElementIdSeek(
      varFor(relationship),
      input,
      varFor(from),
      varFor(to),
      args.map(varFor)
    )(_)))
  }

  private def idSeekInput(ids: Seq[Any]): ManySeekableArgs = {
    ids match {
      case Seq(expression: Expression) =>
        ManySeekableArgs(expression)
      case _ =>
        val idExpressions = ids.map {
          case x: Expression => x
          // This is a bit hacky but we cannot separate passing in an expression string
          // like "$param" and an actual id string "thisisanelementid". If we don't do this hack
          // the caller would have to quote elementIds all the time.
          case x: String =>
            try {
              Parser.parseExpression(x)
            } catch {
              case _: Exception => StringLiteral(x)(pos.withInputLength(0))
            }
          case x @ (_: Long | _: Int)     => SignedDecimalIntegerLiteral(x.toString)(pos)
          case x @ (_: Float | _: Double) => DecimalDoubleLiteral(x.toString)(pos)
          case x                          => throw new IllegalArgumentException(s"$x is not a supported value for ID")
        }
        ManySeekableArgs(ListLiteral(idExpressions)(pos))
    }
  }

  def allRelationshipsScan(pattern: String, args: String*): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    newNode(varFor(p.from))
    newNode(varFor(p.to))
    if (!p.length.isSimple) throw new UnsupportedOperationException("Cannot do a scan from a variable pattern")

    p.dir match {
      case SemanticDirection.OUTGOING =>
        appendAtCurrentIndent(LeafOperator(DirectedAllRelationshipsScan(
          varFor(p.relName),
          varFor(p.from),
          varFor(p.to),
          args.map(varFor).toSet
        )(_)))
      case SemanticDirection.INCOMING =>
        appendAtCurrentIndent(LeafOperator(DirectedAllRelationshipsScan(
          varFor(p.relName),
          varFor(p.to),
          varFor(p.from),
          args.map(varFor).toSet
        )(_)))
      case SemanticDirection.BOTH =>
        appendAtCurrentIndent(LeafOperator(UndirectedAllRelationshipsScan(
          varFor(p.relName),
          varFor(p.from),
          varFor(p.to),
          args.map(varFor).toSet
        )(_)))
    }
  }

  def partitionedAllRelationshipsScan(pattern: String, args: String*): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    newNode(varFor(p.from))
    newNode(varFor(p.to))
    if (!p.length.isSimple) throw new UnsupportedOperationException("Cannot do a scan from a variable pattern")

    p.dir match {
      case SemanticDirection.OUTGOING =>
        appendAtCurrentIndent(LeafOperator(PartitionedDirectedAllRelationshipsScan(
          varFor(p.relName),
          varFor(p.from),
          varFor(p.to),
          args.map(varFor).toSet
        )(_)))
      case SemanticDirection.INCOMING =>
        appendAtCurrentIndent(LeafOperator(PartitionedDirectedAllRelationshipsScan(
          varFor(p.relName),
          varFor(p.to),
          varFor(p.from),
          args.map(varFor).toSet
        )(_)))
      case SemanticDirection.BOTH =>
        appendAtCurrentIndent(LeafOperator(PartitionedUndirectedAllRelationshipsScan(
          varFor(p.relName),
          varFor(p.from),
          varFor(p.to),
          args.map(varFor).toSet
        )(_)))
    }
  }

  def relationshipTypeScan(pattern: String, args: String*): IMPL = {
    relationshipTypeScan(pattern, IndexOrderNone, args: _*)
  }

  def relationshipTypeScan(pattern: String, indexOrder: IndexOrder, args: String*): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    newNode(varFor(p.from))
    newNode(varFor(p.to))
    if (!p.length.isSimple) throw new UnsupportedOperationException("Cannot do a scan from a variable pattern")
    val typ =
      if (p.relTypes.size == 1) p.relTypes.head
      else throw new UnsupportedOperationException("Cannot do a scan with multiple types")

    p.dir match {
      case SemanticDirection.OUTGOING =>
        appendAtCurrentIndent(LeafOperator(DirectedRelationshipTypeScan(
          varFor(p.relName),
          varFor(p.from),
          typ,
          varFor(p.to),
          args.map(varFor).toSet,
          indexOrder
        )(_)))
      case SemanticDirection.INCOMING =>
        appendAtCurrentIndent(LeafOperator(DirectedRelationshipTypeScan(
          varFor(p.relName),
          varFor(p.to),
          typ,
          varFor(p.from),
          args.map(varFor).toSet,
          indexOrder
        )(_)))
      case SemanticDirection.BOTH =>
        appendAtCurrentIndent(LeafOperator(UndirectedRelationshipTypeScan(
          varFor(p.relName),
          varFor(p.from),
          typ,
          varFor(p.to),
          args.map(varFor).toSet,
          indexOrder
        )(_)))
    }
  }

  def partitionedRelationshipTypeScan(pattern: String, args: String*): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    newNode(varFor(p.from))
    newNode(varFor(p.to))
    if (!p.length.isSimple) throw new UnsupportedOperationException("Cannot do a scan from a variable pattern")
    val typ =
      if (p.relTypes.size == 1) p.relTypes.head
      else throw new UnsupportedOperationException("Cannot do a scan with multiple types")

    p.dir match {
      case SemanticDirection.OUTGOING =>
        appendAtCurrentIndent(LeafOperator(PartitionedDirectedRelationshipTypeScan(
          varFor(p.relName),
          varFor(p.from),
          typ,
          varFor(p.to),
          args.map(varFor).toSet
        )(_)))
      case SemanticDirection.INCOMING =>
        appendAtCurrentIndent(LeafOperator(PartitionedDirectedRelationshipTypeScan(
          varFor(p.relName),
          varFor(p.to),
          typ,
          varFor(p.from),
          args.map(varFor).toSet
        )(_)))
      case SemanticDirection.BOTH =>
        appendAtCurrentIndent(LeafOperator(PartitionedUndirectedRelationshipTypeScan(
          varFor(p.relName),
          varFor(p.from),
          typ,
          varFor(p.to),
          args.map(varFor).toSet
        )(_)))
    }
  }

  def nodeCountFromCountStore(name: String, labels: Seq[Option[String]], args: String*): IMPL = {
    val labelNames = labels.map(maybeLabel => maybeLabel.map(labelName)).toList
    appendAtCurrentIndent(LeafOperator(NodeCountFromCountStore(
      varFor(name),
      labelNames,
      args.map(a => varFor(VariableParser.unescaped(a))).toSet
    )(_)))
  }

  def relationshipCountFromCountStore(
    name: String,
    maybeStartLabel: Option[String],
    relTypes: Seq[String],
    maybeEndLabel: Option[String],
    args: String*
  ): IMPL = {
    val startLabel = maybeStartLabel.map(labelName)
    val relTypeNames = relTypes.map(relTypeName)
    val endLabel = maybeEndLabel.map(labelName)
    appendAtCurrentIndent(LeafOperator(RelationshipCountFromCountStore(
      varFor(name),
      startLabel,
      relTypeNames,
      endLabel,
      args.map(a => varFor(VariableParser.unescaped(a))).toSet
    )(_)))
  }

  def nodeIndexOperator(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    paramExpr: IterableOnce[Expression] = None,
    argumentIds: Set[String] = Set.empty,
    unique: Boolean = false,
    customQueryExpression: Option[QueryExpression[Expression]] = None,
    indexType: IndexType = IndexType.RANGE,
    supportPartitionedScan: Boolean = true
  ): IMPL = {
    val planBuilder = (idGen: IdGen) => {
      val plan = nodeIndexSeek(
        indexSeekString,
        getValue,
        indexOrder,
        paramExpr.iterator.toSeq,
        argumentIds,
        unique,
        customQueryExpression,
        indexType,
        supportPartitionedScan
      )(idGen)
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def partitionedNodeIndexOperator(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    paramExpr: IterableOnce[Expression] = None,
    argumentIds: Set[String] = Set.empty,
    customQueryExpression: Option[QueryExpression[Expression]] = None,
    indexType: IndexType = IndexType.RANGE
  ): IMPL = {
    val planBuilder = (idGen: IdGen) => {
      val plan = partitionedNodeIndexSeek(
        indexSeekString,
        getValue,
        paramExpr.iterator.toSeq,
        argumentIds,
        customQueryExpression,
        indexType
      )(idGen)
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def relationshipIndexOperator(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    paramExpr: Iterable[Expression] = Seq.empty,
    argumentIds: Set[String] = Set.empty,
    unique: Boolean = false,
    customQueryExpression: Option[QueryExpression[Expression]] = None,
    indexType: IndexType = IndexType.RANGE,
    supportPartitionedScan: Boolean = true
  ): IMPL = {
    val planBuilder = (idGen: IdGen) => {
      val plan = relationshipIndexSeek(
        indexSeekString,
        getValue,
        indexOrder,
        paramExpr,
        argumentIds,
        unique,
        customQueryExpression,
        indexType,
        supportPartitionedScan
      )(idGen)
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def partitionedRelationshipIndexOperator(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    paramExpr: Iterable[Expression] = Seq.empty,
    argumentIds: Set[String] = Set.empty,
    customQueryExpression: Option[QueryExpression[Expression]] = None,
    indexType: IndexType = IndexType.RANGE
  ): IMPL = {
    val planBuilder = (idGen: IdGen) => {
      val plan = partitionedRelationshipIndexSeek(
        indexSeekString,
        getValue,
        paramExpr,
        argumentIds,
        customQueryExpression,
        indexType
      )(idGen)
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def nodeIndexSeek(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    paramExpr: Iterable[Expression] = Seq.empty,
    argumentIds: Set[String] = Set.empty,
    unique: Boolean = false,
    customQueryExpression: Option[QueryExpression[Expression]] = None,
    indexType: IndexType = IndexType.RANGE,
    supportPartitionedScan: Boolean = true
  ): IdGen => NodeIndexLeafPlan = {
    val label = resolver.getLabelId(IndexSeek.labelFromIndexSeekString(indexSeekString))
    val propIds: PartialFunction[String, Int] = {
      case x => resolver.getPropertyKeyId(x)
    }
    val planBuilder = (idGen: IdGen) => {
      val plan = IndexSeek.nodeIndexSeek(
        indexSeekString,
        getValue,
        indexOrder,
        paramExpr,
        argumentIds,
        Some(propIds),
        label,
        unique,
        customQueryExpression,
        indexType,
        supportPartitionedScan
      )(idGen)
      newNode(varFor(plan.idName.name))
      plan
    }
    planBuilder
  }

  def partitionedNodeIndexSeek(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    paramExpr: Iterable[Expression] = Seq.empty,
    argumentIds: Set[String] = Set.empty,
    customQueryExpression: Option[QueryExpression[Expression]] = None,
    indexType: IndexType = IndexType.RANGE
  ): IdGen => NodeIndexLeafPlan = {
    val label = resolver.getLabelId(IndexSeek.labelFromIndexSeekString(indexSeekString))
    val propIds: PartialFunction[String, Int] = {
      case x => resolver.getPropertyKeyId(x)
    }
    val planBuilder = (idGen: IdGen) => {
      val plan = IndexSeek.partitionedNodeIndexSeek(
        indexSeekString,
        getValue,
        paramExpr,
        argumentIds,
        Some(propIds),
        label,
        customQueryExpression,
        indexType
      )(idGen)
      newNode(varFor(plan.idName.name))
      plan
    }
    planBuilder
  }

  def relationshipIndexSeek(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    paramExpr: Iterable[Expression] = Seq.empty,
    argumentIds: Set[String] = Set.empty,
    unique: Boolean = false,
    customQueryExpression: Option[QueryExpression[Expression]] = None,
    indexType: IndexType = IndexType.RANGE,
    supportPartitionedScan: Boolean = true
  ): IdGen => RelationshipIndexLeafPlan = {
    val relType = resolver.getRelTypeId(IndexSeek.relTypeFromIndexSeekString(indexSeekString))
    val propIds: PartialFunction[String, Int] = {
      case x => resolver.getPropertyKeyId(x)
    }
    val planBuilder = (idGen: IdGen) => {
      val plan = IndexSeek.relationshipIndexSeek(
        indexSeekString,
        getValue,
        indexOrder,
        paramExpr,
        argumentIds,
        Some(propIds),
        relType,
        unique,
        customQueryExpression,
        indexType,
        supportPartitionedScan
      )(idGen)
      newRelationship(varFor(plan.idName.name))
      newNode(varFor(plan.leftNode.name))
      newNode(varFor(plan.rightNode.name))
      plan
    }
    planBuilder
  }

  def partitionedRelationshipIndexSeek(
    indexSeekString: String,
    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
    paramExpr: Iterable[Expression] = Seq.empty,
    argumentIds: Set[String] = Set.empty,
    customQueryExpression: Option[QueryExpression[Expression]] = None,
    indexType: IndexType = IndexType.RANGE
  ): IdGen => RelationshipIndexLeafPlan = {
    val relType = resolver.getRelTypeId(IndexSeek.relTypeFromIndexSeekString(indexSeekString))
    val propIds: PartialFunction[String, Int] = {
      case x => resolver.getPropertyKeyId(x)
    }
    val planBuilder = (idGen: IdGen) => {
      val plan = IndexSeek.partitionedRelationshipIndexSeek(
        indexSeekString,
        getValue,
        paramExpr,
        argumentIds,
        Some(propIds),
        relType,
        customQueryExpression,
        indexType
      )(idGen)
      newRelationship(varFor(plan.idName.name))
      newNode(varFor(plan.leftNode.name))
      newNode(varFor(plan.rightNode.name))
      plan
    }
    planBuilder
  }

  def multiNodeIndexSeekOperator(seeks: (IMPL => IdGen => NodeIndexLeafPlan)*): IMPL = {
    val planBuilder = (idGen: IdGen) => {
      MultiNodeIndexSeek(seeks.map(_(this)(idGen).asInstanceOf[NodeIndexSeekLeafPlan]))(idGen)
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def pointDistanceNodeIndexSeek(
    node: String,
    labelName: String,
    property: String,
    point: String,
    distance: Double,
    getValue: GetValueFromIndexBehavior = DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    inclusive: Boolean = false,
    argumentIds: Set[String] = Set.empty,
    indexType: IndexType = IndexType.POINT
  ): IMPL = {
    pointDistanceNodeIndexSeekExpr(
      node,
      labelName,
      property,
      point,
      literalFloat(distance),
      getValue,
      indexOrder,
      inclusive,
      argumentIds,
      indexType
    )
  }

  def pointBoundingBoxNodeIndexSeek(
    node: String,
    labelName: String,
    property: String,
    lowerLeft: String,
    upperRight: String,
    getValue: GetValueFromIndexBehavior = DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    argumentIds: Set[String] = Set.empty,
    indexType: IndexType = IndexType.POINT
  ): IMPL = {
    pointBoundingBoxNodeIndexSeekExpr(
      node,
      labelName,
      property,
      lowerLeft,
      upperRight,
      getValue,
      indexOrder,
      argumentIds,
      indexType
    )
  }

  def pointDistanceNodeIndexSeekExpr(
    node: String,
    labelName: String,
    property: String,
    point: String,
    distanceExpr: Expression,
    getValue: GetValueFromIndexBehavior = DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    inclusive: Boolean = false,
    argumentIds: Set[String] = Set.empty,
    indexType: IndexType = IndexType.POINT
  ): IMPL = {
    val label = resolver.getLabelId(labelName)

    val propId = resolver.getPropertyKeyId(property)
    val planBuilder = (idGen: IdGen) => {
      val labelToken = LabelToken(labelName, LabelId(label))
      val propToken = PropertyKeyToken(PropertyKeyName(property)(NONE), PropertyKeyId(propId))
      val indexedProperty = IndexedProperty(propToken, getValue, NODE_TYPE)
      val e =
        RangeQueryExpression(PointDistanceSeekRangeWrapper(
          PointDistanceRange(function("point", parseExpression(point)), distanceExpr, inclusive)
        )(NONE))
      val plan = NodeIndexSeek(
        varFor(node),
        labelToken,
        Seq(indexedProperty),
        e,
        argumentIds.map(varFor),
        indexOrder,
        indexType,
        supportPartitionedScan = false
      )(idGen)
      newNode(plan.idName)
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def pointBoundingBoxNodeIndexSeekExpr(
    node: String,
    labelName: String,
    property: String,
    lowerLeft: String,
    upperRight: String,
    getValue: GetValueFromIndexBehavior = DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    argumentIds: Set[String] = Set.empty,
    indexType: IndexType = IndexType.POINT
  ): IMPL = {
    val label = resolver.getLabelId(labelName)

    val propId = resolver.getPropertyKeyId(property)
    val planBuilder = (idGen: IdGen) => {
      val labelToken = LabelToken(labelName, LabelId(label))
      val propToken = PropertyKeyToken(PropertyKeyName(property)(NONE), PropertyKeyId(propId))
      val indexedProperty = IndexedProperty(propToken, getValue, NODE_TYPE)
      val e =
        RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(
          PointBoundingBoxRange(
            function("point", parseExpression(lowerLeft)),
            function("point", parseExpression(upperRight))
          )
        )(NONE))
      val plan = NodeIndexSeek(
        varFor(node),
        labelToken,
        Seq(indexedProperty),
        e,
        argumentIds.map(varFor),
        indexOrder,
        indexType,
        supportPartitionedScan = false
      )(idGen)
      newNode(plan.idName)
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def pointDistanceRelationshipIndexSeek(
    rel: String,
    start: String,
    end: String,
    typeName: String,
    property: String,
    point: String,
    distance: Double,
    directed: Boolean = true,
    inclusive: Boolean = false,
    getValue: GetValueFromIndexBehavior = DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    argumentIds: Set[String] = Set.empty,
    indexType: IndexType = IndexType.POINT
  ): IMPL = {
    pointDistanceRelationshipIndexSeekExpr(
      rel,
      start,
      end,
      typeName,
      property,
      point,
      literalFloat(distance),
      directed,
      inclusive,
      getValue,
      indexOrder,
      argumentIds,
      indexType
    )
  }

  def pointDistanceRelationshipIndexSeekExpr(
    relationship: String,
    startNode: String,
    endNode: String,
    typeName: String,
    property: String,
    point: String,
    distanceExpr: Expression,
    directed: Boolean = true,
    inclusive: Boolean = false,
    getValue: GetValueFromIndexBehavior = DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    argumentIds: Set[String] = Set.empty,
    indexType: IndexType = IndexType.POINT
  ): IMPL = {
    val typ = resolver.getRelTypeId(typeName)

    val propId = resolver.getPropertyKeyId(property)
    val planBuilder = (idGen: IdGen) => {
      val typeToken = RelationshipTypeToken(typeName, RelTypeId(typ))
      val propToken = PropertyKeyToken(PropertyKeyName(property)(NONE), PropertyKeyId(propId))
      val indexedProperty = IndexedProperty(propToken, getValue, RELATIONSHIP_TYPE)
      val e =
        RangeQueryExpression(PointDistanceSeekRangeWrapper(
          PointDistanceRange(function("point", parseExpression(point)), distanceExpr, inclusive)
        )(NONE))

      val plan =
        if (directed) {
          DirectedRelationshipIndexSeek(
            varFor(relationship),
            varFor(startNode),
            varFor(endNode),
            typeToken,
            Seq(indexedProperty),
            e,
            argumentIds.map(varFor),
            indexOrder,
            indexType,
            supportPartitionedScan = false
          )(idGen)
        } else {
          UndirectedRelationshipIndexSeek(
            varFor(relationship),
            varFor(startNode),
            varFor(endNode),
            typeToken,
            Seq(indexedProperty),
            e,
            argumentIds.map(varFor),
            indexOrder,
            indexType,
            supportPartitionedScan = false
          )(idGen)
        }
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def pointBoundingBoxRelationshipIndexSeek(
    rel: String,
    start: String,
    end: String,
    typeName: String,
    property: String,
    lowerLeft: String,
    upperRight: String,
    directed: Boolean = true,
    getValue: GetValueFromIndexBehavior = DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    argumentIds: Set[String] = Set.empty,
    indexType: IndexType = IndexType.POINT
  ): IMPL = {
    pointBoundingBoxRelationshipIndexSeekExpr(
      rel,
      start,
      end,
      typeName,
      property,
      lowerLeft,
      upperRight,
      directed,
      getValue,
      indexOrder,
      argumentIds,
      indexType
    )
  }

  def pointBoundingBoxRelationshipIndexSeekExpr(
    relationship: String,
    startNode: String,
    endNode: String,
    typeName: String,
    property: String,
    lowerLeft: String,
    upperRight: String,
    directed: Boolean = true,
    getValue: GetValueFromIndexBehavior = DoNotGetValue,
    indexOrder: IndexOrder = IndexOrderNone,
    argumentIds: Set[String] = Set.empty,
    indexType: IndexType = IndexType.POINT
  ): IMPL = {
    val typ = resolver.getRelTypeId(typeName)

    val propId = resolver.getPropertyKeyId(property)
    val planBuilder = (idGen: IdGen) => {
      val typeToken = RelationshipTypeToken(typeName, RelTypeId(typ))
      val propToken = PropertyKeyToken(PropertyKeyName(property)(NONE), PropertyKeyId(propId))
      val indexedProperty = IndexedProperty(propToken, getValue, RELATIONSHIP_TYPE)
      val e =
        RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(
          PointBoundingBoxRange(
            function("point", parseExpression(lowerLeft)),
            function("point", parseExpression(upperRight))
          )
        )(NONE))

      val plan =
        if (directed) {
          DirectedRelationshipIndexSeek(
            varFor(relationship),
            varFor(startNode),
            varFor(endNode),
            typeToken,
            Seq(indexedProperty),
            e,
            argumentIds.map(varFor),
            indexOrder,
            indexType,
            supportPartitionedScan = false
          )(idGen)
        } else {
          UndirectedRelationshipIndexSeek(
            varFor(relationship),
            varFor(startNode),
            varFor(endNode),
            typeToken,
            Seq(indexedProperty),
            e,
            argumentIds.map(varFor),
            indexOrder,
            indexType,
            supportPartitionedScan = false
          )(idGen)
        }
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def aggregation(groupingExpressions: Seq[String], aggregationExpression: Seq[String]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => {
      Aggregation(
        lp,
        toVarMap(Parser.parseProjections(groupingExpressions: _*)),
        parseAggregationProjections(aggregationExpression: _*)
      )(_)
    }))
  }

  def aggregation(
    groupingExpressions: Map[String, Expression],
    aggregationExpressions: Map[String, Expression]
  ): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => {
      Aggregation(lp, toVarMap(groupingExpressions), toVarMap(aggregationExpressions))(_)
    }))
  }

  def orderedAggregation(
    groupingExpressions: Seq[String],
    aggregationExpression: Seq[String],
    orderToLeverage: Seq[String]
  ): IMPL = {
    val order = orderToLeverage.map(parseExpression)
    appendAtCurrentIndent(UnaryOperator(lp =>
      OrderedAggregation(
        lp,
        toVarMap(Parser.parseProjections(groupingExpressions: _*)),
        parseAggregationProjections(aggregationExpression: _*),
        order
      )(_)
    ))
  }

  def orderedAggregation(
    groupingExpressions: Map[String, Expression],
    aggregationExpressions: Map[String, Expression],
    orderToLeverage: Seq[String]
  ): IMPL = {
    val order = orderToLeverage.map(parseExpression)
    appendAtCurrentIndent(UnaryOperator(lp => {
      OrderedAggregation(lp, toVarMap(groupingExpressions), toVarMap(aggregationExpressions), order)(_)
    }))
  }

  def apply(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => Apply(lhs, rhs)(_)))

  def antiSemiApply(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => AntiSemiApply(lhs, rhs)(_)))

  def semiApply(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => SemiApply(lhs, rhs)(_)))

  def letAntiSemiApply(item: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => LetAntiSemiApply(lhs, rhs, varFor(item))(_)))

  def letSemiApply(item: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => LetSemiApply(lhs, rhs, varFor(item))(_)))

  def conditionalApply(items: String*): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => ConditionalApply(lhs, rhs, items.map(varFor))(_)))

  def antiConditionalApply(items: String*): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => AntiConditionalApply(lhs, rhs, items.map(varFor))(_)))

  def selectOrSemiApply(predicateString: String): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => {
      SelectOrSemiApply(lhs, rhs, parseExpression(predicateString))(_)
    }))
  }

  def selectOrSemiApply(predicate: Expression): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => SelectOrSemiApply(lhs, rhs, predicate)(_)))

  def selectOrAntiSemiApply(predicateString: String): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => {
      SelectOrAntiSemiApply(lhs, rhs, parseExpression(predicateString))(_)
    }))
  }

  def letSelectOrSemiApply(idName: String, predicateString: String): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => {
      LetSelectOrSemiApply(lhs, rhs, varFor(idName), parseExpression(predicateString))(_)
    }))
  }

  def letSelectOrAntiSemiApply(idName: String, predicateString: String): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => {
      LetSelectOrAntiSemiApply(lhs, rhs, varFor(idName), parseExpression(predicateString))(_)
    }))
  }

  def rollUpApply(collectionName: String, variableToCollect: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      RollUpApply(lhs, rhs, varFor(collectionName), varFor(variableToCollect))(_)
    ))

  def foreachApply(variable: String, expression: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      ForeachApply(lhs, rhs, varFor(variable), parseExpression(expression))(_)
    ))

  def foreachApply(variable: String, expression: Expression): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      ForeachApply(lhs, rhs, varFor(variable), expression)(_)
    ))

  def foreach(variable: String, expression: String, mutations: Seq[SimpleMutatingPattern]): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => Foreach(lp, varFor(variable), parseExpression(expression), mutations)(_)))

  def subqueryForeach(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => SubqueryForeach(lhs, rhs)(_)))

  def cartesianProduct(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => CartesianProduct(lhs, rhs)(_)))

  def union(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => Union(lhs, rhs)(_)))

  def assertSameNode(node: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => AssertSameNode(varFor(node), lhs, rhs)(_)))

  def assertSameRelationship(idName: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => AssertSameRelationship(varFor(idName), lhs, rhs)(_)))

  def orderedUnion(sortedOn: String*): IMPL =
    orderedUnionColumns(Parser.parseSort(sortedOn))

  def orderedUnionColumns(sortedOn: Seq[ColumnOrder]): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => OrderedUnion(lhs, rhs, sortedOn)(_)))

  def expandAll(pattern: String): IMPL = expand(pattern, ExpandAll)

  def nonFuseable(): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => NonFuseable(lp)(_)))

  def nonPipelined(): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => NonPipelined(lp)(_)))

  def nonPipelinedStreaming(expandFactor: Long = 1L): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => NonPipelinedStreaming(lp, expandFactor)(_)))

  def prober(probe: Prober.Probe): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => Prober(lp, probe)(_)))

  def cacheProperties(properties: String*): IMPL = {
    cacheProperties(properties.map(parseExpression(_).asInstanceOf[LogicalProperty]).toSet)
  }

  def cacheProperties(properties: Set[LogicalProperty]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => CacheProperties(source, properties)(_)))
  }

  def remoteBatchProperties(properties: String*): IMPL = {
    remoteBatchProperties(properties.map(parseExpression(_).asInstanceOf[LogicalProperty]).toSet)
  }

  def remoteBatchProperties(properties: Set[LogicalProperty]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => RemoteBatchProperties(source, properties)(_)))
  }

  def setProperty(entity: String, propertyKey: String, value: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetProperty(source, parseExpression(entity), PropertyKeyName(propertyKey)(pos), parseExpression(value))(_)
    ))
  }

  def setProperty(entity: Expression, propertyKey: String, value: Expression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetProperty(source, entity, PropertyKeyName(propertyKey)(pos), value)(_)
    ))
  }

  def setNodeProperty(node: String, propertyKey: String, value: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetNodeProperty(source, varFor(node), PropertyKeyName(propertyKey)(pos), parseExpression(value))(_)
    ))
  }

  def setNodeProperty(node: String, propertyKey: String, value: Expression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetNodeProperty(source, varFor(node), PropertyKeyName(propertyKey)(pos), value)(_)
    ))
  }

  def setRelationshipProperty(relationship: String, propertyKey: String, value: String): IMPL = {
    appendAtCurrentIndent(
      UnaryOperator(source =>
        SetRelationshipProperty(
          source,
          varFor(relationship),
          PropertyKeyName(propertyKey)(pos),
          parseExpression(value)
        )(_)
      )
    )
  }

  def setRelationshipProperty(relationship: String, propertyKey: String, value: Expression): IMPL = {
    appendAtCurrentIndent(
      UnaryOperator(source =>
        SetRelationshipProperty(
          source,
          varFor(relationship),
          PropertyKeyName(propertyKey)(pos),
          value
        )(_)
      )
    )
  }

  def setProperties(entity: String, items: (String, String)*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetProperties(
        source,
        parseExpression(entity),
        items.map(item => (PropertyKeyName(item._1)(pos), parseExpression(item._2)))
      )(_)
    ))
  }

  def setPropertiesExpression(entity: Expression, items: (String, Expression)*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetProperties(
        source,
        entity,
        items.map(item => (PropertyKeyName(item._1)(pos), item._2))
      )(_)
    ))
  }

  def setNodeProperties(node: String, items: (String, String)*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetNodeProperties(
        source,
        varFor(node),
        items.map(item => (PropertyKeyName(item._1)(pos), parseExpression(item._2)))
      )(_)
    ))
  }

  def setNodePropertiesExpression(node: String, items: (String, Expression)*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetNodeProperties(
        source,
        varFor(node),
        items.map(item => (PropertyKeyName(item._1)(pos), item._2))
      )(_)
    ))
  }

  def setRelationshipProperties(relationship: String, items: (String, String)*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetRelationshipProperties(
        source,
        varFor(relationship),
        items.map(item => (PropertyKeyName(item._1)(pos), parseExpression(item._2)))
      )(_)
    ))
  }

  def setRelationshipPropertiesExpression(relationship: String, items: (String, Expression)*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetRelationshipProperties(
        source,
        varFor(relationship),
        items.map(item => (PropertyKeyName(item._1)(pos), item._2))
      )(_)
    ))
  }

  def setPropertiesFromMap(entity: String, map: String, removeOtherProps: Boolean): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetPropertiesFromMap(source, parseExpression(entity), parseExpression(map), removeOtherProps)(_)
    ))
  }

  def setPropertiesFromMap(entity: String, map: Expression, removeOtherProps: Boolean): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetPropertiesFromMap(source, parseExpression(entity), map, removeOtherProps)(_)
    ))
  }

  def setNodePropertiesFromMap(node: String, map: String, removeOtherProps: Boolean): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetNodePropertiesFromMap(source, varFor(node), parseExpression(map), removeOtherProps)(_)
    ))
  }

  def setNodePropertiesFromMap(node: String, map: Expression, removeOtherProps: Boolean): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetNodePropertiesFromMap(source, varFor(node), map, removeOtherProps)(_)
    ))
  }

  def setRelationshipPropertiesFromMap(relationship: String, map: String, removeOtherProps: Boolean): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetRelationshipPropertiesFromMap(source, varFor(relationship), parseExpression(map), removeOtherProps)(_)
    ))
  }

  def setRelationshipPropertiesFromMap(relationship: String, map: Expression, removeOtherProps: Boolean): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetRelationshipPropertiesFromMap(source, varFor(relationship), map, removeOtherProps)(_)
    ))
  }

  def create(commands: CreateCommand*): IMPL = {
    commands.foreach {
      case node: CreateNode => newNode(VariableParser.unescaped(node.variable))
      case relationship: CreateRelationship =>
        newRelationship(VariableParser.unescaped(relationship.variable))
        newNode(VariableParser.unescaped(relationship.startNode))
        newNode(VariableParser.unescaped(relationship.endNode))
    }

    appendAtCurrentIndent(UnaryOperator(source => Create(source, commands)(_)))
  }

  def merge(
    nodes: Seq[CreateNode] = Seq.empty,
    relationships: Seq[CreateRelationship] = Seq.empty,
    onMatch: Seq[SetMutatingPattern] = Seq.empty,
    onCreate: Seq[SetMutatingPattern] = Seq.empty,
    lockNodes: Set[String] = Set.empty
  ): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      Merge(source, nodes, relationships, onMatch, onCreate, lockNodes.map(varFor))(_)
    ))
  }

  def nodeHashJoin(nodes: String*): IMPL = {
    appendAtCurrentIndent(BinaryOperator((left, right) => NodeHashJoin(nodes.map(varFor).toSet, left, right)(_)))
  }

  def rightOuterHashJoin(nodes: String*): IMPL = {
    appendAtCurrentIndent(BinaryOperator((left, right) => RightOuterHashJoin(nodes.map(varFor).toSet, left, right)(_)))
  }

  def leftOuterHashJoin(nodes: String*): IMPL = {
    appendAtCurrentIndent(BinaryOperator((left, right) => LeftOuterHashJoin(nodes.map(varFor).toSet, left, right)(_)))
  }

  def valueHashJoin(predicate: String): IMPL = {
    val expression = parseExpression(predicate)
    expression match {
      case e: Equals =>
        appendAtCurrentIndent(BinaryOperator((left, right) => ValueHashJoin(left, right, e)(_)))
      case _ => throw new IllegalArgumentException(s"can't join on $expression")
    }
  }

  def input(
    nodes: Seq[String] = Seq.empty,
    relationships: Seq[String] = Seq.empty,
    variables: Seq[String] = Seq.empty,
    nullable: Boolean = true
  ): IMPL = {
    if (indent != 0) {
      throw new IllegalStateException("The input operator has to be the left-most leaf of the plan")
    }
    if (
      nodes.toSet.size < nodes.size || relationships.toSet.size < relationships.size || variables.toSet.size < variables.size
    ) {
      throw new IllegalArgumentException("Input must create unique variables")
    }
    newNodes(nodes)
    newRelationships(relationships)
    newVariables(variables)
    appendAtCurrentIndent(LeafOperator(Input(
      nodes.map(varFor),
      relationships.map(varFor),
      variables.map(varFor),
      nullable
    )(_)))
  }

  def injectCompilationError(): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => {
      InjectCompilationError(lp)(_)
    }))
  }

  def filter(predicateStrings: String*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => {
      Selection(predicateStrings.map(parseExpression), lp)(_)
    }))
  }

  def simulatedFilter(selectivity: Double): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => {
      SimulatedSelection(lp, selectivity)(_)
    }))
  }

  def filterExpression(predicateExpressions: Expression*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp =>
      Selection(predicateExpressions.map(_.endoRewrite(expressionRewriter)), lp)(_)
    ))
  }

  def filterExpressionOrString(predicateExpressionsOrStrings: AnyRef*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => {
      val predicates = predicateExpressionsOrStrings.map {
        case s: String     => parseExpression(s)
        case e: Expression => e.endoRewrite(expressionRewriter)
        case other => throw new IllegalArgumentException(
            s"Expected Expression or String, got [${other.getClass.getSimpleName}] $other}"
          )
      }
      Selection(predicates, lp)(_)
    }))
  }

  def errorPlan(e: Exception): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => ErrorPlan(lp, e)(_)))
  }

  def nestedPlanCollectExpressionProjection(resultList: String, resultPart: String): IMPL = {
    val inner = parseExpression(resultPart)
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      // TODO Set.empty?
      Projection(
        lhs,
        Map(varFor(resultList) -> NestedPlanCollectExpression(rhs, inner, "collect(...)")(NONE))
      )(_)
    ))
  }

  def nestedPlanExistsExpressionProjection(resultList: String): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      Projection(lhs, Map(varFor(resultList) -> NestedPlanExistsExpression(rhs, "exists(...)")(NONE)))(_)
    ))
  }

  def nestedPlanGetByNameExpressionProjection(columnNameToGet: String, resultName: String): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      // TODO Set.empty?
      Projection(
        lhs,
        Map(varFor(resultName) -> NestedPlanGetByNameExpression(rhs, varFor(columnNameToGet), "getByName(...)")(NONE))
      )(_)
    ))
  }

  /**
   * The right-hand side is a nested plan of a NestedPlanGetByNameExpression that is placed inside of
   * the extract expression of a list comprehension on a literal list of strings.
   */
  def nestedPlanGetByNameExpressionInListComprehensionProjection(
    listElementVariable: String,
    list: Seq[String],
    columnNameToGet: String,
    resultName: String
  ): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => {
      val nestedPlanExpression = NestedPlanGetByNameExpression(rhs, varFor(columnNameToGet), "getByName(...)")(NONE)
      val literalList = ListLiteral(list.map(StringLiteral(_)(NONE.withInputLength(0))))(NONE)
      val listComprehension =
        ListComprehension(varFor(listElementVariable), literalList, None, Some(nestedPlanExpression))(NONE)
      Projection(
        lhs,
        Map(varFor(resultName) -> listComprehension)
      )(_)
    }))
  }

  def triadicSelection(positivePredicate: Boolean, sourceId: String, seenId: String, targetId: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      TriadicSelection(lhs, rhs, positivePredicate, varFor(sourceId), varFor(seenId), varFor(targetId))(_)
    ))

  def triadicBuild(triadicSelectionId: Int, sourceId: String, seenId: String): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp =>
      TriadicBuild(lp, varFor(sourceId), varFor(seenId), Some(Id(triadicSelectionId)))(_)
    ))

  def triadicFilter(triadicSelectionId: Int, positivePredicate: Boolean, sourceId: String, targetId: String): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp =>
      TriadicFilter(lp, positivePredicate, varFor(sourceId), varFor(targetId), Some(Id(triadicSelectionId)))(_)
    ))

  def loadCSV(url: String, variableName: String, format: CSVFormat, fieldTerminator: Option[String] = None): IMPL = {
    val urlExpr = parseExpression(url)
    appendAtCurrentIndent(UnaryOperator(lp =>
      LoadCSV(
        lp,
        urlExpr,
        varFor(variableName),
        format,
        fieldTerminator,
        legacyCsvQuoteEscaping = GraphDatabaseSettings.csv_legacy_quote_escaping.defaultValue(),
        csvBufferSize = GraphDatabaseSettings.csv_buffer_size.defaultValue().toInt
      )(_)
    ))
  }

  def injectValue(variable: String, value: String): IMPL = {
    val collection = s"${variable}Collection"
    val level = indent
    unwind(s"$collection AS $variable")
    indent = level
    projection(s"$collection + [$value] AS $collection")
    indent = level
    aggregation(Seq(), Seq(s"collect($variable) AS $collection"))
  }

  def transactionForeach(
    batchSize: Long = TransactionForeach.defaultBatchSize,
    concurrency: TransactionConcurrency = TransactionConcurrency.Serial,
    onErrorBehaviour: InTransactionsOnErrorBehaviour = OnErrorFail,
    maybeReportAs: Option[String] = None
  ): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      TransactionForeach(
        lhs,
        rhs,
        literalInt(batchSize),
        concurrency,
        onErrorBehaviour,
        maybeReportAs.map(varFor)
      )(_)
    ))

  def transactionApply(
    batchSize: Long = TransactionForeach.defaultBatchSize,
    concurrency: TransactionConcurrency = TransactionConcurrency.Serial,
    onErrorBehaviour: InTransactionsOnErrorBehaviour = OnErrorFail,
    maybeReportAs: Option[String] = None
  ): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      TransactionApply(
        lhs,
        rhs,
        literalInt(batchSize),
        concurrency,
        onErrorBehaviour,
        maybeReportAs.map(varFor)
      )(_)
    ))

  def trail(
    trailParameters: TrailParameters
  ): IMPL = {
    // This one comes in as an argument, so we need to declare it as a node here
    newNode(varFor(trailParameters.innerStart))
    // This is the node we "expand-to" , so we need to declare it as a node here
    newNode(varFor(trailParameters.end))

    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      Trail(
        lhs,
        rhs,
        Repetition(trailParameters.min, trailParameters.max),
        varFor(trailParameters.start),
        varFor(trailParameters.end),
        varFor(trailParameters.innerStart),
        varFor(trailParameters.innerEnd),
        trailParameters.groupNodes.map { case (inner, outer) => VariableGrouping(varFor(inner), varFor(outer))(pos) },
        trailParameters.groupRelationships.map { case (inner, outer) =>
          VariableGrouping(varFor(inner), varFor(outer))(pos)
        },
        trailParameters.innerRelationships.map(varFor),
        trailParameters.previouslyBoundRelationships.map(varFor),
        trailParameters.previouslyBoundRelationshipGroups.map(varFor),
        trailParameters.reverseGroupVariableProjections
      )(_)
    ))
  }

  def bidirectionalRepeatTrail(
    trailParameters: TrailParameters
  ): IMPL = {
    // These come in as arguments, so we need to declare them as nodes here
    newNode(varFor(trailParameters.innerStart))
    newNode(varFor(trailParameters.innerEnd))

    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      rhs match {
        case repeatOptions: RepeatOptions =>
          BidirectionalRepeatTrail(
            lhs,
            repeatOptions,
            Repetition(trailParameters.min, trailParameters.max),
            varFor(trailParameters.start),
            varFor(trailParameters.end),
            varFor(trailParameters.innerStart),
            varFor(trailParameters.innerEnd),
            trailParameters.groupNodes.map { case (inner, outer) =>
              VariableGrouping(varFor(inner), varFor(outer))(pos)
            },
            trailParameters.groupRelationships.map { case (inner, outer) =>
              VariableGrouping(varFor(inner), varFor(outer))(pos)
            },
            trailParameters.innerRelationships.map(varFor),
            trailParameters.previouslyBoundRelationships.map(varFor),
            trailParameters.previouslyBoundRelationshipGroups.map(varFor),
            trailParameters.reverseGroupVariableProjections
          )(_)
        case _ => throw new IllegalArgumentException("BidirectionalRepeatTrail must have RepeatOptions as its RHS.")
      }
    ))
  }

  def repeatOptions(): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) =>
      RepeatOptions(lhs, rhs)(_)
    ))
  }

  // SHIP IT

  protected def buildLogicalPlan(): LogicalPlan = tree.build()

  def getSemanticTable: SemanticTable = semanticTable

  /**
   * Called every time a new node is introduced by some logical operator.
   */
  def newNode(node: LogicalVariable): Unit = {
    semanticTable = semanticTable.addNode(node.asInstanceOf[Variable])
  }

  /**
   * Called every time a new relationship is introduced by some logical operator.
   */
  def newRelationship(relationship: LogicalVariable): Unit = {
    semanticTable = semanticTable.addRelationship(relationship.asInstanceOf[Variable])
  }

  /**
   * Called every time a new variable is introduced by some logical operator.
   */
  def newVariable(variable: Variable): Unit = {
    semanticTable = semanticTable.addTypeInfoCTAny(variable)
  }

  /**
   * Called if a variable needs to have a specific type
   */
  def newVariable(variable: Variable, typ: CypherType): Unit = {
    semanticTable = semanticTable.addTypeInfo(variable, typ.invariant)
  }

  /**
   * Called if a variable needs to have a specific type
   */
  def newVariable(variable: Variable, typ: TypeSpec): Unit = {
    semanticTable = semanticTable.addTypeInfo(variable, typ)
  }

  protected def newAlias(variable: LogicalVariable, expression: Expression): Unit = {
    if (!semanticTable.types.contains(variable)) {
      val typeInfo = semanticTable.types.get(expression).orElse(findTypeIgnoringPosition(expression))
      val spec = typeInfo.map(_.actual).getOrElse(CTAny.invariant)
      semanticTable = semanticTable.addTypeInfo(variable, spec)
    }
  }

  private def findTypeIgnoringPosition(expr: Expression) =
    semanticTable.types.iterator
      .map { case (k, v) => (k.node, v) } // unwrap nodes to discard position
      .collectFirst { case (`expr`, t) => t }

  protected def newNodes(nodes: Iterable[String]): Seq[LogicalVariable] = {
    val result = nodes.map(varFor).toSeq
    result.foreach(newNode)
    result
  }

  protected def newRelationships(relationships: Iterable[String]): Seq[LogicalVariable] = {
    val result = relationships.map(varFor).toSeq
    result.foreach(newRelationship)
    result
  }

  protected def newVariables(variables: Iterable[String]): Seq[LogicalVariable] = {
    val result = variables.map(varFor).toSeq
    result.foreach(newVariable)
    result
  }

  private val hasLabelsAndHasTypeNormalizer = new HasLabelsAndHasTypeNormalizer {
    override def isNode(expr: Expression): Boolean = semanticTable.typeFor(expr).is(CTNode)

    override def isRelationship(expr: Expression): Boolean = semanticTable.typeFor(expr).is(CTRelationship)
  }

  protected def expressionRewriter: Rewriter =
    inSequence(hasLabelsAndHasTypeNormalizer, combineHasLabels, desugarMapProjection.instance)

  /**
   * Returns the finalized output of the builder.
   */
  protected def build(readOnly: Boolean = true): T

  // HELPERS
  private def parseExpression(expression: String): Expression = {
    (Parser.parseExpression(expression) match {
      case f: FunctionInvocation if f.needsToBeResolved =>
        ResolvedFunctionInvocation(resolver.functionSignature)(f).coerceArguments
      case e => e
    }).endoRewrite(expressionRewriter)
  }

  private def parseProjections(projections: String*): Map[LogicalVariable, Expression] = {
    toVarMap(Parser.parseProjections(projections: _*)).view.mapValues {
      case f: FunctionInvocation if f.needsToBeResolved =>
        ResolvedFunctionInvocation(resolver.functionSignature)(f).coerceArguments
      case e => e
    }.toMap
  }

  private def parseAggregationProjections(projections: String*): Map[LogicalVariable, Expression] = {
    toVarMap(Parser.parseAggregationProjections(projections: _*)).view.mapValues {
      case f: FunctionInvocation if f.needsToBeResolved =>
        ResolvedFunctionInvocation(resolver.functionSignature)(f).coerceArguments
      case e => e
    }.toMap
  }

  protected def appendAtCurrentIndent(operatorBuilder: OperatorBuilder): IMPL = {
    if (tree == null) {
      if (wholePlan) {
        throw new IllegalStateException("Must call produceResult before adding other operators.")
      } else {
        tree = new Tree(operatorBuilder)
        looseEnds += tree
      }
    } else {
      val newTree = new Tree(operatorBuilder)

      def appendAtIndent(): Unit = {
        val parent = looseEnds(indent)
        parent.left = Some(newTree)
        looseEnds(indent) = newTree
      }

      indent - (looseEnds.size - 1) match {
        case 1 => // new rhs
          val parent = looseEnds.last
          parent.right = Some(newTree)
          looseEnds += newTree

        case 0 => // append to lhs
          appendAtIndent()

        case -1 => // end of rhs
          appendAtIndent()
          looseEnds.remove(looseEnds.size - 1)

        case _ =>
          throw new IllegalStateException("out of bounds")
      }
      indent = 0
    }
    self
  }

  // AST construction
  protected def varFor(name: String): Variable = Variable(name)(pos)
  private def labelName(s: String): LabelName = LabelName(s)(pos)
  private def relTypeName(s: String): RelTypeName = RelTypeName(s)(pos)

  private def literalInt(value: Long): SignedDecimalIntegerLiteral =
    SignedDecimalIntegerLiteral(value.toString)(pos)

  private def literalFloat(value: Double): DecimalDoubleLiteral =
    DecimalDoubleLiteral(value.toString)(pos)
  def literalString(str: String): StringLiteral = StringLiteral(str)(pos.withInputLength(0))

  def function(name: String, args: Expression*): FunctionInvocation =
    FunctionInvocation(FunctionName(name)(pos), distinct = false, args.toIndexedSeq)(pos)
}

object AbstractLogicalPlanBuilder {
  val pos: InputPosition = InputPosition.NONE

  case class Predicate(entity: String, predicate: String) {

    def asVariablePredicate: VariablePredicate =
      VariablePredicate(Variable(entity)(pos), Parser.parseExpression(predicate))
  }

  case class TrailParameters(
    min: Int,
    max: UpperBound,
    start: String,
    end: String,
    innerStart: String,
    innerEnd: String,
    groupNodes: Set[(String, String)],
    groupRelationships: Set[(String, String)],
    innerRelationships: Set[String],
    previouslyBoundRelationships: Set[String],
    previouslyBoundRelationshipGroups: Set[String],
    reverseGroupVariableProjections: Boolean
  )

  def createPattern(
    nodes: Seq[CreateNode] = Seq.empty,
    relationships: Seq[CreateRelationship] = Seq.empty
  ): CreatePattern = {
    CreatePattern(nodes ++ relationships)
  }

  def createNode(node: String, labels: String*): CreateNode =
    CreateNode(varFor(node), labels.map(LabelName(_)(pos)).toSet, None)

  def createNodeWithProperties(node: String, labels: Seq[String], properties: String): CreateNode =
    CreateNode(varFor(node), labels.map(LabelName(_)(pos)).toSet, Some(Parser.parseExpression(properties)))

  def createNodeWithProperties(node: String, labels: Seq[String], properties: MapExpression): CreateNode =
    CreateNode(varFor(node), labels.map(LabelName(_)(pos)).toSet, Some(properties))

  def createRelationship(
    relationship: String,
    left: String,
    typ: String,
    right: String,
    direction: SemanticDirection = OUTGOING,
    properties: Option[String] = None
  ): CreateRelationship = {
    val props = properties.map(Parser.parseExpression)
    if (props.exists(!_.isInstanceOf[MapExpression]))
      throw new IllegalArgumentException("Property must be a Map Expression")
    CreateRelationship(
      varFor(relationship),
      varFor(left),
      RelTypeName(typ)(pos),
      varFor(right),
      direction,
      props
    )
  }

  def createRelationshipExpression(
    relationship: String,
    left: String,
    typ: String,
    right: String,
    direction: SemanticDirection = OUTGOING,
    properties: Option[MapExpression] = None
  ): CreateRelationship = {
    CreateRelationship(
      varFor(relationship),
      varFor(left),
      RelTypeName(typ)(pos),
      varFor(right),
      direction,
      properties
    )
  }

  def setNodeProperty(node: String, key: String, value: String): SetMutatingPattern =
    SetNodePropertyPattern(varFor(node), PropertyKeyName(key)(InputPosition.NONE), Parser.parseExpression(value))

  def setNodeProperties(node: String, items: (String, String)*): SetMutatingPattern =
    SetNodePropertiesPattern(
      varFor(node),
      items.map(i => (PropertyKeyName(i._1)(InputPosition.NONE), Parser.parseExpression(i._2)))
    )

  def setNodePropertiesFromMap(node: String, map: String, removeOtherProps: Boolean = true): SetMutatingPattern =
    SetNodePropertiesFromMapPattern(varFor(node), Parser.parseExpression(map), removeOtherProps)

  def setRelationshipProperty(relationship: String, key: String, value: String): SetMutatingPattern =
    SetRelationshipPropertyPattern(
      varFor(relationship),
      PropertyKeyName(key)(InputPosition.NONE),
      Parser.parseExpression(value)
    )

  def setRelationshipProperties(rel: String, items: (String, String)*): SetMutatingPattern =
    SetRelationshipPropertiesPattern(
      varFor(rel),
      items.map(i => (PropertyKeyName(i._1)(InputPosition.NONE), Parser.parseExpression(i._2)))
    )

  def setRelationshipPropertiesFromMap(
    node: String,
    map: String,
    removeOtherProps: Boolean = true
  ): SetMutatingPattern =
    SetRelationshipPropertiesFromMapPattern(varFor(node), Parser.parseExpression(map), removeOtherProps)

  def setProperty(entity: String, key: String, value: String): SetMutatingPattern =
    SetPropertyPattern(
      Parser.parseExpression(entity),
      PropertyKeyName(key)(InputPosition.NONE),
      Parser.parseExpression(value)
    )

  def setProperties(entity: String, items: (String, String)*): SetMutatingPattern =
    SetPropertiesPattern(
      Parser.parseExpression(entity),
      items.map(i => (PropertyKeyName(i._1)(InputPosition.NONE), Parser.parseExpression(i._2)))
    )

  def setPropertyFromMap(entity: String, map: String, removeOtherProps: Boolean = true): SetMutatingPattern =
    SetPropertiesFromMapPattern(Parser.parseExpression(entity), Parser.parseExpression(map), removeOtherProps)

  def setLabel(node: String, labels: String*): SetMutatingPattern =
    SetLabelPattern(varFor(node), labels.map(l => LabelName(l)(InputPosition.NONE)))

  def removeLabel(node: String, labels: String*): RemoveLabelPattern =
    RemoveLabelPattern(varFor(node), labels.map(l => LabelName(l)(InputPosition.NONE)))

  def delete(entity: String, forced: Boolean = false): org.neo4j.cypher.internal.ir.DeleteExpression =
    org.neo4j.cypher.internal.ir.DeleteExpression(Parser.parseExpression(entity), forced)

  def andsReorderable(predicateExpressionsOrStrings: AnyRef*): AndsReorderable = {
    val predicates = predicateExpressionsOrStrings.map {
      case s: String     => Parser.parseExpression(s)
      case e: Expression => e
      case other => throw new IllegalArgumentException(
          s"Expected Expression or String, got [${other.getClass.getSimpleName}] $other}"
        )
    }
    AndsReorderable(ListSet.from(predicates))(pos)
  }
}
