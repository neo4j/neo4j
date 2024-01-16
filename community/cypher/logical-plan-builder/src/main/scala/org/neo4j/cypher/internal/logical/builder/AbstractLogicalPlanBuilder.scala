/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.logical.builder

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.RelationshipTypeToken
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.ShortestPaths
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir
import org.neo4j.cypher.internal.ir.CSVFormat
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
import org.neo4j.cypher.internal.ir.ShortestPathPattern
import org.neo4j.cypher.internal.ir.SimpleMutatingPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.pos
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Anti
import org.neo4j.cypher.internal.logical.plans.AntiConditionalApply
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.AssertSameNode
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
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.ExhaustiveLimit
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.ExpansionMode
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.Foreach
import org.neo4j.cypher.internal.logical.plans.ForeachApply
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexSeek
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.InjectCompilationError
import org.neo4j.cypher.internal.logical.plans.Input
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
import org.neo4j.cypher.internal.logical.plans.NestedPlanCollectExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanExistsExpression
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeekLeafPlan
import org.neo4j.cypher.internal.logical.plans.NonFuseable
import org.neo4j.cypher.internal.logical.plans.NonPipelined
import org.neo4j.cypher.internal.logical.plans.NonPipelinedHead
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.OrderedUnion
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxRange
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PointDistanceRange
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.logical.plans.ProcedureCall
import org.neo4j.cypher.internal.logical.plans.ProcedureSignature
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.logical.plans.ProjectEndpoints
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.logical.plans.PruningVarExpand
import org.neo4j.cypher.internal.logical.plans.QualifiedName
import org.neo4j.cypher.internal.logical.plans.QueryExpression
import org.neo4j.cypher.internal.logical.plans.RangeQueryExpression
import org.neo4j.cypher.internal.logical.plans.RelationshipCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.RelationshipIndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.RemoveLabels
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
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
import org.neo4j.cypher.internal.logical.plans.SingleSeekableArg
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.SubqueryForeach
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.Top1WithTies
import org.neo4j.cypher.internal.logical.plans.TransactionApply
import org.neo4j.cypher.internal.logical.plans.TransactionForeach
import org.neo4j.cypher.internal.logical.plans.TriadicBuild
import org.neo4j.cypher.internal.logical.plans.TriadicFilter
import org.neo4j.cypher.internal.logical.plans.TriadicSelection
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipIndexSeek
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipTypeScan
import org.neo4j.cypher.internal.logical.plans.Union
import org.neo4j.cypher.internal.logical.plans.UnwindCollection
import org.neo4j.cypher.internal.logical.plans.UserFunctionSignature
import org.neo4j.cypher.internal.logical.plans.ValueHashJoin
import org.neo4j.cypher.internal.logical.plans.VarExpand
import org.neo4j.cypher.internal.logical.plans.VariablePredicate
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.InputPosition.NONE
import org.neo4j.cypher.internal.util.LabelId
import org.neo4j.cypher.internal.util.PropertyKeyId
import org.neo4j.cypher.internal.util.RelTypeId
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.topDown
import org.neo4j.graphdb.schema.IndexType

import scala.collection.GenTraversableOnce
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
abstract class AbstractLogicalPlanBuilder[T, IMPL <: AbstractLogicalPlanBuilder[T, IMPL]](protected val resolver: Resolver, wholePlan: Boolean = true) {

  self: IMPL =>

  val patternParser = new PatternParser
  protected var semanticTable = new SemanticTable()

  protected sealed trait OperatorBuilder
  protected case class LeafOperator(planToIdConstructor: IdGen => LogicalPlan) extends OperatorBuilder{
    private val id = idGen.id()
    _idOfLastPlan = id
    val planConstructor: Unit => LogicalPlan = _ => planToIdConstructor(SameId(id))
  }
  protected case class UnaryOperator(planToIdConstructor: LogicalPlan => IdGen => LogicalPlan) extends OperatorBuilder {
    private val id = idGen.id()
    _idOfLastPlan = id
    val planConstructor: LogicalPlan => LogicalPlan = planToIdConstructor(_)(SameId(id))
  }
  protected case class BinaryOperator(planToIdConstructor: (LogicalPlan, LogicalPlan) => IdGen => LogicalPlan) extends OperatorBuilder {
    private val id = idGen.id()
    _idOfLastPlan = id
    val planConstructor: (LogicalPlan, LogicalPlan) => LogicalPlan = planToIdConstructor(_, _)(SameId(id))
  }

  protected class Tree(operator: OperatorBuilder) {
    private var _left: Option[Tree] = None
    private var _right: Option[Tree] = None

    def left: Option[Tree] = _left
    def left_=(newVal: Option[Tree]): Unit = {
      operator match {
        case _:LeafOperator =>
          throw new IllegalArgumentException(s"Cannot attach a LHS to a leaf plan.")
        case _ =>
      }
      _left = newVal
    }

    def right: Option[Tree] = _right
    def right_=(newVal: Option[Tree]): Unit = {
      operator match {
        case _:LeafOperator =>
          throw new IllegalArgumentException(s"Cannot attach a RHS to a leaf plan.")
        case _:UnaryOperator =>
          throw new IllegalArgumentException(s"Cannot attach a RHS to a unary plan.")
        case _ =>
      }
      _right = newVal
    }

    def build(): LogicalPlan = {
      operator match {
        case o:LeafOperator =>
          o.planConstructor()
        case o:UnaryOperator =>
          o.planConstructor(left.get.build())
        case o:BinaryOperator =>
          o.planConstructor(left.get.build(), right.get.build())
      }
    }
  }

  val idGen: IdGen = new SequentialIdGen()

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

  // OPERATORS

  def produceResults(vars: String*): IMPL = {
    resultColumns = vars.map(VariableParser.unescaped).toArray
    tree = new Tree(UnaryOperator(lp => ProduceResult(lp, resultColumns)(_)))
    looseEnds += tree
    self
  }

  def procedureCall(call: String, withFakedFullDeclarations: Boolean = false): IMPL = {
    val unresolvedCall = Parser.parseProcedureCall(call)
    appendAtCurrentIndent(UnaryOperator(lp => {
      val resolvedCall =
        ResolvedCall(resolver.procedureSignature)(unresolvedCall)
          .coerceArguments
      val rewrittenResolvedCall = if (withFakedFullDeclarations) resolvedCall.withFakedFullDeclarations else resolvedCall
      ProcedureCall(lp, rewrittenResolvedCall)(_)
    }))
    self
  }

  def optional(protectedSymbols: String*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Optional(lp, protectedSymbols.toSet)(_)))
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

  def expand(pattern: String,
             expandMode: ExpansionMode = ExpandAll,
             projectedDir: SemanticDirection = OUTGOING,
             nodePredicate: Predicate = AbstractLogicalPlanBuilder.NO_PREDICATE,
             relationshipPredicate: Predicate = AbstractLogicalPlanBuilder.NO_PREDICATE): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    if (expandMode == ExpandAll) {
      newNode(varFor(p.to))
    }

    p.length match {
      case SimplePatternLength =>
        appendAtCurrentIndent(UnaryOperator(lp => Expand(lp, p.from, p.dir, p.relTypes, p.to, p.relName, expandMode)(_)))
      case varPatternLength: VarPatternLength =>
        appendAtCurrentIndent(UnaryOperator(lp => VarExpand(lp,
                                                            p.from,
                                                            p.dir,
                                                            projectedDir,
                                                            p.relTypes,
                                                            p.to,
                                                            p.relName,
                                                            varPatternLength,
                                                            expandMode,
                                                            nodePredicate.asVariablePredicate,
                                                            relationshipPredicate.asVariablePredicate
                                                           )(_)))
    }
    self
  }

  def shortestPath(pattern: String,
                   pathName: Option[String] = None,
                   all: Boolean = false,
                   predicates: Seq[String] = Seq.empty,
                   withFallback: Boolean = false,
                   disallowSameNode: Boolean = true): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))

    val length = p.length match {
      case SimplePatternLength => None
      case VarPatternLength(min, max) => Some(Some(Range(Some(UnsignedDecimalIntegerLiteral(min.toString)(pos)), max.map(i => UnsignedDecimalIntegerLiteral(i.toString)(pos)))(pos)))
    }

    appendAtCurrentIndent(UnaryOperator(lp => FindShortestPaths(lp,
      ShortestPathPattern(pathName, PatternRelationship(p.relName, (p.from, p.to), p.dir, p.relTypes, p.length), !all)
      (ShortestPaths(RelationshipChain(
        NodePattern(Some(varFor(p.from)), Seq.empty, None, None)(pos), // labels, properties and predicates are not used at runtime
        RelationshipPattern(Some(varFor(p.relName)),
          p.relTypes,
          length,
          None, // properties are not used at runtime
          p.dir
        )(pos),
        NodePattern(Some(varFor(p.to)), Seq.empty, None, None)(pos) // labels, properties and predicates are not used at runtime
      )(pos), !all)(pos)),
      predicates.map(parseExpression),
      withFallback,
      disallowSameNode
    )(_)))
  }

  def pruningVarExpand(pattern: String,
                       nodePredicate: Predicate = AbstractLogicalPlanBuilder.NO_PREDICATE,
                       relationshipPredicate: Predicate = AbstractLogicalPlanBuilder.NO_PREDICATE): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    newNode(varFor(p.to))
    p.length match {
      case VarPatternLength(min, Some(max)) =>
        appendAtCurrentIndent(UnaryOperator(lp => PruningVarExpand(lp,
          p.from,
          p.dir,
          p.relTypes,
          p.to,
          min,
          max,
          nodePredicate.asVariablePredicate,
          relationshipPredicate.asVariablePredicate
        )(_)))
      case _ =>
        throw new IllegalArgumentException("This pattern is not compatible with pruning var expand")
    }
    self
  }

  def expandInto(pattern: String): IMPL = expand(pattern, ExpandInto)

  def optionalExpandAll(pattern: String,
                        predicate: Option[String] = None): IMPL =
    optionalExpandAll(
      patternParser.parse(pattern),
      predicate.map(parseExpression).map(p => Ands(Seq(p))(p.position)),
    )

  private def optionalExpandAll(pattern: PatternParser.Pattern,
                                predicate: Option[Expression]): IMPL = {
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
          OptionalExpand(lp, pattern.from, pattern.dir, pattern.relTypes, pattern.to, pattern.relName, ExpandAll, rewrittenPredicate)(_)))
      case _ =>
        throw new IllegalArgumentException("Cannot have optional expand with variable length pattern")
    }
    self
  }

  def optionalExpandInto(pattern: String, predicate: Option[String] = None): IMPL = {
    val p = patternParser.parse(pattern)
    p.length match {
      case SimplePatternLength =>
        val pred = predicate.map(parseExpression).map(p => Ands(Seq(p))(p.position))
        appendAtCurrentIndent(UnaryOperator(lp => OptionalExpand(lp, p.from, p.dir, p.relTypes, p.to, p.relName, ExpandInto, pred)(_)))
      case _ =>
        throw new IllegalArgumentException("Cannot have optional expand with variable length pattern")
    }
    self
  }

  def projectEndpoints(pattern: String, startInScope: Boolean, endInScope: Boolean): IMPL = {
    val p = patternParser.parse(pattern)
    val relTypesAsNonEmptyOption = if (p.relTypes.isEmpty) None else Some(p.relTypes)
    val directed = p.dir match {
      case SemanticDirection.INCOMING => throw new IllegalArgumentException("Please turn your pattern around. ProjectEndpoints does not accept INCOMING.")
      case SemanticDirection.OUTGOING => true
      case SemanticDirection.BOTH => false
    }
    newNode(varFor(p.from))
    newNode(varFor(p.to))
    appendAtCurrentIndent(UnaryOperator(lp => ProjectEndpoints(lp, p.relName, p.from, startInScope, p.to, endInScope,
                                                               relTypesAsNonEmptyOption, directed, p.length)(_)))
  }

  def partialSort(alreadySortedPrefix: Seq[ColumnOrder], stillToSortSuffix: Seq[ColumnOrder]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => PartialSort(lp, alreadySortedPrefix, stillToSortSuffix, None)(_)))
    self
  }

  def partialSort(alreadySortedPrefix: Seq[ColumnOrder], stillToSortSuffix: Seq[ColumnOrder], skipSortingPrefixLength: Long): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => PartialSort(lp, alreadySortedPrefix, stillToSortSuffix, Some(literalInt(skipSortingPrefixLength)))(_)))
    self
  }

  def sort(sortItems: Seq[ColumnOrder]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Sort(lp, sortItems)(_)))
    self
  }

  def top(sortItems: Seq[ColumnOrder], limit: Long): IMPL =
    top(sortItems, literalInt(limit))

  def top(sortItems: Seq[ColumnOrder], limitExpr: Expression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Top(lp, sortItems, limitExpr)(_)))
    self
  }

  def top1WithTies(sortItems: Seq[ColumnOrder]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Top1WithTies(lp, sortItems)(_)))
    self
  }

  def partialTop(alreadySortedPrefix: Seq[ColumnOrder], stillToSortSuffix: Seq[ColumnOrder], limit: Long): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => PartialTop(lp, alreadySortedPrefix, stillToSortSuffix, literalInt(limit), None)(_)))
    self
  }

  def partialTop(alreadySortedPrefix: Seq[ColumnOrder], stillToSortSuffix: Seq[ColumnOrder], limit: Long, skipSortingPrefixLength: Long): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => PartialTop(lp, alreadySortedPrefix, stillToSortSuffix, literalInt(limit), Some(literalInt(skipSortingPrefixLength)))(_)))
    self
  }
  def eager(reasons: Seq[EagernessReason.Reason] = Seq(EagernessReason.Unknown)): IMPL = {
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

  def detachDeleteNode(node: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DetachDeleteNode(lp, parseExpression(node))(_)))
    self
  }

  def deleteRelationship(rel: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DeleteRelationship(lp, parseExpression(rel))(_)))
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

  def detachDeleteExpression(expression: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DetachDeleteExpression(lp, parseExpression(expression))(_)))
    self
  }

  def setLabels(nodeVariable:String, labels: String*): IMPL = {
    val labelNames = labels.map(l => LabelName(l)(InputPosition.NONE))
    appendAtCurrentIndent(UnaryOperator(lp => SetLabels(lp, nodeVariable, labelNames)(_)))
  }

  def removeLabels(nodeVariable: String, labels: String*): IMPL = {
    val labelNames = labels.map(l => LabelName(l)(InputPosition.NONE))
    appendAtCurrentIndent(UnaryOperator(lp => RemoveLabels(lp, nodeVariable, labelNames)(_)))
  }

  def unwind(projectionString: String): IMPL = {
    val (name, expression) = Parser.parseProjections(projectionString).head
    appendAtCurrentIndent(UnaryOperator(lp => UnwindCollection(lp, name, expression)(_)))
    self
  }

  def projection(projectionStrings: String*): IMPL = {
    projection(Parser.parseProjections(projectionStrings: _*))
  }

  def projection(projectExpressions: Map[String, Expression]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => {
      val rewrittenProjections = projectExpressions.map { case (name, expr) => name -> rewriteExpression(expr) }
      projectExpressions.foreach { case (name, expr) => newAlias(varFor(name), expr) }
      Projection(lp, rewrittenProjections)(_)
    }))
    self
  }

  def distinct(projectionStrings: String*): IMPL = {
    val projections = Parser.parseProjections(projectionStrings: _*)
    appendAtCurrentIndent(UnaryOperator(lp => Distinct(lp, projections)(_)))
    self
  }

  def orderedDistinct(orderToLeverage: Seq[String], projectionStrings: String*): IMPL = {
    val order = orderToLeverage.map(parseExpression)
    val projections = Parser.parseProjections(projectionStrings: _*)
    appendAtCurrentIndent(UnaryOperator(lp => OrderedDistinct(lp, projections, order)(_)))
    self
  }

  def allNodeScan(node: String, args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(AllNodesScan(n, args.map(VariableParser.unescaped).toSet)(_)))
  }

  def argument(args: String*): IMPL = {
    appendAtCurrentIndent(LeafOperator(Argument(args.toSet)(_)))
  }

  def nodeByLabelScan(node: String, label: String, args: String*): IMPL =
    nodeByLabelScan(node, label, IndexOrderNone, args: _*)

  def nodeByLabelScan(node: String, label: String, indexOrder: IndexOrder, args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(NodeByLabelScan(n, labelName(label), args.map(VariableParser.unescaped).toSet, indexOrder)(_)))
  }

  def nodeByIdSeek(node: String, args: Set[String], ids: AnyVal*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    val idExpressions = ids.map {
      case x@(_:Long|_:Int) => SignedDecimalIntegerLiteral(x.toString)(pos)
      case x@(_:Float|_:Double) =>  DecimalDoubleLiteral(x.toString)(pos)
      case x => throw new IllegalArgumentException(s"$x is not a supported value for ID")
    }
    val input = ManySeekableArgs(ListLiteral(idExpressions)(pos))

    appendAtCurrentIndent(LeafOperator(NodeByIdSeek(n, input, args)(_)))
  }

  def directedRelationshipByIdSeek(relationship: String, from: String, to: String, args: Set[String], ids: AnyVal*): IMPL = {
    newRelationship(varFor(relationship))
    newNode(varFor(from))
    newNode(varFor(to))
    val idExpressions = ids.map {
      case x@(_:Long|_:Int) => SignedDecimalIntegerLiteral(x.toString)(pos)
      case x@(_:Float|_:Double) =>  DecimalDoubleLiteral(x.toString)(pos)
      case x => throw new IllegalArgumentException(s"$x is not a supported value for ID")
    }
    val input = ManySeekableArgs(ListLiteral(idExpressions)(pos))

    appendAtCurrentIndent(LeafOperator(DirectedRelationshipByIdSeek(relationship, input, from, to, args)(_)))
  }

  def undirectedRelationshipByIdSeek(relationship: String, from: String, to: String, args: Set[String], ids: AnyVal*): IMPL = {
    newRelationship(varFor(relationship))
    newNode(varFor(from))
    newNode(varFor(to))
    val idExpressions = ids.map {
      case x@(_:Long|_:Int) => SignedDecimalIntegerLiteral(x.toString)(pos)
      case x@(_:Float|_:Double) =>  DecimalDoubleLiteral(x.toString)(pos)
      case x => throw new IllegalArgumentException(s"$x is not a supported value for ID")
    }
    val input = ManySeekableArgs(ListLiteral(idExpressions)(pos))

    appendAtCurrentIndent(LeafOperator(UndirectedRelationshipByIdSeek(relationship, input, from, to, args)(_)))
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
    val typ = if (p.relTypes.size == 1) p.relTypes.head else throw new UnsupportedOperationException("Cannot do a scan with multiple types")

    p.dir match {
      case SemanticDirection.OUTGOING =>
        appendAtCurrentIndent(LeafOperator(DirectedRelationshipTypeScan(p.relName, p.from, typ, p.to, args.toSet, indexOrder)(_)))
      case SemanticDirection.INCOMING =>
        appendAtCurrentIndent(LeafOperator(DirectedRelationshipTypeScan(p.relName, p.to, typ, p.from, args.toSet, indexOrder)(_)))
      case SemanticDirection.BOTH =>
        appendAtCurrentIndent(LeafOperator(UndirectedRelationshipTypeScan(p.relName, p.from, typ, p.to, args.toSet, indexOrder)(_)))
    }
  }

  def nodeCountFromCountStore(node: String, labels: Seq[Option[String]], args: String*): IMPL = {
    val labelNames = labels.map(maybeLabel => maybeLabel.map(labelName)).toList
    appendAtCurrentIndent(LeafOperator(NodeCountFromCountStore(node, labelNames, args.map(VariableParser.unescaped).toSet)(_)))
  }

  def relationshipCountFromCountStore(name: String, maybeStartLabel: Option[String], relTypes: Seq[String], maybeEndLabel: Option[String], args: String*): IMPL = {
    val startLabel = maybeStartLabel.map(labelName)
    val relTypeNames = relTypes.map(relTypeName)
    val endLabel = maybeEndLabel.map(labelName)
    appendAtCurrentIndent(LeafOperator(RelationshipCountFromCountStore(name, startLabel, relTypeNames, endLabel, args.map(VariableParser.unescaped).toSet)(_)))
  }

  def nodeIndexOperator(indexSeekString: String,
                        getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
                        indexOrder: IndexOrder = IndexOrderNone,
                        paramExpr: GenTraversableOnce[Expression] = None,
                        argumentIds: Set[String] = Set.empty,
                        unique: Boolean = false,
                        customQueryExpression: Option[QueryExpression[Expression]] = None,
                        indexType: IndexType = IndexType.BTREE): IMPL = {
    val planBuilder = (idGen: IdGen) => {
      val plan = nodeIndexSeek(indexSeekString, getValue, indexOrder, paramExpr.seq.toSeq, argumentIds, unique, customQueryExpression, indexType)(idGen)
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def relationshipIndexOperator(indexSeekString: String,
                                getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
                                indexOrder: IndexOrder = IndexOrderNone,
                                paramExpr: Iterable[Expression] = Seq.empty,
                                argumentIds: Set[String] = Set.empty,
                                customQueryExpression: Option[QueryExpression[Expression]] = None,
                                indexType: IndexType = IndexType.BTREE): IMPL = {
    val planBuilder = (idGen: IdGen) => {
      val plan = relationshipIndexSeek(indexSeekString, getValue, indexOrder, paramExpr, argumentIds, customQueryExpression, indexType)(idGen)
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def nodeIndexSeek(indexSeekString: String,
                    getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
                    indexOrder: IndexOrder = IndexOrderNone,
                    paramExpr: Iterable[Expression] = Seq.empty,
                    argumentIds: Set[String] = Set.empty,
                    unique: Boolean = false,
                    customQueryExpression: Option[QueryExpression[Expression]] = None,
                    indexType: IndexType = IndexType.BTREE): IdGen => NodeIndexLeafPlan = {
    val label = resolver.getLabelId(IndexSeek.labelFromIndexSeekString(indexSeekString))
    val propIds: PartialFunction[String, Int] = {
      case x => resolver.getPropertyKeyId(x)
    }
    val planBuilder = (idGen: IdGen) => {
      val plan = IndexSeek.nodeIndexSeek(indexSeekString, getValue, indexOrder, paramExpr, argumentIds, Some(propIds), label, unique,
        customQueryExpression, indexType)(idGen)
      newNode(varFor(plan.idName))
      plan
    }
    planBuilder
  }

  def relationshipIndexSeek(indexSeekString: String,
                            getValue: String => GetValueFromIndexBehavior = _ => DoNotGetValue,
                            indexOrder: IndexOrder = IndexOrderNone,
                            paramExpr: Iterable[Expression] = Seq.empty,
                            argumentIds: Set[String] = Set.empty,
                            customQueryExpression: Option[QueryExpression[Expression]] = None,
                            indexType: IndexType = IndexType.BTREE): IdGen => RelationshipIndexLeafPlan = {
    val relType = resolver.getRelTypeId(IndexSeek.relTypeFromIndexSeekString(indexSeekString))
    val propIds: PartialFunction[String, Int] = {
      case x => resolver.getPropertyKeyId(x)
    }
    val planBuilder = (idGen: IdGen) => {
      val plan = IndexSeek.relationshipIndexSeek(indexSeekString, getValue, indexOrder, paramExpr, argumentIds, Some(propIds), relType,
        customQueryExpression, indexType)(idGen)
      newRelationship(varFor(plan.idName))
      newNode(varFor(plan.leftNode))
      newNode(varFor(plan.rightNode))
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

  def pointDistanceNodeIndexSeek(node: String,
                                 labelName: String,
                                 property: String,
                                 point: String,
                                 distance: Double,
                                 getValue: GetValueFromIndexBehavior = DoNotGetValue,
                                 indexOrder: IndexOrder = IndexOrderNone,
                                 inclusive: Boolean = false,
                                 argumentIds: Set[String] = Set.empty,
                                 indexType: IndexType = IndexType.BTREE): IMPL = {
    pointDistanceNodeIndexSeekExpr(node, labelName, property, point, literalFloat(distance), getValue, indexOrder, inclusive, argumentIds, indexType)
  }

  def pointBoundingBoxNodeIndexSeek(node: String,
                                    labelName: String,
                                    property: String,
                                    lowerLeft: String,
                                    upperRight: String,
                                    getValue: GetValueFromIndexBehavior = DoNotGetValue,
                                    indexOrder: IndexOrder = IndexOrderNone,
                                    argumentIds: Set[String] = Set.empty,
                                    indexType: IndexType = IndexType.BTREE): IMPL = {
    pointBoundingBoxNodeIndexSeekExpr(node, labelName, property, lowerLeft, upperRight, getValue, indexOrder, argumentIds, indexType)
  }

  def pointDistanceNodeIndexSeekExpr(node: String,
                                     labelName: String,
                                     property: String,
                                     point: String,
                                     distanceExpr: Expression,
                                     getValue: GetValueFromIndexBehavior = DoNotGetValue,
                                     indexOrder: IndexOrder = IndexOrderNone,
                                     inclusive: Boolean = false,
                                     argumentIds: Set[String] = Set.empty,
                                     indexType: IndexType = IndexType.BTREE): IMPL = {
    val label = resolver.getLabelId(labelName)

    val propId = resolver.getPropertyKeyId(property)
    val planBuilder = (idGen: IdGen) => {
      val labelToken = LabelToken(labelName, LabelId(label))
      val propToken = PropertyKeyToken(PropertyKeyName(property)(NONE), PropertyKeyId(propId))
      val indexedProperty = IndexedProperty(propToken, getValue, NODE_TYPE)
      val e =
        RangeQueryExpression(PointDistanceSeekRangeWrapper(
          PointDistanceRange(function("point", parseExpression(point)), distanceExpr, inclusive))(NONE))
      val plan = NodeIndexSeek(node,
                               labelToken,
                               Seq(indexedProperty),
                               e,
                               argumentIds,
                               indexOrder,
                               indexType)(idGen)
      newNode(varFor(plan.idName))
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def pointBoundingBoxNodeIndexSeekExpr(node: String,
                                        labelName: String,
                                        property: String,
                                        lowerLeft: String,
                                        upperRight: String,
                                        getValue: GetValueFromIndexBehavior = DoNotGetValue,
                                        indexOrder: IndexOrder = IndexOrderNone,
                                        argumentIds: Set[String] = Set.empty,
                                        indexType: IndexType = IndexType.BTREE): IMPL = {
    val label = resolver.getLabelId(labelName)

    val propId = resolver.getPropertyKeyId(property)
    val planBuilder = (idGen: IdGen) => {
      val labelToken = LabelToken(labelName, LabelId(label))
      val propToken = PropertyKeyToken(PropertyKeyName(property)(NONE), PropertyKeyId(propId))
      val indexedProperty = IndexedProperty(propToken, getValue, NODE_TYPE)
      val e =
        RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(
          PointBoundingBoxRange(function("point", parseExpression(lowerLeft)), function("point", parseExpression(upperRight))))(NONE))
      val plan = NodeIndexSeek(node,
        labelToken,
        Seq(indexedProperty),
        e,
        argumentIds,
        indexOrder,
        indexType)(idGen)
      newNode(varFor(plan.idName))
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def pointBoundingBoxRelationshipIndexSeek(rel: String,
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
                                            indexType: IndexType = IndexType.BTREE): IMPL = {
    pointBoundingBoxRelationshipIndexSeekExpr(rel, start, end, typeName, property, lowerLeft, upperRight, directed, getValue, indexOrder, argumentIds, indexType)
  }

  def pointBoundingBoxRelationshipIndexSeekExpr(relationship: String,
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
                                                indexType: IndexType = IndexType.BTREE): IMPL = {
    val typ = resolver.getRelTypeId(typeName)

    val propId = resolver.getPropertyKeyId(property)
    val planBuilder = (idGen: IdGen) => {
      val typeToken = RelationshipTypeToken(typeName, RelTypeId(typ))
      val propToken = PropertyKeyToken(PropertyKeyName(property)(NONE), PropertyKeyId(propId))
      val indexedProperty = IndexedProperty(propToken, getValue, NODE_TYPE)
      val e =
        RangeQueryExpression(PointBoundingBoxSeekRangeWrapper(
          PointBoundingBoxRange(function("point", parseExpression(lowerLeft)), function("point", parseExpression(upperRight))))(NONE))

      val plan =
        if (directed) {
          DirectedRelationshipIndexSeek(relationship,  startNode, endNode, typeToken,  Seq(indexedProperty), e, argumentIds, indexOrder, indexType)(idGen)
        } else {
          UndirectedRelationshipIndexSeek(relationship,  startNode, endNode, typeToken,  Seq(indexedProperty), e, argumentIds, indexOrder, indexType)(idGen)
        }
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def aggregation(groupingExpressions: Seq[String],
                  aggregationExpression: Seq[String]): IMPL = {
    val expressions = Parser.parseProjections(aggregationExpression: _*).mapValues {
      case f: FunctionInvocation if f.needsToBeResolved =>
        ResolvedFunctionInvocation(resolver.functionSignature)(f).coerceArguments
      case e => e
    }

    appendAtCurrentIndent(UnaryOperator(lp => {
      Aggregation(lp,
        Parser.parseProjections(groupingExpressions: _*), expressions)(_)
    }))
  }

  def orderedAggregation(groupingExpressions: Seq[String],
                         aggregationExpression: Seq[String],
                         orderToLeverage: Seq[String]): IMPL = {
    val order = orderToLeverage.map(parseExpression)
    appendAtCurrentIndent(UnaryOperator(lp => OrderedAggregation(lp,
      Parser.parseProjections(groupingExpressions: _*),
      Parser.parseProjections(aggregationExpression: _*),
      order)(_)))
  }

  def apply(fromSubquery: Boolean = false): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => Apply(lhs, rhs, fromSubquery)(_)))

  def antiSemiApply(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => AntiSemiApply(lhs, rhs)(_)))

  def semiApply(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => SemiApply(lhs, rhs)(_)))

  def letAntiSemiApply(item: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => LetAntiSemiApply(lhs, rhs, item)(_)))

  def letSemiApply(item: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => LetSemiApply(lhs, rhs, item)(_)))

  def conditionalApply(items: String*): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => ConditionalApply(lhs, rhs, items)(_)))

  def antiConditionalApply(items: String*): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => AntiConditionalApply(lhs, rhs, items)(_)))

  def selectOrSemiApply(predicate: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => SelectOrSemiApply(lhs, rhs, parseExpression(predicate))(_)))

  def selectOrSemiApply(predicate: Expression): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => SelectOrSemiApply(lhs, rhs, predicate)(_)))

  def selectOrAntiSemiApply(predicate: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => SelectOrAntiSemiApply(lhs, rhs, parseExpression(predicate))(_)))

  def letSelectOrSemiApply(idName: String, predicate: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => LetSelectOrSemiApply(lhs, rhs, idName, parseExpression(predicate))(_)))

  def letSelectOrAntiSemiApply(idName: String, predicate: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => LetSelectOrAntiSemiApply(lhs, rhs, idName, parseExpression(predicate))(_)))

  def rollUpApply(collectionName: String,
                  variableToCollect: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => RollUpApply(lhs, rhs, collectionName, variableToCollect)(_)))

  def foreachApply(variable: String, expression: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => ForeachApply(lhs, rhs, variable, parseExpression(expression))(_)))

  def foreach(variable: String, expression: String, mutations: Seq[SimpleMutatingPattern]): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => Foreach(lp, variable, parseExpression(expression), mutations)(_)))

  def subqueryForeach(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => SubqueryForeach(lhs, rhs)(_)))

  def cartesianProduct(fromSubquery: Boolean = false): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => CartesianProduct(lhs, rhs, fromSubquery)(_)))

  def union(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => Union(lhs, rhs)(_)))

  def assertSameNode(node: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => AssertSameNode(node, lhs, rhs)(_)))

  def orderedUnion(sortedOn: Seq[ColumnOrder]): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => OrderedUnion(lhs, rhs, sortedOn)(_)))

  def expandAll(pattern: String): IMPL = expand(pattern, ExpandAll)

  def nonFuseable(): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => NonFuseable(lp)(_)))

  def nonPipelined(): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => NonPipelined(lp)(_)))

  def nonPipelinedHead(expandFactor: Long = 1L): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => NonPipelinedHead(lp, expandFactor)(_)))

  def prober(probe: Prober.Probe): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => Prober(lp, probe)(_)))

  def cacheProperties(properties: String*): IMPL = {
    cacheProperties(properties.map(parseExpression(_).asInstanceOf[LogicalProperty]).toSet)
  }

  def cacheProperties(properties: Set[LogicalProperty]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => CacheProperties(source, properties)(_)))
  }

  def setProperty(entity: String, propertyKey: String, value: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => SetProperty(source, parseExpression(entity), PropertyKeyName(propertyKey)(pos), parseExpression(value))(_)))
  }

  def setPropertyExpression(entity: Expression, propertyKey: String, value: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetProperty(source, entity, PropertyKeyName(propertyKey)(pos), parseExpression(value))(_)
    ))
  }

  def setNodeProperty(node: String, propertyKey: String, value: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => SetNodeProperty(source, node, PropertyKeyName(propertyKey)(pos), parseExpression(value))(_)))
  }

  def setNodeProperty(node: String, propertyKey: String, expr: Expression): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => SetNodeProperty(source, node, PropertyKeyName(propertyKey)(pos), expr)(_)))
  }

  def setRelationshipProperty(relationship: String, propertyKey: String, value: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => SetRelationshipProperty(source, relationship, PropertyKeyName(propertyKey)(pos), parseExpression(value))(_)))
  }

  def setProperties(entity: String, items: (String, String)*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => SetProperties(source, parseExpression(entity), items.map(item => (PropertyKeyName(item._1)(pos), parseExpression(item._2))))(_)))
  }

  def setPropertiesExpression(entity: Expression, items: (String, String)*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source =>
      SetProperties(
        source,
        entity,
        items.map(item => (PropertyKeyName(item._1)(pos), parseExpression(item._2)))
      )(_)
    ))
  }

  def setNodeProperties(node: String, items: (String, String)*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => SetNodeProperties(source, node, items.map(item => (PropertyKeyName(item._1)(pos), parseExpression(item._2))))(_)))
  }

  def setRelationshipProperties(node: String, items: (String, String)*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => SetRelationshipProperties(source, node, items.map(item => (PropertyKeyName(item._1)(pos), parseExpression(item._2))))(_)))
  }

  def setPropertiesFromMap(entity: String, map: String, removeOtherProps: Boolean): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => SetPropertiesFromMap(source, parseExpression(entity), parseExpression(map), removeOtherProps)(_)))
  }

  def setNodePropertiesFromMap(node: String, map: String, removeOtherProps: Boolean): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => SetNodePropertiesFromMap(source, node, parseExpression(map), removeOtherProps)(_)))
  }

  def setRelationshipPropertiesFromMap(relationship: String, map: String, removeOtherProps: Boolean): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => SetRelationshipPropertiesFromMap(source, relationship, parseExpression(map), removeOtherProps)(_)))
  }

  def create(nodes: CreateNode*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => Create(source, nodes, Seq.empty)(_)))
  }

  def create(nodes :Seq[CreateNode], relationships: Seq[CreateRelationship]): IMPL = {
    nodes.foreach(node => {
      newNode(varFor(VariableParser.unescaped(node.idName)))
    })
    relationships.foreach(relationship => {
      newRelationship(varFor(VariableParser.unescaped(relationship.idName)))
      newNode(varFor(VariableParser.unescaped(relationship.startNode)))
      newNode(varFor(VariableParser.unescaped(relationship.endNode)))
    })
    appendAtCurrentIndent(UnaryOperator(source => Create(source, nodes, relationships)(_)))
  }

  def merge(nodes: Seq[CreateNode] = Seq.empty,
            relationships: Seq[CreateRelationship] = Seq.empty,
            onMatch: Seq[SetMutatingPattern] = Seq.empty,
            onCreate : Seq[SetMutatingPattern] = Seq.empty,
            lockNodes: Set[String] = Set.empty): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => Merge(source, nodes, relationships, onMatch, onCreate, lockNodes)(_)))
  }

  def nodeHashJoin(nodes: String*): IMPL = {
    appendAtCurrentIndent(BinaryOperator((left, right) => NodeHashJoin(nodes.toSet, left, right)(_)))
  }

  def rightOuterHashJoin(nodes: String*): IMPL = {
    appendAtCurrentIndent(BinaryOperator((left, right) => RightOuterHashJoin(nodes.toSet, left, right)(_)))
  }

  def leftOuterHashJoin(nodes: String*): IMPL = {
    appendAtCurrentIndent(BinaryOperator((left, right) => LeftOuterHashJoin(nodes.toSet, left, right)(_)))
  }

  def valueHashJoin(predicate: String): IMPL = {
    val expression = parseExpression(predicate)
    expression match {
      case e: Equals =>
        appendAtCurrentIndent(BinaryOperator((left, right) => ValueHashJoin(left, right, e)(_)))
      case _ => throw new IllegalArgumentException(s"can't join on $expression")
    }
  }

  def input(nodes: Seq[String] = Seq.empty, relationships: Seq[String] = Seq.empty, variables: Seq[String] = Seq.empty, nullable: Boolean = true): IMPL = {
    if (indent != 0) {
      throw new IllegalStateException("The input operator has to be the left-most leaf of the plan")
    }
    if (nodes.toSet.size < nodes.size || relationships.toSet.size < relationships.size || variables.toSet.size < variables.size) {
      throw new IllegalArgumentException("Input must create unique variables")
    }
    appendAtCurrentIndent(LeafOperator(Input(newNodes(nodes), newRelationships(relationships), newVariables(variables), nullable)(_)))
  }

  def injectCompilationError(): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => {
      InjectCompilationError(lp)(_)
    }))
  }

  def filter(predicateStrings: String*): IMPL = {
    val predicates = predicateStrings.map(parseExpression)
    appendAtCurrentIndent(UnaryOperator(lp => {
      val rewrittenPredicates = predicates.map(rewriteExpression)
      Selection(rewrittenPredicates, lp)(_)
    }))
  }

  def filterExpression(predicateExpressions: Expression*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Selection(predicateExpressions, lp)(_)))
  }

  def errorPlan(e: Exception): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => ErrorPlan(lp, e)(_)))
  }

  def nestedPlanCollectExpressionProjection(resultList: String, resultPart: String): IMPL = {
    val inner = parseExpression(resultPart)
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => Projection(lhs, Map(resultList -> NestedPlanCollectExpression(rhs, inner, "collect(...)")(NONE)))(_)))
  }

  def nestedPlanExistsExpressionProjection(resultList: String): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => Projection(lhs, Map(resultList -> NestedPlanExistsExpression(rhs, "exists(...)")(NONE)))(_)))
  }

  def triadicSelection(positivePredicate: Boolean, sourceId: String, seenId: String, targetId: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => TriadicSelection(lhs, rhs, positivePredicate, sourceId, seenId, targetId)(_)))

  def triadicBuild(triadicSelectionId: Int, sourceId: String, seenId: String): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => TriadicBuild(lp, sourceId, seenId, Some(Id(triadicSelectionId)))(_)))

  def triadicFilter(triadicSelectionId: Int, positivePredicate: Boolean, sourceId: String, targetId: String): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => TriadicFilter(lp, positivePredicate, sourceId, targetId, Some(Id(triadicSelectionId)))(_)))

  def loadCSV(url: String,
              variableName: String,
              format: CSVFormat,
              fieldTerminator: Option[String] = None): IMPL = {
    val urlExpr = parseExpression(url)
    appendAtCurrentIndent(UnaryOperator(lp => LoadCSV(
      lp,
      urlExpr,
      variableName,
      format,
      fieldTerminator,
      legacyCsvQuoteEscaping = GraphDatabaseSettings.csv_legacy_quote_escaping.defaultValue(),
      csvBufferSize = GraphDatabaseSettings.csv_buffer_size.defaultValue().toInt
    )(_)))
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

  def transactionForeach(batchSize: Long = TransactionForeach.defaultBatchSize): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => TransactionForeach(lhs, rhs, literalInt(batchSize))(_)))

  def transactionApply(batchSize: Long = TransactionForeach.defaultBatchSize): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => TransactionApply(lhs, rhs, literalInt(batchSize))(_)))

  // SHIP IP

  protected def buildLogicalPlan(): LogicalPlan = tree.build()

  def getSemanticTable: SemanticTable = semanticTable

  /**
   * Called every time a new node is introduced by some logical operator.
   */
  def newNode(node: Variable): Unit = {
    semanticTable = semanticTable.addNode(node)
  }

  /**
   * Called every time a new relationship is introduced by some logical operator.
   */
  def newRelationship(relationship: Variable): Unit = {
    semanticTable = semanticTable.addRelationship(relationship)
  }

  /**
   * Called every time a new variable is introduced by some logical operator.
   */
  def newVariable(variable: Variable): Unit = {
    semanticTable = semanticTable.addTypeInfoCTAny(variable)
  }

  protected def newAlias(variable: Variable, expression: Expression): Unit = {
    val typeInfo = semanticTable.types.get(expression).orElse(findTypeIgnoringPosition(expression))
    val spec = typeInfo.map(_.actual).getOrElse(CTAny.invariant)
    semanticTable = semanticTable.addTypeInfo(variable, spec)
  }

  private def findTypeIgnoringPosition(expr: Expression) =
    semanticTable.types.iterator.collectFirst { case (`expr`, t) => t }

  protected def newNodes(nodes: Seq[String]): Seq[String] = {
    nodes.map(varFor).foreach(newNode)
    nodes
  }

  protected def newRelationships(relationships: Seq[String]): Seq[String] = {
    relationships.map(varFor).foreach(newRelationship)
    relationships
  }

  protected def newVariables(variables: Seq[String]): Seq[String] = {
    variables.map(varFor).foreach(newVariable)
    variables
  }

  /**
   * Allows implementations to rewrite expressions using contextual information
   */
  protected def rewriteExpression(expr: Expression): Expression = expr

  /**
   * Returns the finalized output of the builder.
   */
  protected def build(readOnly: Boolean = true): T

  // HELPERS
  private def parseExpression(expression: String): Expression = {
    Parser.parseExpression(expression) match {
      case f: FunctionInvocation if f.needsToBeResolved =>
        ResolvedFunctionInvocation(resolver.functionSignature)(f).coerceArguments
      case e => e
    }
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
  def literalString(str: String): StringLiteral = StringLiteral(str)(pos)
  def function(name: String, args: Expression*): FunctionInvocation =
    FunctionInvocation(FunctionName(name)(pos), distinct = false, args.toIndexedSeq)(pos)
}

object AbstractLogicalPlanBuilder {
  val pos: InputPosition = new InputPosition(0, 1, 0)
  val NO_PREDICATE: Predicate = Predicate("", "")

  case class Predicate(entity: String, predicate: String) {
    def asVariablePredicate: Option[VariablePredicate] = {
      if (entity == "") {
        None
      } else {
        Some(VariablePredicate(Variable(entity)(pos), Parser.parseExpression(predicate)))
      }
    }
  }

  def createPattern(nodes: Seq[CreateNode] = Seq.empty,
                    relationships: Seq[CreateRelationship] = Seq.empty): CreatePattern = {
    CreatePattern(nodes, relationships)
  }

  def createNode(node: String, labels: String*): CreateNode =
    CreateNode(node, labels.map(LabelName(_)(pos)), None)

  def createNodeWithProperties(node: String, labels: Seq[String], properties: String): CreateNode =
    CreateNode(node, labels.map(LabelName(_)(pos)), Some(Parser.parseExpression(properties)))

  def createRelationship(relationship: String,
                         left: String,
                         typ: String,
                         right: String,
                         direction: SemanticDirection = OUTGOING,
                         properties: Option[String] = None): CreateRelationship =
    CreateRelationship(relationship, left, RelTypeName(typ)(pos), right, direction, properties.map(Parser.parseExpression))

  def setNodeProperty(node: String, key: String, value: String): SetMutatingPattern =
    SetNodePropertyPattern(node, PropertyKeyName(key)(InputPosition.NONE), Parser.parseExpression(value))

  def setNodeProperties(node: String, items: (String, String)*): SetMutatingPattern =
    SetNodePropertiesPattern(node, items.map(i => (PropertyKeyName(i._1)(InputPosition.NONE), Parser.parseExpression(i._2))))

  def setNodePropertiesFromMap(node: String, map: String, removeOtherProps: Boolean = true): SetMutatingPattern =
    SetNodePropertiesFromMapPattern(node, Parser.parseExpression(map), removeOtherProps)

  def setRelationshipProperty(relationship: String, key: String, value: String): SetMutatingPattern =
    SetRelationshipPropertyPattern(relationship, PropertyKeyName(key)(InputPosition.NONE), Parser.parseExpression(value))

  def setRelationshipProperties(rel: String, items: (String, String)*): SetMutatingPattern =
    SetRelationshipPropertiesPattern(rel, items.map(i => (PropertyKeyName(i._1)(InputPosition.NONE), Parser.parseExpression(i._2))))

  def setRelationshipPropertiesFromMap(node: String, map: String, removeOtherProps: Boolean = true): SetMutatingPattern =
    SetRelationshipPropertiesFromMapPattern(node, Parser.parseExpression(map), removeOtherProps)

  def setProperty(entity: String, key: String, value: String): SetMutatingPattern =
    SetPropertyPattern(Parser.parseExpression(entity), PropertyKeyName(key)(InputPosition.NONE), Parser.parseExpression(value))

  def setProperties(entity: String, items: (String, String)*): SetMutatingPattern =
    SetPropertiesPattern(Parser.parseExpression(entity), items.map(i => (PropertyKeyName(i._1)(InputPosition.NONE), Parser.parseExpression(i._2))))

  def setPropertyFromMap(entity: String, map: String, removeOtherProps: Boolean = true): SetMutatingPattern =
    SetPropertiesFromMapPattern(Parser.parseExpression(entity), Parser.parseExpression(map), removeOtherProps)

  def setLabel(node: String, labels: String*): SetMutatingPattern =
    SetLabelPattern(node, labels.map(l => LabelName(l)(InputPosition.NONE)))

  def removeLabel(node: String, labels: String*): RemoveLabelPattern =
    RemoveLabelPattern(node, labels.map(l => LabelName(l)(InputPosition.NONE)))

  def delete(entity: String, forced: Boolean = false): ir.DeleteExpression =
    ir.DeleteExpression(Parser.parseExpression(entity), forced)
}
