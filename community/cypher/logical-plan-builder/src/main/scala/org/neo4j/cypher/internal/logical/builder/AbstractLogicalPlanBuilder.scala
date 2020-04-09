/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.LabelToken
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.PropertyKeyToken
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.Range
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.ShortestPaths
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.ir.CreateNode
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.ShortestPathPattern
import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.ir.VarPatternLength
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.Predicate
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.pos
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.AllNodesScan
import org.neo4j.cypher.internal.logical.plans.Anti
import org.neo4j.cypher.internal.logical.plans.AntiSemiApply
import org.neo4j.cypher.internal.logical.plans.Apply
import org.neo4j.cypher.internal.logical.plans.Argument
import org.neo4j.cypher.internal.logical.plans.CacheProperties
import org.neo4j.cypher.internal.logical.plans.CartesianProduct
import org.neo4j.cypher.internal.logical.plans.ColumnOrder
import org.neo4j.cypher.internal.logical.plans.ConditionalApply
import org.neo4j.cypher.internal.logical.plans.Create
import org.neo4j.cypher.internal.logical.plans.CursorProperty
import org.neo4j.cypher.internal.logical.plans.DeleteRelationship
import org.neo4j.cypher.internal.logical.plans.DetachDeleteNode
import org.neo4j.cypher.internal.logical.plans.DirectedRelationshipByIdSeek
import org.neo4j.cypher.internal.logical.plans.Distinct
import org.neo4j.cypher.internal.logical.plans.DoNotGetValue
import org.neo4j.cypher.internal.logical.plans.DoNotIncludeTies
import org.neo4j.cypher.internal.logical.plans.DropResult
import org.neo4j.cypher.internal.logical.plans.Eager
import org.neo4j.cypher.internal.logical.plans.EmptyResult
import org.neo4j.cypher.internal.logical.plans.ErrorPlan
import org.neo4j.cypher.internal.logical.plans.Expand
import org.neo4j.cypher.internal.logical.plans.ExpandAll
import org.neo4j.cypher.internal.logical.plans.ExpandCursorProperties
import org.neo4j.cypher.internal.logical.plans.ExpandInto
import org.neo4j.cypher.internal.logical.plans.ExpansionMode
import org.neo4j.cypher.internal.logical.plans.FindShortestPaths
import org.neo4j.cypher.internal.logical.plans.GetValueFromIndexBehavior
import org.neo4j.cypher.internal.logical.plans.IndexLeafPlan
import org.neo4j.cypher.internal.logical.plans.IndexOrder
import org.neo4j.cypher.internal.logical.plans.IndexOrderNone
import org.neo4j.cypher.internal.logical.plans.IndexSeek
import org.neo4j.cypher.internal.logical.plans.IndexSeekLeafPlan
import org.neo4j.cypher.internal.logical.plans.IndexedProperty
import org.neo4j.cypher.internal.logical.plans.Input
import org.neo4j.cypher.internal.logical.plans.LeftOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.Limit
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ManySeekableArgs
import org.neo4j.cypher.internal.logical.plans.MultiNodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NestedPlanCollectExpression
import org.neo4j.cypher.internal.logical.plans.NestedPlanExistsExpression
import org.neo4j.cypher.internal.logical.plans.NodeByIdSeek
import org.neo4j.cypher.internal.logical.plans.NodeByLabelScan
import org.neo4j.cypher.internal.logical.plans.NodeCountFromCountStore
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.logical.plans.NodeIndexSeek
import org.neo4j.cypher.internal.logical.plans.NonFuseable
import org.neo4j.cypher.internal.logical.plans.Optional
import org.neo4j.cypher.internal.logical.plans.OptionalExpand
import org.neo4j.cypher.internal.logical.plans.OrderedAggregation
import org.neo4j.cypher.internal.logical.plans.OrderedDistinct
import org.neo4j.cypher.internal.logical.plans.PartialSort
import org.neo4j.cypher.internal.logical.plans.PartialTop
import org.neo4j.cypher.internal.logical.plans.PointDistanceRange
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
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
import org.neo4j.cypher.internal.logical.plans.ResolvedCall
import org.neo4j.cypher.internal.logical.plans.RightOuterHashJoin
import org.neo4j.cypher.internal.logical.plans.RollUpApply
import org.neo4j.cypher.internal.logical.plans.SelectOrAntiSemiApply
import org.neo4j.cypher.internal.logical.plans.SelectOrSemiApply
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.logical.plans.SemiApply
import org.neo4j.cypher.internal.logical.plans.SetNodePropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetNodeProperty
import org.neo4j.cypher.internal.logical.plans.SetProperty
import org.neo4j.cypher.internal.logical.plans.SetRelationshipPropertiesFromMap
import org.neo4j.cypher.internal.logical.plans.SetRelationshipProperty
import org.neo4j.cypher.internal.logical.plans.SingleSeekableArg
import org.neo4j.cypher.internal.logical.plans.Skip
import org.neo4j.cypher.internal.logical.plans.Sort
import org.neo4j.cypher.internal.logical.plans.Top
import org.neo4j.cypher.internal.logical.plans.UndirectedRelationshipByIdSeek
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
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.attribution.SequentialIdGen

import scala.collection.mutable.ArrayBuffer

/**
 * Used by [[AbstractLogicalPlanBuilder]] to resolve tokens and procedures
 */
trait Resolver {
  /**
   * Obtain the token of a label by name.
   */
  def getLabelId(label: String): Int

  def getPropertyKeyId(prop: String): Int

  def procedureSignature(name: QualifiedName): ProcedureSignature

  def functionSignature(name: QualifiedName): Option[UserFunctionSignature]
}

/**
 * Test help utility for hand-writing objects needing logical plans.
 */
abstract class AbstractLogicalPlanBuilder[T, IMPL <: AbstractLogicalPlanBuilder[T, IMPL]](protected val resolver: Resolver) {

  self: IMPL =>

  val patternParser = new PatternParser

  protected sealed trait OperatorBuilder
  protected case class LeafOperator(planToIdConstructor: IdGen => LogicalPlan) extends OperatorBuilder{
    private val id = idGen.id()
    _idOfLastPlan = id
    val plan: LogicalPlan = planToIdConstructor(SameId(id))
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
        case o:LeafOperator => o.plan
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

  def procedureCall(call: String): IMPL = {
    val uc = Parser.parseProcedureCall(call)
    appendAtCurrentIndent(UnaryOperator(lp => ProcedureCall(lp, ResolvedCall(resolver.procedureSignature)(uc))(_)))
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

  def limit(count: Long): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Limit(lp, literalInt(count), DoNotIncludeTies)(_)))
    self
  }

  def skip(count: Long): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Skip(lp, literalInt(count))(_)))
    self
  }

  def expand(pattern: String,
             expandMode: ExpansionMode = ExpandAll,
             projectedDir: SemanticDirection = OUTGOING,
             nodePredicate: Predicate = AbstractLogicalPlanBuilder.NO_PREDICATE,
             relationshipPredicate: Predicate = AbstractLogicalPlanBuilder.NO_PREDICATE,
             cacheNodeProperties: Seq[String] = Seq.empty,
             cacheRelProperties: Seq[String] = Seq.empty): IMPL = {
    val p = patternParser.parse(pattern)
    newRelationship(varFor(p.relName))
    if (expandMode == ExpandAll) {
      newNode(varFor(p.to))
    }

    val expandProperties = getExpandProperties(p.from, p.relName, cacheNodeProperties, cacheRelProperties)
    p.length match {
      case SimplePatternLength =>
        appendAtCurrentIndent(UnaryOperator(lp => Expand(lp, p.from, p.dir, p.relTypes, p.to, p.relName, expandMode, expandProperties)(_)))
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

    p.length match {
      case SimplePatternLength => throw new IllegalArgumentException("Shortest path must have a variable length pattern")
      case VarPatternLength(min, max) =>
        appendAtCurrentIndent(UnaryOperator(lp => FindShortestPaths(lp,
          ShortestPathPattern(pathName, PatternRelationship(p.relName, (p.from, p.to), p.dir, p.relTypes, p.length), !all)
          (ShortestPaths(RelationshipChain(
            NodePattern(Some(varFor(p.from)), Seq.empty, None)(pos), // labels and properties are not used at runtime
            RelationshipPattern(Some(varFor(p.relName)),
              p.relTypes,
              Some(Some(Range(Some(UnsignedDecimalIntegerLiteral(min.toString)(pos)), max.map(i => UnsignedDecimalIntegerLiteral(i.toString)(pos)))(pos))),
              None, // properties are not used at runtime
              p.dir
            )(pos),
            NodePattern(Some(varFor(p.to)), Seq.empty, None)(pos) // labels and properties are not used at runtime
          )(pos), !all)(pos)),
          predicates.map(Parser.parseExpression),
          withFallback,
          disallowSameNode
        )(_)))
    }
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
                        predicate: Option[String] = None,
                        cacheNodeProperties: Seq[String] = Seq.empty,
                        cacheRelProperties: Seq[String] = Seq.empty): IMPL = {
    val p = patternParser.parse(pattern)
    p.length match {
      case SimplePatternLength =>
        val pred = predicate.map(Parser.parseExpression)
        appendAtCurrentIndent(UnaryOperator(lp =>
          OptionalExpand(lp, p.from, p.dir, p.relTypes, p.to, p.relName, ExpandAll, pred, getExpandProperties(p.from, p.relName, cacheNodeProperties, cacheRelProperties)
    )(_)))
      case _ =>
        throw new IllegalArgumentException("Cannot have optional expand with variable length pattern")
    }
    self
  }

  def optionalExpandInto(pattern: String, predicate: Option[String] = None): IMPL = {
    val p = patternParser.parse(pattern)
    p.length match {
      case SimplePatternLength =>
        val pred = predicate.map(Parser.parseExpression)
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
    appendAtCurrentIndent(UnaryOperator(lp => ProjectEndpoints(lp, p.relName, p.from, startInScope, p.to, endInScope,
                                                               relTypesAsNonEmptyOption, directed, p.length)(_)))
  }

  def partialSort(alreadySortedPrefix: Seq[ColumnOrder], stillToSortSuffix: Seq[ColumnOrder]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => PartialSort(lp, alreadySortedPrefix, stillToSortSuffix)(_)))
    self
  }

  def sort(sortItems: Seq[ColumnOrder]): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Sort(lp, sortItems)(_)))
    self
  }

  def top(sortItems: Seq[ColumnOrder], limit: Long): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Top(lp, sortItems, literalInt(limit))(_)))
    self
  }
  def partialTop(alreadySortedPrefix: Seq[ColumnOrder], stillToSortSuffix: Seq[ColumnOrder], limit: Long): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => PartialTop(lp, alreadySortedPrefix, stillToSortSuffix, literalInt(limit))(_)))
    self
  }

  def eager(): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Eager(lp)(_)))
    self
  }


  def emptyResult(): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => EmptyResult(lp)(_)))
    self
  }

  def detachDeleteNode(node: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DetachDeleteNode(lp, Parser.parseExpression(node))(_)))
    self
  }

  def deleteRelationship(rel: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DeleteRelationship(lp, Parser.parseExpression(rel))(_)))
    self
  }

  def unwind(projectionString: String): IMPL = {
    val (name, expression) = Parser.parseProjections(projectionString).head
    appendAtCurrentIndent(UnaryOperator(lp => UnwindCollection(lp, name, expression)(_)))
    self
  }

  def projection(projectionStrings: String*): IMPL = {
    val projections = Parser.parseProjections(projectionStrings: _*)
    appendAtCurrentIndent(UnaryOperator(lp => Projection(lp, projections)(_)))
    self
  }

  def distinct(projectionStrings: String*): IMPL = {
    val projections = Parser.parseProjections(projectionStrings: _*)
    appendAtCurrentIndent(UnaryOperator(lp => Distinct(lp, projections)(_)))
    self
  }

  def orderedDistinct(orderToLeverage: Seq[String], projectionStrings: String*): IMPL = {
    val order = orderToLeverage.map(Parser.parseExpression)
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

  def nodeByLabelScan(node: String, label: String, indexOrder: IndexOrder, args: String*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    appendAtCurrentIndent(LeafOperator(NodeByLabelScan(n, labelName(label), args.map(VariableParser.unescaped).toSet, indexOrder)(_)))
  }

  def nodeByIdSeek(node: String, args: Set[String], ids: AnyVal*): IMPL = {
    val n = VariableParser.unescaped(node)
    newNode(varFor(n))
    val idExpressions = ids.map {
      case x@(_:Long|_:Int) => UnsignedDecimalIntegerLiteral(x.toString)(pos)
      case x@(_:Float|_:Double) =>  DecimalDoubleLiteral(x.toString)(pos)
      case x => throw new IllegalArgumentException(s"$x is not a supported value for ID")
    }
    val input =
      if (idExpressions.length == 1) {
        SingleSeekableArg(idExpressions.head)
      } else {
        ManySeekableArgs(ListLiteral(idExpressions)(pos))
      }

    appendAtCurrentIndent(LeafOperator(NodeByIdSeek(n, input, args)(_)))
  }

  def directedRelationshipByIdSeek(relationship: String, from: String, to: String, args: Set[String], ids: AnyVal*): IMPL = {
    newRelationship(varFor(relationship))
    val idExpressions = ids.map {
      case x@(_:Long|_:Int) => UnsignedDecimalIntegerLiteral(x.toString)(pos)
      case x@(_:Float|_:Double) =>  DecimalDoubleLiteral(x.toString)(pos)
      case x => throw new IllegalArgumentException(s"$x is not a supported value for ID")
    }
    val input =
      if (idExpressions.length == 1) {
        SingleSeekableArg(idExpressions.head)
      } else {
        ManySeekableArgs(ListLiteral(idExpressions)(pos))
      }

    appendAtCurrentIndent(LeafOperator(DirectedRelationshipByIdSeek(relationship, input, from, to, args)(_)))
  }

  def undirectedRelationshipByIdSeek(relationship: String, from: String, to: String, args: Set[String], ids: AnyVal*): IMPL = {
    newRelationship(varFor(relationship))
    val idExpressions = ids.map {
      case x@(_:Long|_:Int) => UnsignedDecimalIntegerLiteral(x.toString)(pos)
      case x@(_:Float|_:Double) =>  DecimalDoubleLiteral(x.toString)(pos)
      case x => throw new IllegalArgumentException(s"$x is not a supported value for ID")
    }
    val input =
      if (idExpressions.length == 1) {
        SingleSeekableArg(idExpressions.head)
      } else {
        ManySeekableArgs(ListLiteral(idExpressions)(pos))
      }

    appendAtCurrentIndent(LeafOperator(UndirectedRelationshipByIdSeek(relationship, input, from, to, args)(_)))
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
                        getValue: GetValueFromIndexBehavior = DoNotGetValue,
                        indexOrder: IndexOrder = IndexOrderNone,
                        paramExpr: Option[Expression] = None,
                        argumentIds: Set[String] = Set.empty,
                        unique: Boolean = false,
                        customQueryExpression: Option[QueryExpression[Expression]] = None): IMPL = {

    val planBuilder = (idGen: IdGen) => {
      val plan = indexSeek(indexSeekString, getValue, indexOrder, paramExpr, argumentIds, unique, customQueryExpression)(idGen)
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def indexSeek(indexSeekString: String,
                getValue: GetValueFromIndexBehavior = DoNotGetValue,
                indexOrder: IndexOrder = IndexOrderNone,
                paramExpr: Option[Expression] = None,
                argumentIds: Set[String] = Set.empty,
                unique: Boolean = false,
                customQueryExpression: Option[QueryExpression[Expression]] = None): IdGen => IndexLeafPlan = {
    val label = resolver.getLabelId(IndexSeek.labelFromIndexSeekString(indexSeekString))
    val propIds: PartialFunction[String, Int] = { case x => resolver.getPropertyKeyId(x) }
    val planBuilder = (idGen: IdGen) => {
      val plan = IndexSeek(indexSeekString, getValue, indexOrder, paramExpr, argumentIds, Some(propIds), label, unique,
                           customQueryExpression)(idGen)
      newNode(varFor(plan.idName))
      plan
    }
    planBuilder
  }

  def multiNodeIndexSeekOperator(seeks: (IMPL => IdGen => IndexLeafPlan)*): IMPL = {
    val planBuilder = (idGen: IdGen) => {
      MultiNodeIndexSeek(seeks.map(_(this)(idGen).asInstanceOf[IndexSeekLeafPlan]))(idGen)
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def pointDistanceIndexSeek(node: String,
                             labelName: String,
                             property: String,
                             point: String,
                             distance: Double,
                             getValue: GetValueFromIndexBehavior = DoNotGetValue,
                             indexOrder: IndexOrder = IndexOrderNone,
                             inclusive: Boolean = false,
                             argumentIds: Set[String] = Set.empty): IMPL = {
    val label = resolver.getLabelId(labelName)

    val propId = resolver.getPropertyKeyId(property)
    val planBuilder = (idGen: IdGen) => {
      val labelToken = LabelToken(labelName, LabelId(label))
      val propToken = PropertyKeyToken(PropertyKeyName(property)(NONE), PropertyKeyId(propId))
      val indexedProperty = IndexedProperty(propToken, getValue)
      val e =
        RangeQueryExpression(PointDistanceSeekRangeWrapper(
          PointDistanceRange(function("point", Parser.parseExpression(point)), literalFloat(distance), inclusive))(NONE))
      val plan = NodeIndexSeek(node,
                               labelToken,
                               Seq(indexedProperty),
                               e,
                               argumentIds,
                               indexOrder)(idGen)
      newNode(varFor(plan.idName))
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def aggregation(groupingExpressions: Seq[String],
                  aggregationExpression: Seq[String]): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => Aggregation(lp,
      Parser.parseProjections(groupingExpressions: _*),
      Parser.parseProjections(aggregationExpression: _*))(_)))

  def orderedAggregation(groupingExpressions: Seq[String],
                         aggregationExpression: Seq[String],
                         orderToLeverage: Seq[String]): IMPL = {
    val order = orderToLeverage.map(Parser.parseExpression)
    appendAtCurrentIndent(UnaryOperator(lp => OrderedAggregation(lp,
      Parser.parseProjections(groupingExpressions: _*),
      Parser.parseProjections(aggregationExpression: _*),
      order)(_)))
  }

  def apply(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => Apply(lhs, rhs)(_)))

  def antiSemiApply(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => AntiSemiApply(lhs, rhs)(_)))

  def semiApply(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => SemiApply(lhs, rhs)(_)))

  def conditionalApply(items: String*): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => ConditionalApply(lhs, rhs, items)(_)))

  def selectOrSemiApply(predicate: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => SelectOrSemiApply(lhs, rhs, Parser.parseExpression(predicate))(_)))

  def selectOrAntiSemiApply(predicate: String): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => SelectOrAntiSemiApply(lhs, rhs, Parser.parseExpression(predicate))(_)))

  def rollUpApply(collectionName: String,
                  variableToCollect: String,
                  nullableIdentifiers: Set[String] = Set.empty): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => RollUpApply(lhs, rhs, collectionName, variableToCollect, nullableIdentifiers)(_)))

  def cartesianProduct(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => CartesianProduct(lhs, rhs)(_)))

  def union(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => Union(lhs, rhs)(_)))

  def expandAll(pattern: String): IMPL = expand(pattern, ExpandAll)

  def nonFuseable(): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => NonFuseable(lp)(_)))

  def cacheProperties(properties: String*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => CacheProperties(source,
      properties.map(Parser.parseExpression(_).asInstanceOf[LogicalProperty]).toSet)(_)))
  }

  def setProperty(entity: String, propertyKey: String, value: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => SetProperty(source, Parser.parseExpression(entity), PropertyKeyName(propertyKey)(pos), Parser.parseExpression(value))(_)))
  }

  def setNodeProperty(node: String, propertyKey: String, value: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => SetNodeProperty(source, node, PropertyKeyName(propertyKey)(pos), Parser.parseExpression(value))(_)))
  }

  def setRelationshipProperty(relationship: String, propertyKey: String, value: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => SetRelationshipProperty(source, relationship, PropertyKeyName(propertyKey)(pos), Parser.parseExpression(value))(_)))
  }

  def setNodePropertiesFromMap(node: String, map: String, removeOtherProps: Boolean): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => SetNodePropertiesFromMap(source, node, Parser.parseExpression(map), removeOtherProps)(_)))
  }

  def setRelationshipPropertiesFromMap(relationship: String, map: String, removeOtherProps: Boolean): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => SetRelationshipPropertiesFromMap(source, relationship, Parser.parseExpression(map), removeOtherProps)(_)))
  }

  def create(nodes: CreateNode*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(source => Create(source, nodes, Seq.empty)(_)))
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
    val expression = Parser.parseExpression(predicate)
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
    nodes.foreach(node => newNode(varFor(node)))
    relationships.foreach(rel => newRelationship(varFor(rel)))
    variables.foreach(v => newVariable(varFor(v)))
    appendAtCurrentIndent(LeafOperator(Input(nodes, relationships, variables, nullable)(_)))
  }

  def filter(predicateStrings: String*): IMPL = {
    val predicates = predicateStrings.map(Parser.parseExpression)
    appendAtCurrentIndent(UnaryOperator(lp => Selection(predicates, lp)(_)))
  }

  def filterExpression(predicateExpressions: Expression*): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Selection(predicateExpressions, lp)(_)))
  }

  def dropResult(): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DropResult(lp)(_)))
  }

  def errorPlan(e: Exception): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => ErrorPlan(lp, e)(_)))
  }

  def nestedPlanCollectExpressionProjection(resultList: String, resultPart: String): IMPL = {
    val inner = Parser.parseExpression(resultPart)
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => Projection(lhs, Map(resultList -> NestedPlanCollectExpression(rhs, inner)(NONE)))(_)))
  }

  def nestedPlanExistsExpressionProjection(resultList: String): IMPL = {
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => Projection(lhs, Map(resultList -> NestedPlanExistsExpression(rhs)(NONE)))(_)))
  }

  // SHIP IP

  protected def buildLogicalPlan(): LogicalPlan = tree.build()

  // ABSTRACT METHODS

  /**
   * Called everytime a new node is introduced by some logical operator.
   */
  protected def newNode(node: Variable): Unit

  /**
   * Called everytime a new relationship is introduced by some logical operator.
   */
  protected def newRelationship(relationship: Variable): Unit

  /**
   * Called everytime a new variable is introduced by some logical operator.
   */
  protected def newVariable(variable: Variable): Unit

  /**
   * Returns the finalized output of the builder.
   */
  protected def build(readOnly: Boolean = true): T

  // HELPERS

  protected def appendAtCurrentIndent(operatorBuilder: OperatorBuilder): IMPL = {
    if (tree == null) {
      throw new IllegalStateException("Must call produceResult before adding other operators.")
    }

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
  private def literalMap(map: Map[String, Expression]) =
    MapExpression(map.map {
      case (key, value) => PropertyKeyName(key)(pos) -> value
    }.toSeq)(pos)
  def literalString(str: String): StringLiteral = StringLiteral(str)(pos)
  def function(name: String, args: Expression*): FunctionInvocation =
    FunctionInvocation(FunctionName(name)(pos), distinct = false, args.toIndexedSeq)(pos)

  private def getExpandProperties(from: String,
                                  rel: String,
                                  cacheNodeProperties: Seq[String],
                                  cacheRelProperties: Seq[String]): Option[ExpandCursorProperties] = {
    if (cacheNodeProperties.isEmpty && cacheRelProperties.isEmpty) {
      None
    } else {
      Some(
        ExpandCursorProperties(
          nodeProperties = cacheNodeProperties.map(prop => CursorProperty(from, NODE_TYPE, PropertyKeyName(prop)(NONE))),
          relProperties = cacheRelProperties.map(prop => CursorProperty(rel, RELATIONSHIP_TYPE, PropertyKeyName(prop)(NONE)))
        ))
    }
  }
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

  def createNode(node: String, labels: String*): CreateNode =
    CreateNode(node, labels.map(LabelName(_)(pos)), None)
}
