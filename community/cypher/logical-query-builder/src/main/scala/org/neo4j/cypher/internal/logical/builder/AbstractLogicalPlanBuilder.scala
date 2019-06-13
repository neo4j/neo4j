/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.neo4j.cypher.internal.ir.SimplePatternLength
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.pos
import org.neo4j.cypher.internal.logical.plans._
import org.neo4j.cypher.internal.v4_0.expressions._
import org.neo4j.cypher.internal.v4_0.util.InputPosition
import org.neo4j.cypher.internal.v4_0.util.attribution.{Id, IdGen, SameId, SequentialIdGen}

import scala.collection.mutable.ArrayBuffer

/**
  * Used by [[AbstractLogicalPlanBuilder]] to resolve tokens
  */
trait TokenResolver {
  /**
    * Obtain the token of a label by name.
    */
  def getLabelId(label: String): Int

  def getPropertyKeyId(prop: String): Int
}

/**
  * Test help utility for hand-writing objects needing logical plans.
  */
abstract class AbstractLogicalPlanBuilder[T, IMPL <: AbstractLogicalPlanBuilder[T, IMPL]](protected val tokenResolver: TokenResolver) {

  self: IMPL =>

  private sealed trait OperatorBuilder
  private case class LeafOperator(planToIdConstructor: IdGen => LogicalPlan) extends OperatorBuilder{
    private val id = idGen.id()
    _idOfLastPlan = id
    val plan: LogicalPlan = planToIdConstructor(SameId(id))
  }
  private case class UnaryOperator(planToIdConstructor: LogicalPlan => IdGen => LogicalPlan) extends OperatorBuilder {
    private val id = idGen.id()
    _idOfLastPlan = id
    val planConstructor: LogicalPlan => LogicalPlan = planToIdConstructor(_)(SameId(id))
  }
  private case class BinaryOperator(planToIdConstructor: (LogicalPlan, LogicalPlan) => IdGen => LogicalPlan) extends OperatorBuilder {
    private val id = idGen.id()
    _idOfLastPlan = id
    val planConstructor: (LogicalPlan, LogicalPlan) => LogicalPlan = planToIdConstructor(_, _)(SameId(id))
  }

  private class Tree(operator: OperatorBuilder) {
    var left: Option[Tree] = None
    var right: Option[Tree] = None

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

  private val idGen: IdGen = new SequentialIdGen()

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
    resultColumns = vars.toArray
    tree = new Tree(UnaryOperator(lp => ProduceResult(lp, resultColumns)(_)))
    looseEnds += tree
    self
  }

  def optional(): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Optional(lp, Set.empty)(_)))
    self
  }

  def limit(count: Int): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Limit(lp, literalInt(count), DoNotIncludeTies)(_)))
    self
  }

  def expand(pattern: String): IMPL = {
    val p = PatternParser.parse(pattern)
    p.length match {
      case SimplePatternLength =>
        appendAtCurrentIndent(UnaryOperator(lp => Expand(lp, p.from, p.dir, p.relTypes, p.to, p.relName, ExpandAll)(_)))
      // TODO VarExpand and PruningVarExpand
    }
    self
  }

  def optionalExpandAll(pattern: String, predicate: Option[String]): IMPL = {
    val p = PatternParser.parse(pattern)
    p.length match {
      case SimplePatternLength =>
        val pred = predicate.map(ExpressionParser.parseExpression)
        appendAtCurrentIndent(UnaryOperator(lp => OptionalExpand(lp, p.from, p.dir, p.relTypes, p.to, p.relName, ExpandAll, pred)(_)))
      case _ =>
        throw new IllegalArgumentException("Cannot have optional expand with variable length pattern")
    }
    self
  }

  def optionalExpandInto(pattern: String, predicate: Option[String]): IMPL = {
    val p = PatternParser.parse(pattern)
    p.length match {
      case SimplePatternLength =>
        val pred = predicate.map(ExpressionParser.parseExpression)
        appendAtCurrentIndent(UnaryOperator(lp => OptionalExpand(lp, p.from, p.dir, p.relTypes, p.to, p.relName, ExpandInto, pred)(_)))
      case _ =>
        throw new IllegalArgumentException("Cannot have optional expand with variable length pattern")
    }
    self
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

  def top(sortItems: Seq[ColumnOrder], limitVariable: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => Top(lp, sortItems, varFor(limitVariable))(_)))
    self
  }

  def partialTop(alreadySortedPrefix: Seq[ColumnOrder], stillToSortSuffix: Seq[ColumnOrder], limit: Long): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => PartialTop(lp, alreadySortedPrefix, stillToSortSuffix, literalInt(limit))(_)))
    self
  }

  def partialTop(alreadySortedPrefix: Seq[ColumnOrder], stillToSortSuffix: Seq[ColumnOrder], limitVariable: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => PartialTop(lp, alreadySortedPrefix, stillToSortSuffix, varFor(limitVariable))(_)))
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
    appendAtCurrentIndent(UnaryOperator(lp => DetachDeleteNode(lp, varFor(node))(_)))
    self
  }

  def deleteRel(rel: String): IMPL = {
    appendAtCurrentIndent(UnaryOperator(lp => DeleteRelationship(lp, varFor(rel))(_)))
    self
  }

  def expandInto(pattern: String): IMPL = {
    val p = PatternParser.parse(pattern)
    newNode(varFor(p.from))
    newNode(varFor(p.to))
    appendAtCurrentIndent(UnaryOperator(lp => Expand(lp, p.from, p.dir, p.relTypes, p.to, p.relName, ExpandInto)(_)))
    self
  }

  def unwind(projectionString: String): IMPL = {
    val (name, expression) = ExpressionParser.parseProjections(projectionString).head
    appendAtCurrentIndent(UnaryOperator(lp => UnwindCollection(lp, name, expression)(_)))
    self
  }

  def projection(projectionStrings: String*): IMPL = {
    val projections = ExpressionParser.parseProjections(projectionStrings: _*)
    appendAtCurrentIndent(UnaryOperator(lp => Projection(lp, projections)(_)))
    self
  }

  def distinct(projectionStrings: String*): IMPL = {
    val projections = ExpressionParser.parseProjections(projectionStrings: _*)
    appendAtCurrentIndent(UnaryOperator(lp => Distinct(lp, projections)(_)))
    self
  }

  def orderedDistinct(orderToLeverage: Seq[Expression], projectionStrings: String*): IMPL = {
    val projections = ExpressionParser.parseProjections(projectionStrings: _*)
    appendAtCurrentIndent(UnaryOperator(lp => OrderedDistinct(lp, projections, orderToLeverage)(_)))
    self
  }

  def allNodeScan(node: String, args: String*): IMPL = {
    newNode(varFor(node))
    appendAtCurrentIndent(LeafOperator(AllNodesScan(node, args.toSet)(_)))
  }

  def argument(args: String*): IMPL = {
    appendAtCurrentIndent(LeafOperator(Argument(args.toSet)(_)))
  }

  def nodeByLabelScan(node: String, label: String, args: String*): IMPL = {
    newNode(varFor(node))
    appendAtCurrentIndent(LeafOperator(NodeByLabelScan(node, labelName(label), args.toSet)(_)))
  }

  def nodeByIdSeek(node: String, ids: Long*): IMPL = {
    newNode(varFor(node))
      val idExpressions = ids.map(l => UnsignedDecimalIntegerLiteral(l.toString)(pos))
    val input =
      if (idExpressions.length == 1) {
        SingleSeekableArg(idExpressions.head)
      } else {
        ManySeekableArgs(ListLiteral(idExpressions)(pos))
      }

    appendAtCurrentIndent(LeafOperator(NodeByIdSeek(node, input , Set.empty)(_)))
  }

  def nodeIndexOperator(indexSeekString: String,
                        getValue: GetValueFromIndexBehavior = DoNotGetValue,
                        indexOrder: IndexOrder = IndexOrderNone,
                        paramExpr: Option[Expression] = None,
                        argumentIds: Set[String] = Set.empty,
                        unique: Boolean = false,
                        customQueryExpression: Option[QueryExpression[Expression]] = None): IMPL = {
    val label = tokenResolver.getLabelId(IndexSeek.labelFromIndexSeekString(indexSeekString))
    val propIds = PartialFunction(tokenResolver.getPropertyKeyId)
    val planBuilder = (idGen: IdGen) => {
      val plan = IndexSeek(indexSeekString, getValue, indexOrder, paramExpr, argumentIds, Some(propIds), label, unique, customQueryExpression)(idGen)
      newNode(varFor(plan.idName))
      plan
    }
    appendAtCurrentIndent(LeafOperator(planBuilder))
  }

  def aggregation(groupingExpressions: Seq[String],
                  aggregationExpression: Seq[String]): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => Aggregation(lp,
      ExpressionParser.parseProjections(groupingExpressions: _*),
      ExpressionParser.parseProjections(aggregationExpression: _*))(_)))

  def orderedAggregation(groupingExpressions: Seq[String],
                         aggregationExpression: Seq[String],
                         orderToLeverage: Seq[Expression]): IMPL =
    appendAtCurrentIndent(UnaryOperator(lp => OrderedAggregation(lp,
      ExpressionParser.parseProjections(groupingExpressions: _*),
      ExpressionParser.parseProjections(aggregationExpression: _*),
      orderToLeverage)(_)))

  def apply(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => Apply(lhs, rhs)(_)))

  def cartesianProduct(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => CartesianProduct(lhs, rhs)(_)))

  def union(): IMPL =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => Union(lhs, rhs)(_)))

  def expandAll(pattern: String): IMPL = {
    val p = PatternParser.parse(pattern)
    appendAtCurrentIndent(UnaryOperator(source => Expand(source, p.from, p.dir, p.relTypes, p.to, p.relName, ExpandAll)(_)))
  }

  def nodeHashJoin(nodes: String*): IMPL = {
    appendAtCurrentIndent(BinaryOperator((left, right) => NodeHashJoin(nodes.toSet, left, right)(_)))
  }

  def argument(): IMPL =
    appendAtCurrentIndent(LeafOperator(Argument()(_)))

  def input(nodes: Seq[String] = Seq.empty, variables: Seq[String] = Seq.empty, nullable: Boolean = true): IMPL = {
    if (indent != 0) {
      throw new IllegalStateException("The input operator has to be the left-most leaf of the plan")
    }
    if (nodes.toSet.size < nodes.size || variables.toSet.size < variables.size) {
      throw new IllegalArgumentException("Input must create unique variables")
    }
    nodes.foreach(node => newNode(varFor(node)))
    appendAtCurrentIndent(LeafOperator(Input(nodes.toArray, variables.toArray, nullable)(_)))
  }

  def filter(predicateStrings: String*): IMPL = {
    val predicates = predicateStrings.map(ExpressionParser.parseExpression)
    appendAtCurrentIndent(UnaryOperator(lp => Selection(predicates, lp)(_)))
  }

  // SHIP IP

  protected def buildLogicalPlan(): LogicalPlan = tree.build()

  // ABSTRACT METHODS

  /**
    * Called everytime a new node is introduced by some logical operator.
    */
  def newNode(node: Variable): Unit

  /**
    * Returns the finalized output of the builder.
    */
  def build(readOnly: Boolean = true): T

  // HELPERS

  private def appendAtCurrentIndent(operatorBuilder: OperatorBuilder): IMPL = {
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

      case 0 => // append to rhs
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
  private def varFor(name: String): Variable = Variable(name)(pos)
  private def labelName(s: String): LabelName = LabelName(s)(pos)
  private def literalInt(value: Long): SignedDecimalIntegerLiteral =
    SignedDecimalIntegerLiteral(value.toString)(pos)

}

object AbstractLogicalPlanBuilder {
  val pos = new InputPosition(0, 1, 0)
}
