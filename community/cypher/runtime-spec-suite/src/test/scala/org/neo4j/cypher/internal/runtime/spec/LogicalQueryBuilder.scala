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
package org.neo4j.cypher.internal.runtime.spec

import org.neo4j.cypher.internal.LogicalQuery
import org.neo4j.cypher.internal.ir.v4_0.SimplePatternLength
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.expressions.Expression
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.cypher.internal.v4_0.util.attribution.{IdGen, SequentialIdGen}

import scala.collection.mutable.ArrayBuffer

/**
  * Used by LogicalQueryBuilder to resolve tokens
  */
trait TokenResolver {
  /**
    * Obtain the token of a label by name.
    */
  def labelId(label: String): Int
}

/**
  * Test help utility for hand-writing logical queries.
  */
class LogicalQueryBuilder(tokenResolver: TokenResolver) extends AstConstructionTestSupport {

  private sealed trait OperatorBuilder
  private case class LeafOperator(plan: LogicalPlan) extends OperatorBuilder
  private case class UnaryOperator(planConstructor: LogicalPlan => LogicalPlan) extends OperatorBuilder
  private case class BinaryOperator(planConstructor: (LogicalPlan, LogicalPlan) => LogicalPlan) extends OperatorBuilder

  private class Tree(operator: OperatorBuilder) {
    var left: Option[Tree] = None
    var right: Option[Tree] = None

    def build(): LogicalPlan = {
      operator match {
        case LeafOperator(plan) => plan
        case UnaryOperator(planConstructor) =>
          planConstructor(left.get.build())
        case BinaryOperator(planConstructor) =>
          planConstructor(left.get.build(), right.get.build())
      }
    }
  }

  implicit val idGen: IdGen = new SequentialIdGen()

  private var tree: Tree = _
  private var looseEnds = new ArrayBuffer[Tree]
  private var indent = 0
  private var resultColumns: Array[String] = _
  private var semanticTable = new SemanticTable()

  /**
    * Increase indent. The indent determines where the next
    * logical plan will be appended to the tree.
    */
  def | : LogicalQueryBuilder = {
    indent += 1
    this
  }

  // OPERATORS

  def produceResults(vars: String*): LogicalQueryBuilder = {
    resultColumns = vars.toArray
    tree = new Tree(UnaryOperator(lp => ProduceResult(lp, resultColumns)))
    looseEnds += tree
    this
  }

  def optional(): LogicalQueryBuilder = {
    appendAtCurrentIndent(UnaryOperator(lp => Optional(lp, Set.empty)))
    this
  }

  def expand(pattern: String): LogicalQueryBuilder = {
    val p = PatternParser.parse(pattern)
    p.length match {
      case SimplePatternLength =>
        appendAtCurrentIndent(UnaryOperator(lp => Expand(lp, p.from, p.dir, p.relTypes, p.to, p.relName, ExpandAll)))
      // TODO VarExpand and PruningVarExpand
    }
    this
  }

  def optionalExpand(pattern: String, predicates: String*): LogicalQueryBuilder = {
    val p = PatternParser.parse(pattern)
    p.length match {
      case SimplePatternLength =>
        val preds = predicates.map(ExpressionParser.parseExpression)
        appendAtCurrentIndent(UnaryOperator(lp => OptionalExpand(lp, p.from, p.dir, p.relTypes, p.to, p.relName, ExpandAll, preds)))
      case _ =>
        throw new IllegalArgumentException("Cannot have optional expand with variable length pattern")
    }
    this
  }

  def partialSort(alreadySortedPrefix: Seq[ColumnOrder], stillToSortSuffix: Seq[ColumnOrder]): LogicalQueryBuilder = {
    appendAtCurrentIndent(UnaryOperator(lp => PartialSort(lp, alreadySortedPrefix, stillToSortSuffix)))
    this
  }

  def sort(sortItems: Seq[ColumnOrder]): LogicalQueryBuilder = {
    appendAtCurrentIndent(UnaryOperator(lp => Sort(lp, sortItems)))
    this
  }

  def top(sortItems: Seq[ColumnOrder], limit: Long): LogicalQueryBuilder = {
    appendAtCurrentIndent(UnaryOperator(lp => Top(lp, sortItems, literalInt(limit))))
    this
  }

  def top(sortItems: Seq[ColumnOrder], limitVariable: String): LogicalQueryBuilder = {
    appendAtCurrentIndent(UnaryOperator(lp => Top(lp, sortItems, varFor(limitVariable))))
    this
  }

  def partialTop(alreadySortedPrefix: Seq[ColumnOrder], stillToSortSuffix: Seq[ColumnOrder], limit: Long): LogicalQueryBuilder = {
    appendAtCurrentIndent(UnaryOperator(lp => PartialTop(lp, alreadySortedPrefix, stillToSortSuffix, literalInt(limit))))
    this
  }

  def partialTop(alreadySortedPrefix: Seq[ColumnOrder], stillToSortSuffix: Seq[ColumnOrder], limitVariable: String): LogicalQueryBuilder = {
    appendAtCurrentIndent(UnaryOperator(lp => PartialTop(lp, alreadySortedPrefix, stillToSortSuffix, varFor(limitVariable))))
    this
  }

  def eager(): LogicalQueryBuilder = {
    appendAtCurrentIndent(UnaryOperator(lp => Eager(lp)))
    this
  }


  def emptyResult(): LogicalQueryBuilder = {
    appendAtCurrentIndent(UnaryOperator(lp => EmptyResult(lp)))
    this
  }

  def detachDeleteNode(node: String): LogicalQueryBuilder = {
    appendAtCurrentIndent(UnaryOperator(lp => DetachDeleteNode(lp, varFor(node))))
    this
  }

  def deleteRel(rel: String): LogicalQueryBuilder = {
    appendAtCurrentIndent(UnaryOperator(lp => DeleteRelationship(lp, varFor(rel))))
    this
  }

  def expandInto(pattern: String): LogicalQueryBuilder = {
    val p = PatternParser.parse(pattern)
    appendAtCurrentIndent(UnaryOperator(lp => Expand(lp, p.from, p.dir, p.relTypes, p.to, p.relName, ExpandInto)))
    this
  }

  def projection(projectionStrings: String*): LogicalQueryBuilder = {
    val projections = ExpressionParser.parseProjections(projectionStrings: _*)
    appendAtCurrentIndent(UnaryOperator(lp => Projection(lp, projections)))
    this
  }

  def distinct(projectionStrings: String*): LogicalQueryBuilder = {
    val projections = ExpressionParser.parseProjections(projectionStrings: _*)
    appendAtCurrentIndent(UnaryOperator(lp => Distinct(lp, projections)))
    this
  }

  def allNodeScan(node: String, args: String*): LogicalQueryBuilder = {
    semanticTable = semanticTable.addNode(varFor(node))
    appendAtCurrentIndent(LeafOperator(AllNodesScan(node, args.toSet)))
  }

  def nodeByLabelScan(node: String, label: String, args: String*): LogicalQueryBuilder = {
    semanticTable = semanticTable.addNode(varFor(node))
    appendAtCurrentIndent(LeafOperator(NodeByLabelScan(node, labelName(label), args.toSet)))
  }

  def nodeIndexOperator(indexSeekString: String,
                        getValue: GetValueFromIndexBehavior = DoNotGetValue,
                        indexOrder: IndexOrder = IndexOrderNone,
                        paramExpr: Option[Expression] = None,
                        argumentIds: Set[String] = Set.empty,
                        propIds: Map[String, Int] = Map.empty,
                        unique: Boolean = false,
                        customQueryExpression: Option[QueryExpression[Expression]] = None): LogicalQueryBuilder = {
    val label = tokenResolver.labelId(IndexSeek.labelFromIndexSeekString(indexSeekString))
    val plan = IndexSeek(indexSeekString, getValue, indexOrder, paramExpr, argumentIds, propIds, label, unique, customQueryExpression)
    appendAtCurrentIndent(LeafOperator(plan))
  }

  def aggregation(groupingExpressions: Map[String, Expression],
                  aggregationExpression: Map[String, Expression]): LogicalQueryBuilder =
    appendAtCurrentIndent(UnaryOperator(lp => Aggregation(lp, groupingExpressions, aggregationExpression)))

  def apply(): LogicalQueryBuilder =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => Apply(lhs, rhs)))

  def union(): LogicalQueryBuilder =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => Union(lhs, rhs)))

  def expandAll(pattern: String): LogicalQueryBuilder = {
    val p = PatternParser.parse(pattern)
    appendAtCurrentIndent(UnaryOperator(source => Expand(source, p.from, p.dir, p.relTypes, p.to, p.relName, ExpandAll)))
  }

  def sort(sortItems: ColumnOrder*): LogicalQueryBuilder =
    appendAtCurrentIndent(UnaryOperator(source => Sort(source, sortItems)))

  def argument(): LogicalQueryBuilder =
    appendAtCurrentIndent(LeafOperator(Argument()))

  def input(variables: String*): LogicalQueryBuilder = {
    if (indent != 0)
      throw new IllegalStateException("The input operator has to be the left-most leaf of the plan")
    if (variables.toSet.size < variables.size)
      throw new IllegalArgumentException("Input must create unique variables")
    appendAtCurrentIndent(LeafOperator(Input(variables.toArray)))
  }

  // SHIP IT

  def build(readOnly: Boolean = true): LogicalQuery = {
    val logicalPlan = tree.build()
    LogicalQuery(logicalPlan,
                 "<<queryText>>",
                 readOnly,
                 resultColumns,
                 semanticTable,
                 new Cardinalities,
                 hasLoadCSV = false,
                 None)
  }

  // HELPERS

  private def appendAtCurrentIndent(operatorBuilder: OperatorBuilder): LogicalQueryBuilder = {
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
    this
  }
}
