/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.neo4j.cypher.internal.compatibility.LogicalQuery
import org.neo4j.cypher.internal.planner.v4_0.spi.PlanningAttributes.Cardinalities
import org.neo4j.cypher.internal.v4_0.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.v4_0.logical.plans._
import org.neo4j.cypher.internal.v4_0.util.attribution.{IdGen, SequentialIdGen}

import scala.collection.mutable.ArrayBuffer

/**
  * Test help utility for hand-writing logical queries.
  */
class LogicalQueryBuilder()
{
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

  /**
    * Increase indent. The indent determines where the next
    * logical plan will be appended to the tree.
    */
  def ║ : LogicalQueryBuilder = {
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

  def allNodeScan(node: String): LogicalQueryBuilder =
    appendAtCurrentIndent(LeafOperator(AllNodesScan(node, Set.empty)))

  def apply(): LogicalQueryBuilder =
    appendAtCurrentIndent(BinaryOperator((lhs, rhs) => Apply(lhs, rhs)))

  def build(): LogicalQuery = {
    val logicalPlan = tree.build()
    LogicalQuery(logicalPlan,
                 "<<queryText>>",
                 readOnly = true,
                 resultColumns,
                 new SemanticTable(),
                 new Cardinalities,
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
