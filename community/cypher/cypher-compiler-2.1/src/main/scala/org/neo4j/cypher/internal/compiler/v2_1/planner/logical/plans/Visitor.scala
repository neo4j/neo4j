/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans

import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.LogicalPlanContext
import org.neo4j.cypher.internal.compiler.v2_1.planner.QueryGraph

trait Visitor[T, R] {
  def visit(target: T): R
}

trait Visitable[T] {
  def accept[R](visitor: Visitor[T, R]): R
}

class QueryPlanTreeStringVisitor(optContext: Option[LogicalPlanContext] = None) extends Visitor[QueryPlan, String] {
  def visit(target: QueryPlan) = {
    // val planRepr = new LogicalPlanTreeStringVisitor(optContext).visit(target.plan)
    val planRepr = target.plan.toString
    val qgRepr = QueryGraphStringVisitor.visit(target.solved)

    s"QueryPlan(\nplan = $planRepr,\nsolved = $qgRepr)\n"
  }
}

class LogicalPlanTreeStringVisitor(optContext: Option[LogicalPlanContext] = None) extends Visitor[LogicalPlan, String] {
  def visit(target: LogicalPlan) = {
    val metrics = optContext match {
      case Some(context) => s"(cost ${context.cost(target)}/cardinality ${context.cardinality(target)})"
      case None => ""
    }

    target.productPrefix + s"$metrics->" +
      target.productIterator.filterNot(_.isInstanceOf[LogicalPlan]).mkString("(", ", ", ")") +
      target.lhs.map { plan => "\nleft - " + plan.accept(this) }.map(indent).getOrElse("") +
      target.rhs.map { plan => "\nright- " + plan.accept(this) }.map(indent).getOrElse("")
  }

  private def indent(s: String): String = s.lines.map { case t => "       " + t }.mkString("\n")
}


object QueryGraphStringVisitor extends Visitor[QueryGraph, String] {
  def visit(target: QueryGraph) = {
    target.toString
  }
}
