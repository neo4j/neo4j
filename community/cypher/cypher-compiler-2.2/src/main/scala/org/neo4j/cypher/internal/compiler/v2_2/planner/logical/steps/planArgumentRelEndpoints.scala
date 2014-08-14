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
package org.neo4j.cypher.internal.compiler.v2_2.planner.logical.steps

import org.neo4j.cypher.internal.compiler.v2_2.InputPosition
import org.neo4j.cypher.internal.compiler.v2_2.InputPosition.NONE
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.planner.logical.plans._
import org.neo4j.graphdb.Direction

import scala.collection.mutable

object planArgumentRelEndpoints {

  def apply(inner: QueryPlan, argRels: Seq[PatternRelationship]): QueryPlan = {
    if (argRels.isEmpty)
      inner
    else {
      // TODO: Avoid calling start/endNode when possible
      // TODO: Perhaps this should happen at the query graph

      val symbols = inner.availableSymbols
      val (predicates, mappings) = computePredicatesAndMappings(symbols, argRels)
      var result = inner.plan

      if (predicates.nonEmpty) {
        result = Selection(predicates, result)
      }

      if (mappings.nonEmpty) {
        val existingIdentifiers = symbols.map { name => name.name -> Identifier(name.name)(InputPosition.NONE)}
        val allProjections = existingIdentifiers ++ mappings
        result = Projection(result, allProjections.toMap)
      }

      QueryPlan(
        result,
        inner.solved.updateTailOrSelf(_.updateGraph(_.addPatternRels(argRels)))
      )
    }
  }

  private def computePredicatesAndMappings(symbols: Set[IdName], rels: Seq[PatternRelationship]): (Seq[Expression], Seq[(String, Expression)]) = {
    val (selectionTasks, projectionTasks) = rels.foldLeft(new ArgTaskBuilder) {
      case (tasks, rel) =>
        val unidirectional = rel.dir != Direction.BOTH
        if (unidirectional) {
          val (start, end) = rel.inOrder
          val (startDefined, endDefined) = (symbols(start), symbols(end))

          tasks += (if (startDefined) selection(areEqual(start, startNode(rel))) else projection(start, startNode(rel)))
          tasks += (if (endDefined) selection(areEqual(end, endNode(rel))) else projection(end, endNode(rel)))
        } else {
          val (left, right) = (rel.left, rel.right)
          val (leftDefined, rightDefined) = (symbols(left), symbols(right))

          if (leftDefined && rightDefined) {
            tasks += selection(Or(areStartAndEndOfRel(left, right, rel), areStartAndEndOfRel(right, left, rel))(NONE))
          }
          else if (leftDefined && !rightDefined) {
            tasks += selection(Or(areEqual(left, startNode(rel)), areEqual(left, endNode(rel)))(NONE))
            tasks += projection(right, otherNode(left, rel))
          }
          else if (!leftDefined && rightDefined) {
            tasks += selection(Or(areEqual(right, startNode(rel)), areEqual(right, endNode(rel)))(NONE))
            tasks += projection(left, otherNode(right, rel))
          }
          else {
            tasks += projection(left, startNode(rel))
            tasks += projection(right, endNode(rel))
          }
        }
    }.result()
    val (predicates, mappings) = (selectionTasks.map(_.predicate), projectionTasks.map(_.mapping))
    (predicates, mappings)
  }

  private def selection(predicate: Expression) = SelectionArgTask(predicate)
  private def projection(left: IdName, value: Expression) = ProjectionArgTask(left.name -> value)
  private def otherNode(node: IdName, rel: PatternRelationship) = CaseExpression(Some(Identifier(node.name)(NONE)), Seq(startNode(rel) -> endNode(rel)), Some(startNode(rel)))(NONE)
  private def areStartAndEndOfRel(start: IdName, end: IdName, rel: PatternRelationship) = And(areEqual(start, startNode(rel)), areEqual(end, endNode(rel)))(NONE)
  private def areEqual(left: IdName, right: Expression) = Equals(Identifier(left.name)(NONE), right)(NONE)
  private def startNode(rel: PatternRelationship): Expression = FunctionInvocation(FunctionName("startNode")(NONE), Identifier(rel.name.name)(NONE))(NONE)
  private def endNode(rel: PatternRelationship): Expression = FunctionInvocation(FunctionName("endNode")(NONE), Identifier(rel.name.name)(NONE))(NONE)

  private class ArgTaskBuilder extends mutable.Builder[ArgTask, (Seq[SelectionArgTask], Seq[ProjectionArgTask])] {
    private val selections = Seq.newBuilder[SelectionArgTask]
    private val projections = Seq.newBuilder[ProjectionArgTask]

    def +=(elem: ArgTask) = elem match {
      case task: SelectionArgTask  => selections += task; this
      case task: ProjectionArgTask => projections += task; this
    }

    def result() = (selections.result(), projections.result())

    def clear() = {
      selections.clear()
      projections.clear()
      this
    }
  }

  private sealed trait ArgTask
  private case class ProjectionArgTask(mapping: (String, Expression)) extends ArgTask
  private case class SelectionArgTask(predicate: Expression) extends ArgTask
}
