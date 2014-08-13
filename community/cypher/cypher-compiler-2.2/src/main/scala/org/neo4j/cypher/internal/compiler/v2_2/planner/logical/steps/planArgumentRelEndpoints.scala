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

object planArgumentRelEndpoints {

  def apply(inner: QueryPlan, argRels: Seq[PatternRelationship]): QueryPlan = {
    if (argRels.isEmpty)
      inner
    else {
      // TODO: Avoid calling start/endNode when possible
      val symbols = inner.availableSymbols
      val allTasks = argRels.flatMap(computeRelEndpointTasks(symbols))
      var result = inner.plan

      // TODO: Perhaps this should happen at the query graph
      val predicates = allTasks collect { case Right(predicate) => predicate}
      if (predicates.nonEmpty) {
        result = Selection(predicates, result)
      }

      val projectedRels = allTasks collect { case Left(mapping) => mapping}
      if (projectedRels.nonEmpty) {
        val existingIdentifiers = symbols.map { name => name.name -> Identifier(name.name)(InputPosition.NONE)}
        val allProjections = existingIdentifiers ++ projectedRels
        result = Projection(inner.plan, allProjections.toMap)
      }

      QueryPlan(
        result,
        inner.solved.updateTailOrSelf(_.updateGraph(_.addPatternRels(argRels)))
      )
    }
  }

  private def computeRelEndpointTasks(symbols: Set[IdName])(rel: PatternRelationship): Seq[Either[(String, Expression), Expression]] = {
    val (start, end) = rel.inOrder
    val (startDefined, unidirectional, endDefined) = (symbols(start), rel.dir != Direction.BOTH, symbols(end))
    (startDefined, unidirectional, endDefined) match {
      case (true, true, true) =>
        Seq(selectRelEndpoint(start.name, "startNode", rel), selectRelEndpoint(end.name, "endNode", rel))
      case (false, true, true) =>
        Seq(projectRelEndpoint(start.name, "startNode", rel), selectRelEndpoint(end.name, "endNode", rel))
      case (true, true, false) =>
        Seq(selectRelEndpoint(start.name, "startNode", rel), projectRelEndpoint(end.name, "endNode", rel))
      case (false, true, false) =>
        Seq(projectRelEndpoint(start.name, "startNode", rel), projectRelEndpoint(end.name, "endNode", rel))

      case (true, false, true) =>
        Seq(Right(Or(
          And(selectRelEndpointPredicate(start.name, "startNode", rel), selectRelEndpointPredicate(end.name, "endNode", rel))(NONE),
          And(selectRelEndpointPredicate(end.name, "startNode", rel), selectRelEndpointPredicate(start.name, "endNode", rel))(NONE)
        )(NONE)))

      case (true, false, false) =>
        fnargl(rel, start, end)

      case (false, false, true) =>
        fnargl(rel, end, start)

      case (false, false, false) =>
        Seq(projectRelEndpoint(start.name, "startNode", rel), projectRelEndpoint(end.name, "endNode", rel))
    }
  }

  def fnargl(rel: PatternRelationship, start: IdName, end: IdName): Seq[Either[(String, Expression), Expression]] =
    Seq(
      Left(start.name -> Or(selectRelEndpointPredicate(start.name, "startNode", rel), selectRelEndpointPredicate(start.name, "endNode", rel))(NONE)),
      Right(
        CaseExpression(
          expression = Some(Identifier(start.name)(NONE)),
          alternatives = Seq((callRelFunction("startNode", rel), callRelFunction("endNode", rel))),
          Some(callRelFunction("startNode", rel))
        )(NONE))
    )

  private def projectRelEndpoint(vname: String, fname: String, rel: PatternRelationship): Either[(String, Expression), Expression] =
    Left(vname -> FunctionInvocation(FunctionName(fname)(NONE), Identifier(rel.name.name)(NONE))(NONE))

  private def selectRelEndpoint(vname: String, fname: String, rel: PatternRelationship): Either[(String, Expression), Expression] =
    Right(selectRelEndpointPredicate(vname, fname, rel))


  def selectRelEndpointPredicate(vname: String, fname: String, rel: PatternRelationship): Expression =
    Equals(Identifier(vname)(NONE), callRelFunction(fname, rel))(NONE)

  def callRelFunction(fname: String, rel: PatternRelationship): Expression =
    FunctionInvocation(FunctionName(fname)(NONE), Identifier(rel.name.name)(NONE))(NONE)
}
