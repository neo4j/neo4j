/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.pipes.{MatchPipe, Pipe}
import org.neo4j.cypher.internal.commands._
import org.neo4j.cypher.internal.executionplan.{PlanBuilder, LegacyPlanBuilder, ExecutionPlanInProgress}
import org.neo4j.cypher.internal.symbols.{SymbolTable, NodeType}
import org.neo4j.cypher.internal.pipes.matching.{PatternRelationship, PatternNode, PatternGraph}
import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.commands.ShortestPath

class MatchBuilder extends LegacyPlanBuilder with PatternGraphBuilder {
  def apply(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    val p = plan.pipe

    val items = q.patterns.filter(yesOrNo(_, p, q.start))
    val patterns = items.map(_.token)
    val predicates = q.where.filter(!_.solved).map(_.token)
    val graph = buildPatternGraph(p.symbols, patterns)

    val mandatoryGraph = graph.mandatoryGraph

    val mandatoryPipe = if (mandatoryGraph.nonEmpty)
      new MatchPipe(p, predicates, mandatoryGraph)
    else
      p

    // We'll create one MatchPipe per DoubleOptionalPattern we have
    val newPipe = graph.doubleOptionalPatterns().
      foldLeft(mandatoryPipe)((lastPipe, patternGraph) => new MatchPipe(lastPipe, predicates, patternGraph))

    plan.copy(
      query = q.copy(patterns = q.patterns.filterNot(items.contains) ++ items.map(_.solve)),
      pipe = newPipe
    )
  }

  def canWorkWith(plan: ExecutionPlanInProgress) = {
    val q = plan.query
    q.patterns.exists(yesOrNo(_, plan.pipe, q.start))
  }

  private def yesOrNo(q: QueryToken[_], p: Pipe, start: Seq[QueryToken[StartItem]]) = q match {
    case Unsolved(x: ShortestPath) => false
    case Unsolved(x: Pattern)      => {
      val patternIdentifiers: Seq[String] = x.possibleStartPoints.map(_._1)

      val areStartPointsResolved = start.map(si => patternIdentifiers.find(_ == si.token.identifierName) match {
        case Some(_) => si.solved
        case None    => true
      }).foldLeft(true)(_ && _)

      areStartPointsResolved
    }
    case _                         => false
  }

  def priority = PlanBuilder.Match
}

trait PatternGraphBuilder {
  def buildPatternGraph(symbols: SymbolTable, patterns: Seq[Pattern]): PatternGraph = {
    val patternNodeMap: scala.collection.mutable.Map[String, PatternNode] = scala.collection.mutable.Map()
    val patternRelMap: scala.collection.mutable.Map[String, PatternRelationship] = scala.collection.mutable.Map()

    symbols.identifiers.
      filter(_._2 == NodeType()). //Find all bound nodes...
      foreach(id => patternNodeMap(id._1) = new PatternNode(id._1)) //...and create patternNodes for them

    patterns.foreach(_ match {
      case RelatedTo(left, right, rel, relType, dir, optional) => {
        val leftNode: PatternNode = patternNodeMap.getOrElseUpdate(left, new PatternNode(left))
        val rightNode: PatternNode = patternNodeMap.getOrElseUpdate(right, new PatternNode(right))

        if (patternRelMap.contains(rel)) {
          throw new SyntaxException("Can't re-use pattern relationship '%s' with different start/end nodes.".format(rel))
        }

        patternRelMap(rel) = leftNode.relateTo(rel, rightNode, relType, dir, optional)
      }
      case VarLengthRelatedTo(pathName, start, end, minHops, maxHops, relType, dir, relsCollection, optional) => {
        val startNode: PatternNode = patternNodeMap.getOrElseUpdate(start, new PatternNode(start))
        val endNode: PatternNode = patternNodeMap.getOrElseUpdate(end, new PatternNode(end))
        patternRelMap(pathName) = startNode.relateViaVariableLengthPathTo(pathName, endNode, minHops, maxHops, relType, dir, relsCollection, optional)
      }
      case _ =>
    })

    new PatternGraph(patternNodeMap.toMap, patternRelMap.toMap, symbols.keys)
  }
}