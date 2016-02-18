/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_0.planner

trait UpdateGraph {

  def mutatingPatterns: Seq[MutatingPattern]

  def readOnly: Boolean = mutatingPatterns.isEmpty

  def containsUpdates: Boolean = !readOnly

  def containsMergeRecursive: Boolean = exists(qg => qg.mergeNodePatterns.nonEmpty || qg.mergeRelationshipPatterns.nonEmpty)

  def containsDeleteRecursive: Boolean = exists(qg => qg.deleteExpressions.nonEmpty)

  /*
   * Finds all nodes being created with CREATE (a)
   */
  def createNodePatterns = mutatingPatterns.collect {
    case p: CreateNodePattern => p
  }

  def mergeNodePatterns = mutatingPatterns.collect {
    case m: MergeNodePattern => m
  }

  def mergeRelationshipPatterns = mutatingPatterns.collect {
    case m: MergeRelationshipPattern => m
  }

  private def foreachPatterns = mutatingPatterns.collect {
    case p: ForeachPattern => p
  }

  private def deleteExpressions = mutatingPatterns.collect {
    case p: DeleteExpressionPattern => p
  }

  def mergeQueryGraph: Option[QueryGraph] = mutatingPatterns.collect {
    case c: MergePattern => c.matchGraph
  }.headOption

  /**
    * Recursively inspects UpdateGraphs contained inside Foreach's
    */
  private def exists(f: UpdateGraph => Boolean): Boolean = f(this) || foreachPatterns.exists(p => p.innerUpdates.allQueryGraphs.exists(f))
}
