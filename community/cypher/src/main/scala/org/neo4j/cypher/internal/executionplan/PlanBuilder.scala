/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.executionplan

import org.neo4j.cypher.internal.pipes.Pipe

/*
PlanBuilders take a unsolved query, and solves another piece of it.
*/
trait PlanBuilder {
  def apply(p: Pipe, q: PartiallySolvedQuery) : (Pipe, PartiallySolvedQuery)
  def isDefinedAt(p: Pipe, q:PartiallySolvedQuery) : Boolean

  // Lower priority wins
  def priority:Int
}

// The priorities are all here, to make it easy to change and compare
// Lower priority wins
object PlanBuilder extends Enumeration {
  val CachedExpressions = -100
  val Filter = -10
  val NamedPath = -9
  val NodeById = -1
  val RelationshipById = -1
  val IndexQuery = 0
  val Extraction = 0
  val Slice = 0
  val ColumnFilter = 0
  val GlobalStart = 1
  val Match = 10
  val ShortestPath = 20
  val SortedAggregation = 30
  val Aggregation = 31
  val Sort = 40
}
