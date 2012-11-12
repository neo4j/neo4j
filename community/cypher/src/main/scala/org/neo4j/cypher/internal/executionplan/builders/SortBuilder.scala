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
package org.neo4j.cypher.internal.executionplan.builders

import org.neo4j.cypher.internal.executionplan.{PartiallySolvedQuery, PlanBuilder}
import org.neo4j.cypher.internal.pipes.{SortPipe, Pipe}

class SortBuilder extends PlanBuilder {
  def apply(p: Pipe, q: PartiallySolvedQuery) = {
    val sortExpressionsToExtract = q.sort.map(_.token).map(_.expression)

    val (pipe, newPsq) = ExtractBuilder.extractIfNecessary(q,p, sortExpressionsToExtract)

    val sortItems = newPsq.sort.map(_.token)
    val resultPipe = new SortPipe(pipe, sortItems.toList)

    (resultPipe, newPsq.copy(sort = newPsq.sort.map(_.solve)))
  }

  def isDefinedAt(p: Pipe, q: PartiallySolvedQuery) = q.extracted && q.sort.filter(_.unsolved).nonEmpty

  def priority: Int = PlanBuilder.Sort
}