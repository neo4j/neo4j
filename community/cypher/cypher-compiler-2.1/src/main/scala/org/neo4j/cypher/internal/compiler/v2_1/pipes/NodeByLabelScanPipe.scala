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
package org.neo4j.cypher.internal.compiler.v2_1.pipes

import org.neo4j.cypher.internal.compiler.v2_1.{LabelId, symbols, PlanDescription, ExecutionContext}
import symbols._

case class NodeByLabelScanPipe(ident: String, label: Either[String, LabelId]) extends Pipe {

  override protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = {
    val optLabelId = label match {
      case Left(str)      => state.query.getOptLabelId(str).map(LabelId)
      case Right(labelId) => Some(labelId)
    }

    optLabelId match {
      case Some(labelId) =>
        state.query.getNodesByLabel(labelId.id).map(n => ExecutionContext.from(ident -> n))
      case None =>
        Iterator.empty
    }
  }

  override def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  override def executionPlanDescription: PlanDescription = ???

  override def symbols: SymbolTable = new SymbolTable(Map(ident -> CTNode))
}
