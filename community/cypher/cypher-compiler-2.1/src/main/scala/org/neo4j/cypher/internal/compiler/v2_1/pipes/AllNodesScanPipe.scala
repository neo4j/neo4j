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

import org.neo4j.cypher.internal.compiler.v2_1.{PlanDescriptionImpl, symbols, ExecutionContext}
import symbols._

case class AllNodesScanPipe(ident: String) extends Pipe {

  override protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] =
    state.query.nodeOps.all.map(n => ExecutionContext.from(ident -> n))

  override def exists(predicate: Pipe => Boolean): Boolean = predicate(this)

  override def executionPlanDescription = new PlanDescriptionImpl(this, "AllNodesScan", Seq.empty, Seq("identifier" -> ident))

  override def symbols: SymbolTable = new SymbolTable(Map(ident -> CTNode))
}
