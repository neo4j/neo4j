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
package org.neo4j.cypher.internal.spi.gdsimpl

import org.neo4j.cypher.internal.spi.PlanContext
import org.neo4j.cypher.MissingIndexException
import org.neo4j.kernel.api.{KernelException, StatementContext}
import org.neo4j.kernel.api.index.InternalIndexState
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.api.index.InternalIndexState

class TransactionBoundPlanContext(ctx: StatementContext, gdb:GraphDatabaseService) extends PlanContext {

  def getIndexRuleId(labelName: String, propertyKey: String): Option[Long] = try {
    val labelId = ctx.getLabelId(labelName)
    val propertyKeyId = ctx.getPropertyKeyId(propertyKey)

    val rule = ctx.getIndexRule(labelId, propertyKeyId)
    ctx.getIndexState(rule) match {
      case InternalIndexState.ONLINE => Some(rule.getId)
      case _                         => None
    }
  } catch {
    case _: KernelException => None
  }

  def checkNodeIndex(idxName: String) {
    if (!gdb.index().existsForNodes(idxName)) {
      throw new MissingIndexException(idxName)
    }
  }

  def checkRelIndex(idxName: String)  {
    if ( !gdb.index().existsForRelationships(idxName) ) {
      throw new MissingIndexException(idxName)
    }
  }

  private def tryGet[T](f: => T): Option[T] = try Some(f) catch {
    case _: KernelException => None
  }

  def getLabelId(labelName: String): Option[Long] = tryGet(ctx.getLabelId(labelName))
}