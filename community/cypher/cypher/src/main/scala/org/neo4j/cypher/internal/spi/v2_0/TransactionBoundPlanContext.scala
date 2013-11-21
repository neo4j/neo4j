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
package org.neo4j.cypher.internal.spi.v2_0

import org.neo4j.cypher.MissingIndexException
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.kernel.api.index.{IndexDescriptor, InternalIndexState}
import org.neo4j.kernel.api.constraints.UniquenessConstraint
import org.neo4j.kernel.api.exceptions.KernelException
import org.neo4j.kernel.api.{StatementConstants, Statement}
import org.neo4j.cypher.internal.compiler.v2_0.spi.PlanContext

class TransactionBoundPlanContext(statement:Statement, gdb:GraphDatabaseService)
  extends TransactionBoundTokenContext(statement) with PlanContext {

  def getIndexRule(labelName: String, propertyKey: String): Option[IndexDescriptor] =
    evalOrNone(labelName, propertyKey, (labelId: Int, propertyKeyId: Int) =>
      getOnlineIndex(statement.readOperations().indexesGetForLabelAndPropertyKey(labelId, propertyKeyId))
    )

  def getUniqueIndexRule(labelName: String, propertyKey: String): Option[IndexDescriptor] =
    evalOrNone(labelName, propertyKey, (labelId: Int, propertyKeyId: Int) =>
      Some(statement.readOperations().uniqueIndexGetForLabelAndPropertyKey(labelId, propertyKeyId))
    )

  def getUniquenessConstraint(labelName: String, propertyKey: String): Option[UniquenessConstraint] =
    evalOrNone(labelName, propertyKey,  { (labelId: Int, propertyKeyId: Int) =>
      val matchingConstraints = statement.readOperations().constraintsGetForLabelAndPropertyKey(labelId, propertyKeyId)
      if ( matchingConstraints.hasNext ) Some(matchingConstraints.next()) else None
    } )

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

  private def evalOrNone[T](labelName: String, propertyKey: String, f: (Int, Int) => Option[T]): Option[T] =
    try {
      statement.readOperations().labelGetForName(labelName) match {
        case labelId if labelId == StatementConstants.NO_SUCH_LABEL => None
        case labelId => statement.readOperations().propertyKeyGetForName(propertyKey) match {
          case propertyKeyId if propertyKeyId == StatementConstants.NO_SUCH_PROPERTY_KEY => None
          case propertyKeyId => f(labelId, propertyKeyId)
        }
      }
    }
    catch {
      case _: KernelException             => None
    }

  private def getOnlineIndex(descriptor: IndexDescriptor): Option[IndexDescriptor] =
    statement.readOperations().indexGetState(descriptor) match {
      case InternalIndexState.ONLINE => Some(descriptor)
      case _                         => None
    }
}
