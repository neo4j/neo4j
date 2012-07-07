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
package org.neo4j.cypher.internal.pipes

import org.neo4j.graphdb.{Transaction, TransactionFailureException, GraphDatabaseService}
import org.neo4j.kernel.impl.nioneo.store.InvalidRecordException
import org.neo4j.cypher.{NodeStillHasRelationshipsException, InternalException}

class CommitPipe(source: Pipe, graph: GraphDatabaseService) extends PipeWithSource(source) {
  lazy val still_has_relationships = "Node record Node\\[(\\d),.*] still has relationships".r

  def createResults(state: QueryState) = {
    lazy val tx = state.transaction match {
      case None => throw new InternalException("Expected to be in a transaction but wasn't")
      case Some(tx : Transaction) => tx
    }
    try {
      try {
        val result = source.createResults(state).toList
        tx.success()
        result
      } catch {
        case e => {
          tx.failure()
          throw e
        }
      } finally {
        tx.finish()
      }
    } catch {
      case e: TransactionFailureException => {
        if (e.getCause != null) {
          val inner = e.getCause
          if (inner.getCause != null) {
            val invalidRecord = inner.getCause
            if (invalidRecord.isInstanceOf[InvalidRecordException]) {
              invalidRecord.getMessage match {
                case still_has_relationships(id) => throw new NodeStillHasRelationshipsException(id.toLong, e)
                case _ => throw e
              }
            }
          }
        }

        throw e
      }
    }
  }

  def executionPlan() = source.executionPlan() + "\r\nTransactionBegin()"

  def symbols = source.symbols

  def dependencies = Seq()
}
