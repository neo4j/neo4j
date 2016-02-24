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
package org.neo4j.cypher.internal.spi

import org.neo4j.cypher.internal.compiler.v3_0.spi.TransactionalContext
import org.neo4j.graphdb.{Lock, PropertyContainer, Transaction}
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.{ReadOperations, Statement}
import org.neo4j.kernel.api.txstate.TxStateHolder
import org.neo4j.kernel.impl.api.KernelStatement
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge

trait ExtendedTransactionalContext extends TransactionalContext {

  override type ReadOps = ReadOperations

  def newContext(): ExtendedTransactionalContext

  def isOpen: Boolean

  def graph: GraphDatabaseQueryService

  def statement: Statement

  def stateView: TxStateHolder

  // needed only for compatibility with 2.3
  def acquireWriteLock(p: PropertyContainer): Lock
}
