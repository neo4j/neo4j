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
package org.neo4j.cypher.internal.spi.v3_0

import org.neo4j.cypher.internal.compiler.v3_0.spi.TransactionalContext
import org.neo4j.graphdb.Transaction
import org.neo4j.kernel.GraphDatabaseQueryService
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.api.txstate.TxStateHolder
import org.neo4j.kernel.impl.api.KernelStatement
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge

case class TransactionBoundTransactionalContext(graph: GraphDatabaseQueryService, initialTx: Transaction,
                                                val isTopLevelTx: Boolean, initialStatement: Statement)
  extends TransactionalContext[GraphDatabaseQueryService, Statement, TxStateHolder] {
  private var tx = initialTx
  private var open = true
  private var _statement = initialStatement
  private val txBridge = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])

  override def statement = _statement

  override def isOpen = open

  override def close(success: Boolean): Unit = {
    try {
      _statement.close()

      if (success)
        tx.success()
      else
        tx.failure()
      tx.close()
    }
    finally {
      open = false
    }
  }

  override def commitAndRestartTx() {
    tx.success()
    tx.close()

    tx = graph.beginTx()
    _statement = txBridge.get()
  }

  override def newContext() = {
    val isTopLevelTx = !txBridge.hasTransaction
    val tx = graph.beginTx()
    val statement = txBridge.get()
    new TransactionBoundTransactionalContext(graph, tx, isTopLevelTx, statement)
  }

  override def stateView: TxStateHolder = statement.asInstanceOf[KernelStatement]
}
