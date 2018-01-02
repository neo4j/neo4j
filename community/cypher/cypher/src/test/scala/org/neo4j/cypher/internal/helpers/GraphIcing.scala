/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.helpers

import java.util.concurrent.TimeUnit

import org.neo4j.graphdb.DynamicLabel._
import org.neo4j.graphdb.{DynamicLabel, Node, Transaction}
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.Statement
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge
import org.neo4j.kernel.impl.transaction.TransactionCounters

import scala.collection.JavaConverters._

trait GraphIcing {

  implicit class RichNode(n: Node) {
    def labels: List[String] = n.getLabels.asScala.map(_.name()).toList

    def addLabels(input: String*) {
      input.foreach(l => n.addLabel(label(l)))
    }
  }

  implicit class RichGraph(graph: GraphDatabaseAPI) {

    def indexPropsForLabel(label: String): List[List[String]] = {
      val indexDefs = graph.schema.getIndexes(DynamicLabel.label(label)).asScala.toList
      indexDefs.map(_.getPropertyKeys.asScala.toList)
    }

    def createConstraint(label:String, property: String) = {
      inTx {
        graph.schema().constraintFor(DynamicLabel.label(label)).assertPropertyIsUnique(property).create()
      }
    }

    def createIndex(label: String, property: String) = {
      val indexDef = inTx {
        graph.schema().indexFor(DynamicLabel.label(label)).on(property).create()
      }

      inTx {
        graph.schema().awaitIndexOnline(indexDef, 10, TimeUnit.SECONDS)
      }

      indexDef
    }

    def statement: Statement = txBridge.get()

    // Runs code inside of a transaction. Will mark the transaction as successful if no exception is thrown
    def inTx[T](f: => T): T = withTx(_ => f)

    // Runs code inside of a transaction. Will mark the transaction as successful if no exception is thrown
    def withTx[T](f: Transaction => T): T = {
      val tx = graph.beginTx()
      try {
        val result = f(tx)
        tx.success()
        result
      } finally {
        tx.close()
      }
    }

    def txCounts = TxCounts(txMonitor.getNumberOfCommittedTransactions, txMonitor.getNumberOfRolledbackTransactions, txMonitor.getNumberOfActiveTransactions)

    private def txMonitor: TransactionCounters = graph.getDependencyResolver.resolveDependency(classOf[TransactionCounters])

    private def txBridge: ThreadToStatementContextBridge = graph.getDependencyResolver.resolveDependency(classOf[ThreadToStatementContextBridge])
  }
}

final case class TxCounts(commits: Long = 0, rollbacks: Long = 0, active: Long = 0) {
  def +(other: TxCounts) = TxCounts(commits + other.commits, rollbacks + other.rollbacks, active + other.active)
  def -(other: TxCounts) = TxCounts(commits - other.commits, rollbacks - other.rollbacks, active - other.active)
}
