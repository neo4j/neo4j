/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.concurrency

import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.helpers.GraphIcing
import org.neo4j.cypher.internal.spi.v2_3.{TransactionBoundPlanContext, TransactionBoundQueryContext}
import org.neo4j.graphdb._
import org.neo4j.kernel.GraphDatabaseAPI
import org.neo4j.kernel.api.index.IndexDescriptor

class ConcurrentIsolationAcceptanceTests extends CypherFunSuite with GraphIcing {

  ignore("index seek stability") {
    val t = new ConcurrencyTest {
      private var index: Option[IndexDescriptor] = None

      private def indexDescriptor = index.getOrElse(fail("Init has not been run"))

      override def init(graph: GraphDatabaseAPI) {
        (1 to 100).par.foreach(_ => createNode(graph))
        graph.createIndex("A", "prop")
        index = Some(graph.inTx {
          val ctx = new TransactionBoundPlanContext(graph.statement, graph)
          ctx.getIndexRule("A", "prop").get
        })
      }

      override def read(graph: GraphDatabaseAPI) {
        graph.withTx { tx =>
          val ctx = new TransactionBoundQueryContext(graph, tx, true, graph.statement)
          val matchingNodes = ctx.exactIndexSearch(indexDescriptor, 10)
          matchingNodes.size // exhaust the iterator
          tx.success()
        }
      }

      override def updaters = Seq(
        (graph: GraphDatabaseAPI) => graph.inTx(randomNode(graph).setProperty("prop", 10)),
        (graph: GraphDatabaseAPI) => graph.inTx(randomNode(graph).setProperty("prop", 20)),
        (graph: GraphDatabaseAPI) => graph.inTx(randomNode(graph).delete()),
        (graph: GraphDatabaseAPI) => graph.inTx(createNode(graph))
      )

      private def createNode(graph: GraphDatabaseAPI) = graph.inTx {
        val n = graph.createNode(DynamicLabel.label("A"))
        n.setProperty("prop", 10)
        maxId += 1
      }
    }

    t.start()
  }
}

