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
package org.neo4j.cypher.internal.helpers

import org.neo4j.graphdb.{DynamicLabel, Node}
import org.neo4j.graphdb.DynamicLabel._
import org.neo4j.kernel.{ThreadToStatementContextBridge, GraphDatabaseAPI}
import collection.JavaConverters._
import java.util.concurrent.TimeUnit
import org.neo4j.kernel.api.StatementContext

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

    def createConstraint(label:String, property: String) {
      inTx {
        graph.schema().constraintFor(DynamicLabel.label(label)).on(property).unique().create()
      }
    }

    def createIndex(label:String, property:String) {
      val indexDef = inTx {
        graph.schema().indexFor(DynamicLabel.label(label)).on(property).create()
      }

      graph.schema().awaitIndexOnline(indexDef, 10, TimeUnit.SECONDS)
    }

    def statementContextForReading: StatementContext = graph.
      getDependencyResolver.
      resolveDependency(classOf[ThreadToStatementContextBridge]).
      getCtxForReading

    def inTx[T](f:  => T):T = {
      val tx = graph.beginTx()
      try {
        val result = f
        tx.success()
        result
      } finally {
        tx.finish()
      }
    }
  }
}