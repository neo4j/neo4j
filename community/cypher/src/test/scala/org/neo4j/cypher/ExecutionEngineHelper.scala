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
package org.neo4j.cypher

import internal.commands.AbstractQuery
import internal.helpers.GraphIcing
import internal.spi.gdsimpl.TransactionBoundQueryContext
import org.junit.Before
import org.neo4j.graphdb.{DynamicLabel, Node}
import org.neo4j.graphdb.DynamicLabel.label
import collection.JavaConverters._
import org.neo4j.kernel.{ThreadToStatementContextBridge, GraphDatabaseAPI}

trait ExecutionEngineHelper extends GraphDatabaseTestBase with GraphIcing {


  var engine: ExecutionEngine = null

  @Before
  def executionEngineHelperInit() {
    engine = new ExecutionEngine(graph)
  }

  def execute(query: AbstractQuery, params:(String,Any)*) = {
    val result = engine.execute(query, params.toMap)
    result
  }

  def parseAndExecute(q: String, params: (String, Any)*): ExecutionResult = {
    val plan = engine.prepare(q)
    val tx = graph.beginTx()

    val statementContext = graph
      .getDependencyResolver
      .resolveDependency(classOf[ThreadToStatementContextBridge])
      .getCtxForWriting

    val ctx = new TransactionBoundQueryContext(graph, tx, statementContext)
    plan.execute(ctx, params.toMap)
  }

  def executeScalar[T](q: String, params: (String, Any)*):T = engine.execute(q, params.toMap).toList match {
    case List(m) => if (m.size!=1)
      fail("expected scalar value: " + m)
      else m.head._2.asInstanceOf[T]
    case x => fail(x.toString())
  }

}