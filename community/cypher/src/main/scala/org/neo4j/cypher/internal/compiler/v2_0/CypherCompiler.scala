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
package org.neo4j.cypher.internal.compiler.v2_0

import commands.AbstractQuery
import executionplan.ExecutionPlanBuilder
import executionplan.verifiers.{OptionalPatternWithoutStartVerifier, HintVerifier}
import parser.CypherParser
import org.neo4j.cypher.{ExecutionPlan, SyntaxException}
import org.neo4j.cypher.internal
import org.neo4j.cypher.internal.spi.gdsimpl.TransactionBoundPlanContext
import org.neo4j.graphdb.GraphDatabaseService

case class CypherCompiler(
  graph: GraphDatabaseService,
  queryCache: (String, => Object) => Object) extends internal.CypherCompiler
{
  val parser = CypherParser()
  val verifiers = Seq(HintVerifier, OptionalPatternWithoutStartVerifier)
  val planBuilder = new ExecutionPlanBuilder(graph)

  @throws(classOf[SyntaxException])
  def prepare(query: String, context: TransactionBoundPlanContext): ExecutionPlan = {
    val cachedQuery = queryCache(query, {
      val parsedQuery = parser.parseToQuery(query)
      parsedQuery.verifySemantics()
      verify(parsedQuery)
      parsedQuery
    }).asInstanceOf[AbstractQuery]

    planBuilder.build(context, cachedQuery)
  }

  def verify(query: AbstractQuery) {
    for (verifier <- verifiers)
      verifier.verify(query)
  }
}
