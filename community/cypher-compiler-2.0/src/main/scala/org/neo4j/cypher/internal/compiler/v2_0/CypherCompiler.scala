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

import org.neo4j.cypher.internal.compiler.v2_0.parser.CypherParser
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.verifiers.{OptionalPatternWithoutStartVerifier, HintVerifier}
import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.compiler.v2_0.spi.PlanContext
import org.neo4j.cypher.internal.compiler.v2_0.executionplan.{ExecutionPlanBuilder, ExecutionPlan}
import org.neo4j.cypher.internal.compiler.v2_0.commands.AbstractQuery
import org.neo4j.graphdb.GraphDatabaseService


case class CypherCompiler(graph: GraphDatabaseService, queryCache: (String, => Object) => Object) {
  val parser = CypherParser()
  val verifiers = Seq(HintVerifier, OptionalPatternWithoutStartVerifier)

  @throws(classOf[SyntaxException])
  def prepare(query: String, context: PlanContext): ExecutionPlan = {
    val cachedQuery = queryCache(query, {
      val parsedQuery = parser.parseToQuery(query)
      parsedQuery.verifySemantics()
      verify(parsedQuery)
      parsedQuery
    }).asInstanceOf[AbstractQuery]

    val planBuilder = new ExecutionPlanBuilder(graph)
    planBuilder.build(context, cachedQuery)
  }

  def verify(query: AbstractQuery) {
    for (verifier <- verifiers)
      verifier.verify(query)
  }
}
