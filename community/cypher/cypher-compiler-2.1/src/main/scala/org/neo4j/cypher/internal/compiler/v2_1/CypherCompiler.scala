/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1

import org.neo4j.cypher.SyntaxException
import org.neo4j.cypher.internal.compiler.v2_1.ast.{Query, Statement}
import org.neo4j.cypher.internal.compiler.v2_1.planner._
import org.neo4j.cypher.internal.compiler.v2_1.parser.CypherParser
import org.neo4j.cypher.internal.compiler.v2_1.planner.Token
import org.neo4j.cypher.internal.compiler.v2_1.runtime.ExecutionPlan
import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext

case class CypherCompiler(planContext: PlanContext, queryCache: (String, => Object) => Object) {
  val parser = CypherParser()
  val estimator = new CardinalityEstimator {
    def estimateAllNodes(): Int = 1000

    def estimateLabelScan(labelId: Token): Int = 100

    def estimateExpandRelationship(labelId: Seq[Token], relationshipType: Seq[Token], dir: Direction): Int = 10
  }

  val calculator = new CostCalculator {
    def costForExpandRelationship(cardinality: Int): Cost = new Cost(cardinality, 1)

    def costForLabelScan(cardinality: Int): Cost = new Cost(cardinality, 5)

    def costForAllNodes(cardinality: Int): Cost = new Cost(cardinality, 1)
  }

  @throws(classOf[SyntaxException])
  def prepare(query: String): Option[ExecutionPlan] = try {
    val statement: Statement = queryCache(query, parser.parse(query)).asInstanceOf[Statement]

    val queryGraph = statement match {
      case q: Query => Some(QueryGraphBuilder.build(q))
      case _ => None
    }

    queryGraph.flatMap {
      qg =>
        val planGenerator = new TemplatePlanGenerator(estimator, calculator)
        Some(planGenerator.generatePlan(planContext, qg))
    }

    None
  } catch {
    case _: Throwable => None
  }
}

class CantHandleQueryException() extends Exception
