/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.spec.rewriters

import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.DecimalDoubleLiteral
import org.neo4j.cypher.internal.expressions.Divide
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LessThan
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.runtime.spec.rewriters.PlanRewriterContext.pos
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen

import scala.util.Random

/**
 * Randomly inserts plans that will cause runtime exceptions.
 * 
 * @param bangProbability probability of runtime failure for the inserted plans
 * @param rouletteProbability probability of inserting a sometimes failing plan at a certain point
 * @param idGen id generator
 */
case class RussianRoulette(
  bangProbability: Double,
  rouletteProbability: Double,
  idGen: IdGen,
  random: Random = Random
) extends Rewriter {

  private val rouletteRewriter: Rewriter = TestPlanRewriterTemplates.everywhere(
    rouletteProbability,
    (plan: LogicalPlan) => {
      Selection(Seq(russianRouletteExpression()), plan)(idGen)
    }
  )

  private def russianRouletteExpression(): Expression = {
    val isBulletInChamber = LessThan(
      FunctionInvocation(FunctionName("rand")(pos), distinct = false, IndexedSeq.empty)(pos),
      DecimalDoubleLiteral(bangProbability.toString)(pos)
    )(pos)
    val bang = Divide(SignedDecimalIntegerLiteral("1")(pos), SignedDecimalIntegerLiteral("0")(pos))(pos)
    CaseExpression(
      expression = None,
      alternatives = /* bang */ List(isBulletInChamber -> bang),
      default = /* click */ Some(True()(pos))
    )(pos)
  }

  override def apply(input: AnyRef): AnyRef = rouletteRewriter.apply(input)
}
