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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.spi.PlanContext

/*
The meta plan generator creates a plan by using other plan generators as
tools.
 */
class TemplatePlanGenerator(estimator: CardinalityEstimator, calculator: CostCalculator) {

  val init = new InitPlanGenerator(estimator, calculator)
  val expansion = new ExpansionPlanGenerator(calculator, estimator)


  def generatePlan(planContext: PlanContext, qg: QueryGraph): AbstractPlan = {
    var planTable: PlanTable = init.generatePlan(planContext, qg, PlanTable.empty)

    while (planTable.size > 1) {
      planTable = expansion.generatePlan(planContext, qg, planTable)
    }

    planTable.m.head
  }
}
