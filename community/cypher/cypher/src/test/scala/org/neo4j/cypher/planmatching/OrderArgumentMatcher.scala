/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.planmatching

import org.neo4j.cypher.internal.ir.ProvidedOrder
import org.neo4j.cypher.internal.plandescription.InternalPlanDescription
import org.neo4j.cypher.internal.plandescription.Arguments.Order
import org.neo4j.cypher.internal.plandescription.PlanDescriptionArgumentSerializer.removeGeneratedNames
import org.neo4j.cypher.internal.v4_0.expressions._
import org.scalatest.matchers.{MatchResult, Matcher}

/**
  * Asserts that the Order argument of a PlanDescription contains the expected provided order.
  */
case class OrderArgumentMatcher(expected: ProvidedOrder) extends Matcher[InternalPlanDescription] {
  override def apply(plan: InternalPlanDescription): MatchResult = {
    val args = plan.arguments.collect { case Order(providedOrder) => providedOrder }
    val anonArgs = args.map(arg => ProvidedOrder(arg.columns.map(col => {
      col.expression match {
        case variable@Variable(varName) => ProvidedOrder.Column(Variable(removeGeneratedNames(varName))(variable.position), col.isAscending)
        case prop@Property(v@Variable(varName), p@PropertyKeyName(propName)) =>
          ProvidedOrder.Column(Property(Variable(removeGeneratedNames(varName))(v.position), PropertyKeyName(propName)(p.position))(prop.position), col.isAscending)
      }
    })))
    MatchResult(
      matches = anonArgs.contains(expected),
      rawFailureMessage = s"Expected ${plan.name} to have order $expected but got $anonArgs.",
      rawNegatedFailureMessage = s"Expected ${plan.name} not to have order $expected."
    )
  }
}
