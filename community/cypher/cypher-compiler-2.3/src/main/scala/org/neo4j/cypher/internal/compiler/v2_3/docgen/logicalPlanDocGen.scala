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
package org.neo4j.cypher.internal.compiler.v2_3.docgen

import org.neo4j.cypher.internal.frontend.v2_3.perty._
import org.neo4j.cypher.internal.frontend.v2_3.perty.recipe.Pretty
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, LogicalPlan}

import scala.reflect.runtime.universe.TypeTag

case object logicalPlanDocGen extends CustomDocGen[LogicalPlan] {

  import Pretty._

  def apply[X <: LogicalPlan : TypeTag](plan: X): Option[DocRecipe[Any]] = {
    val optLeft = plan.lhs
    val optRight = plan.rhs
    val childPlans: Set[Any] = optLeft.toSet ++ optRight.toSet

    val arguments =
      plan
        .productIterator
        .filter((v: Any) => !childPlans.contains(v))
        .toSeq
        .map(pretty[Any])

    val sortedDeps = plan.availableSymbols.toSeq.sorted(IdName.byName)
    val deps = sepList(sortedDeps.map(pretty[IdName]), break = silentBreak)
    val prefix = plan.productPrefix :: brackets(deps, break = noBreak)
    val head = block(prefix)(sepList(arguments))

    val result = (optLeft, optRight) match {
      case (None, None) =>
        head

      case (Some(left), None) =>
        val leftAppender = group("↳ " :: pretty(left))
        group(page(head :/: leftAppender))

      case (Some(left), Some(right)) =>
        val leftAppender = section("↳ left =")(pretty(left))
        val rightAppender = section("↳ right =")(pretty(right))
        group(page(
          nest(head :/: group(page(leftAppender :/: rightAppender)))
        ))

      case (None, Some(right)) =>
        throw new IllegalArgumentException("Right-leaning plans are not supported")
    }

    Pretty(result)
  }
}
