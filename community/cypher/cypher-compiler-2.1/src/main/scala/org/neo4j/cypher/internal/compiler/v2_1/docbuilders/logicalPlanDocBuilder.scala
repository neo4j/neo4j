/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.docbuilders

import org.neo4j.cypher.internal.compiler.v2_1.perty.{NestedDocGenerator, Doc, CachingDocBuilder}
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.LogicalPlan

object logicalPlanDocBuilder extends CachingDocBuilder[Any] {

  import Doc._

  override protected def newNestedDocGenerator: NestedDocGenerator[Any] = {
    case plan: LogicalPlan => (inner) =>
      val optLeft = plan.lhs
      val optRight = plan.rhs
      val childPlans: Set[Any] = optLeft.toSet ++ optRight.toSet

      val arguments =
        plan
          .productIterator
          .filter( (v: Any) => !childPlans.contains(v) )
          .map(inner)

      val deps = sepList(plan.availableSymbols.map(inner), break = breakSilent)
      val depsBlock = block(plan.productPrefix, open = "[", close = "]")(deps)
      val head = block(depsBlock)(sepList(arguments))

      (optLeft, optRight) match {
        case (None, None) =>
          head

        case (Some(left), None) =>
          group(page(head :/: group("↳ " :: inner(left))))

        case (Some(left), Some(right)) =>
          group(page(
            nest(head :/: group(page(
              section("↳ left =", inner(left)) :/:
              section("↳ right =", inner(right))
            )))
          ))

        case (None, Some(right)) =>
          throw new IllegalArgumentException("Right-leaning plans are not supported")
      }
  }
}
