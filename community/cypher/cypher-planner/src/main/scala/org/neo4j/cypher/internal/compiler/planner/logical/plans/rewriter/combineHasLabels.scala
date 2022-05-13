/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.HasAnyLabel
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp

import scala.collection.immutable.ListSet

/**
 * Optimisation that combines multiple has label predicates, like
 * `n:SomeLabel OR n:OtherLabel` into the specialised `HasAnyLabel` expression.
 */
case object combineHasLabels extends Rewriter {

  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  private val instance: Rewriter = bottomUp(
    Rewriter.lift { case ors: Ors => rewrite(ors) }
  )

  private def rewrite(ors: Ors): Expression = {
    val (lonelyHasLabels, nonRewritable) = ors.exprs.partitionMap {
      case hasLabels @ HasLabels(_, labels) if labels.size == 1 => Left(hasLabels)
      case expression                                           => Right(expression)
    }

    val rewrittenHasLabels = ListSet.from(
      lonelyHasLabels
        .groupBy(_.expression)
        .map {
          case (_, singleHasLabels) if singleHasLabels.size == 1 => singleHasLabels.head
          case (entity, hasLabels) => HasAnyLabel(entity, hasLabels.flatMap(_.labels).toSeq)(entity.position)
        }
    )

    if (rewrittenHasLabels.size == lonelyHasLabels.size) {
      ors
    } else {
      val predicates = nonRewritable ++ rewrittenHasLabels
      predicates match {
        case singlePredicate if singlePredicate.size == 1 => singlePredicate.head
        case manyPredicates                               => Ors(manyPredicates)(ors.position)
      }
    }
  }
}
