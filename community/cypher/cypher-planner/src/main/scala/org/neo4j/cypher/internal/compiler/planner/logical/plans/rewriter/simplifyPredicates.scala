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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.expressions.AndedPropertyInequalities
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.ExactSize
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.BottomUpMergeableRewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.ListType

case object simplifyPredicates extends Rewriter with BottomUpMergeableRewriter {
  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  override val innerRewriter: Rewriter = Rewriter.lift {
    case in @ In(exp, ListLiteral(values @ Seq(idValueExpr))) if values.size == 1 =>
      Equals(exp, idValueExpr)(in.position)

    case in @ In(exp, p @ Parameter(_, _: ListType, ExactSize(1))) =>
      Equals(exp, ContainerIndex(p, SignedDecimalIntegerLiteral("0")(p.position))(p.position))(in.position)

    // This form is used to make composite index seeks and scans
    case AndedPropertyInequalities(_, _, predicates) if predicates.size == 1 =>
      predicates.head

    case selection @ Selection(ands, _) =>
      val flatExprs = ands.exprs.flatMap {
        case AndedPropertyInequalities(_, _, predicates) => predicates.toIndexedSeq
        case x                                           => Seq(x)
      }

      selection.copy(
        predicate = ands.copy(exprs = flatExprs)(ands.position)
      )(SameId(selection.id))
  }
  private val instance: Rewriter = bottomUp(innerRewriter)
}
