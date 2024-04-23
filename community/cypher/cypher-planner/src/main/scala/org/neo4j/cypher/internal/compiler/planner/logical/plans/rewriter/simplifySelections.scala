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

import org.neo4j.cypher.internal.compiler.planner.logical.steps.skipAndLimit.planLimitOnTopOf
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.SignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.BottomUpMergeableRewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

/**
 * Removes impossible predicates from the plan. Note that this rewriter assumes
 * we have already folded things like `true AND false`, `true OR true`, etcetera.
 */
case object simplifySelections extends Rewriter with BottomUpMergeableRewriter {

  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  override val innerRewriter: Rewriter = Rewriter.lift {
    case s @ Selection(Ands(preds), source) if isFalse(preds) =>
      planLimitOnTopOf(source, SignedDecimalIntegerLiteral("0")(InputPosition.NONE))(SameId(s.id))

    case Selection(Ands(preds), source) if isTrue(preds) => source
  }

  private val instance: Rewriter = bottomUp(innerRewriter)

  private def isTrue(predicates: Iterable[Expression]): Boolean = predicates.forall {
    case _: True => true
    case _       => false
  }

  private def isFalse(predicates: Iterable[Expression]): Boolean = predicates.forall {
    case _: False => true
    case _        => false
  }
}
