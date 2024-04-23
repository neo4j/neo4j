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

import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.logical.plans.Selection
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.Rewriter.BottomUpMergeableRewriter
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

case object fuseSelections extends Rewriter with BottomUpMergeableRewriter {

  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  override val innerRewriter: Rewriter = Rewriter.lift {
    case topSelection @ Selection(Ands(predicates1), Selection(Ands(predicates2), lhs)) =>
      Selection(Ands(predicates1 ++ predicates2)(predicates1.head.position), lhs)(SameId(topSelection.id))
  }

  private val instance: Rewriter = bottomUp(innerRewriter)
}
