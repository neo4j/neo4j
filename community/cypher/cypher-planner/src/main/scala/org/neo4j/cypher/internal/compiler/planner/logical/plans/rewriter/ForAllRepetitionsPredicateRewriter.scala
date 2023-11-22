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

import org.neo4j.cypher.internal.ir.ast.ForAllRepetitions
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp

/**
 * Rewrites [[ForAllRepetitions]] predicates into expressions that the runtime can evaluate.
 */
case class ForAllRepetitionsPredicateRewriter(anonymousVariableNameGenerator: AnonymousVariableNameGenerator)
    extends Rewriter {

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case far: ForAllRepetitions =>
      far.asAllIterablePredicate(anonymousVariableNameGenerator)
  })

  override def apply(value: AnyRef): AnyRef = instance(value)
}
