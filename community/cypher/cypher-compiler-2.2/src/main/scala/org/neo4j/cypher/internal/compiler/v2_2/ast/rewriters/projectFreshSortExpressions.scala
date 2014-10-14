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
package org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.helpers.FreshIdNameGenerator
import org.neo4j.cypher.internal.compiler.v2_2.{Rewriter, bottomUp, topDown}

/**
 * This rewriter ensures that WITH clauses containing a ORDER BY or WHERE are split, such that the ORDER BY or WHERE does not
 * refer to any newly introduced identifier.
 *
 * This is required due to constraints in the planner. Note that this structure is invalid for semantic checking, which requires
 * that ORDER BY and WHERE _only refer to identifiers introduced in the associated WITH_.
 *
 * Additionally, it splits RETURN clauses containing ORDER BY. This would typically be done earlier during normalizeReturnClauses, however
 * "RETURN * ORDER BY" is not handled at that stage, due to lacking identifier information. If expandStar has already been run, then this
 * will now work as expected.
 */
case object projectFreshSortExpressions extends Rewriter {

  def apply(that: AnyRef): AnyRef = bottomUp(instance).apply(that)

  private val clauseRewriter: (Clause => Seq[Clause]) = {
    case clause @ With(_, _, None, _, _, None) =>
      Seq(clause)

    case clause @ With(_, ri, _, _, _, _) =>
      val duplicateProjection = ri.items.map(item =>
        item.alias.fold(item)(alias => AliasedReturnItem(alias, alias)(item.position))
      )
      Seq(
        clause.copy(orderBy = None, skip = None, limit = None, where = None)(clause.position),
        clause.copy(distinct = false, returnItems = ri.copy(items = duplicateProjection)(ri.position))(clause.position)
      )

    case clause =>
      Seq(clause)
  }

  private val instance: Rewriter = Rewriter.lift {
    case query @ SingleQuery(clauses) =>
      query.copy(clauses = clauses.flatMap(clauseRewriter))(query.position)
  }
}
