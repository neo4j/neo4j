/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_2.helpers.UnNamedNameGenerator
import org.neo4j.cypher.internal.compiler.v2_2.{SemanticState, Rewriter, bottomUp}
import org.neo4j.helpers.ThisShouldNotHappenError

case class expandStar(state: SemanticState) extends Rewriter {

  def apply(that: AnyRef): AnyRef = bottomUp(instance).apply(that)

  private val instance: Rewriter = Rewriter.lift {
    case clause@With(_, ri, _, _, _, _) if ri.includeExisting =>
      clause.copy(returnItems = returnItems(clause, ri.items))(clause.position)

    case clause: PragmaWithout =>
      With(distinct = false, returnItems = returnItems(clause, Seq.empty, clause.excludedNames), orderBy = None, skip = None, limit = None, where = None)(clause.position)

    case clause@Return(_, ri, _, _, _) if ri.includeExisting =>
      clause.copy(returnItems = returnItems(clause, ri.items))(clause.position)
  }

  private def returnItems(clause: Clause, listedItems: Seq[ReturnItem], excludedNames: Set[String] = Set.empty): ReturnItems = {
    val scope = state.scope(clause).getOrElse {
      throw new ThisShouldNotHappenError("cleishm", s"${clause.name} should note its Scope in the SemanticState")
    }

    val symbolNames = scope.symbolNames -- excludedNames
    val expandedItems = symbolNames.toSeq.sorted.map { id =>
      val expr = Identifier(id)(clause.position)
      val alias = Identifier(id)(clause.position)
      AliasedReturnItem(expr, alias)(clause.position)
    }

    ReturnItems(includeExisting = false, expandedItems ++ listedItems)(clause.position)
  }
}
