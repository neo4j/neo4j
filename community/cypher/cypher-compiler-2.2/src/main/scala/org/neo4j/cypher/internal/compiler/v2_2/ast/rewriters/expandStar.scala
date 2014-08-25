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
import org.neo4j.cypher.internal.compiler.v2_2.helpers.UnNamedNameGenerator
import org.neo4j.cypher.internal.compiler.v2_2.planner.SemanticTable
import org.neo4j.cypher.internal.compiler.v2_2.{Rewriter, bottomUp}

case class expandStar(table: SemanticTable) extends Rewriter {

  def apply(that: AnyRef): Option[AnyRef] = bottomUp(instance).apply(that)

  private val instance: Rewriter = Rewriter.lift {
    case clause: ClosingClause =>
      val scope = table.scopes(clause.position)
      clause.returnItems match {
        case retAll: ReturnAll if scope.nonEmpty =>
          val namesInScope = scope.keySet
          val identifiers = namesInScope.filter(UnNamedNameGenerator.isNamed).toSeq.sorted.map(scope)
          val items = identifiers.map(ident => AliasedReturnItem(ident, ident)(ident.position))
          clause.withReturnItems(ListedReturnItems(items)(retAll.position))
        case _ =>
          clause
      }
  }
}
