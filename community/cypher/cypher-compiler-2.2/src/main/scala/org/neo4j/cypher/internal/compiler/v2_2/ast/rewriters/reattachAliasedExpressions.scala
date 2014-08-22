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

import org.neo4j.cypher.internal.compiler.v2_2.planner.SemanticTable
import org.neo4j.cypher.internal.compiler.v2_2.{InputPosition, bottomUp, Rewriter}
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.ast.Return

case class reattachAliasedExpressions(table: SemanticTable) extends Rewriter {
  def apply(that: AnyRef): Option[AnyRef] = bottomUp(instance).apply(that)

  private val instance: Rewriter = Rewriter.lift {

    case r @ Return(_, ListedReturnItems(items: Seq[ReturnItem]), orderBy, _, _) =>
      r.copy(orderBy = orderBy.map(reattachOrderByExpressions(projectionsMap(r.position, items))))(r.position)

    case r @ With(_, ListedReturnItems(items: Seq[ReturnItem]), orderBy, _, _, _) =>
      r.copy(orderBy = orderBy.map(reattachOrderByExpressions(projectionsMap(r.position, items))))(r.position)
  }

  private def projectionsMap(scopeStart: InputPosition, items: Seq[ReturnItem]) = {
    val namesInScope = table.namesInScope(scopeStart)
    items.collect { case item if !namesInScope.contains(item.name) => item.name -> item.expression }.toMap
  }

  private def reattachOrderByExpressions(projectionsMap: Map[String, Expression])(orderBy: OrderBy): OrderBy = {
    orderBy.endoRewrite(bottomUp(Rewriter.lift {
      case Identifier(name) if projectionsMap.contains(name) => projectionsMap(name)
    }))
  }
}
