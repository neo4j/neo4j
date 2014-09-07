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

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast.Identifier

import scala.collection.immutable.HashMap

object identifierNamespacing {
  type IdentifierNames = Map[(String, InputPosition), String]

  case class IdentifierNamespacingRewriter(identifierNames: IdentifierNames) extends Rewriter {
    def apply(that: AnyRef): Option[AnyRef] = bottomUp(rewriter).apply(that)

    private val rewriter: Rewriter = Rewriter.lift {
      case i: Identifier =>
        identifierNames.get((i.name, i.position)).fold(i)(Identifier(_)(i.position))
    }
  }

  def apply(scopeTree: Scope): Rewriter = {
      val identifierNames = namespacedIdentifierNames(HashMap.empty: IdentifierNames, scopeTree)
      IdentifierNamespacingRewriter(identifierNames)
  }

  private def namespacedIdentifierNames(identifierNames: IdentifierNames, scope: Scope): IdentifierNames = {
    val updatedNames = scope.symbolTable.foldLeft(identifierNames) {
      case (acc, (name, symbol)) =>
        val firstPosition = symbol.positions.head
        symbol.positions.foldLeft(acc)((acc, position) => acc + ((name, position) -> s"  $name@${firstPosition.offset}"))
    }
    scope.children.foldLeft(updatedNames)((acc, child) => namespacedIdentifierNames(acc, child))
  }
}
