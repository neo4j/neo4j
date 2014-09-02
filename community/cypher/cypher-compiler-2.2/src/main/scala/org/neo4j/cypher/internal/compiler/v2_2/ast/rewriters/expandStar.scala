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

import org.neo4j.cypher.internal.compiler.v2_2.{Foldable, bottomUp, Rewriter}
import org.neo4j.cypher.internal.compiler.v2_2.ast._
import org.neo4j.cypher.internal.compiler.v2_2.helpers.UnNamedNameGenerator

case object expandStar extends Rewriter {

  def apply(that: AnyRef): Option[AnyRef] = bottomUp(instance).apply(that)

  private val instance: Rewriter = Rewriter.lift {
    case x: ReturnAll =>
      val identifiers = x.seenIdentifiers.get.filter(UnNamedNameGenerator.isNamed).toSeq.sorted
      val returnItems: Seq[ReturnItem] = identifiers.map { id =>
        val expr = Identifier(id)(x.position)
        val alias = Identifier(id)(x.position)
        AliasedReturnItem(expr, alias)(x.position)
      }
      ListedReturnItems(returnItems)(x.position)
  }
}

