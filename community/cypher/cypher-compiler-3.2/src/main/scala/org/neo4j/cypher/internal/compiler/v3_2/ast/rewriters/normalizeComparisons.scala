/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.ast.{Equals, NotEquals, LessThan, _}
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, topDown}

case object normalizeComparisons extends Rewriter {

  override def apply(that: AnyRef): AnyRef = instance(that)

  private val instance: Rewriter = topDown(Rewriter.lift {
    case c@NotEquals(lhs, rhs) =>
      NotEquals(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c@Equals(lhs, rhs) =>
      Equals(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c@LessThan(lhs, rhs) =>
      LessThan(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c@LessThanOrEqual(lhs, rhs) =>
      LessThanOrEqual(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c@GreaterThan(lhs, rhs) =>
      GreaterThan(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c@GreaterThanOrEqual(lhs, rhs) =>
      GreaterThanOrEqual(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
    case c@InvalidNotEquals(lhs, rhs) =>
      InvalidNotEquals(lhs.endoRewrite(copyVariables), rhs.endoRewrite(copyVariables))(c.position)
  })
}
