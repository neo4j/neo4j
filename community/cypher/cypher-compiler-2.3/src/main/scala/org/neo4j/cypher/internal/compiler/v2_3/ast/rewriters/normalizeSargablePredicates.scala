/*
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_3.ast.{Property, InequalityExpression, Not, IsNotNull}
import org.neo4j.cypher.internal.compiler.v2_3.functions.Has
import org.neo4j.cypher.internal.compiler.v2_3.{topDown, Rewriter}

case object normalizeSargablePredicates extends Rewriter {

  override def apply(that: AnyRef): AnyRef = topDown(instance)(that)

  private val instance: Rewriter = Rewriter.lift {

    // turn n.prop IS NOT NULL into has(n.prop)
    case predicate@IsNotNull(property@Property(_, _)) =>
      Has.asInvocation(property)(predicate.position)

    // remove not from inequality expressions by negating them
    case Not(inequality: InequalityExpression) =>
      inequality.negated
  }
}
