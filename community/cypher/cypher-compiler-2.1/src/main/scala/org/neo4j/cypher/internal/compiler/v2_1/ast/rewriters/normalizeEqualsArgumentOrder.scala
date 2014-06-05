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
package org.neo4j.cypher.internal.compiler.v2_1.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_1.{topDown, Rewriter}
import org.neo4j.cypher.internal.compiler.v2_1.ast.{FunctionName, FunctionInvocation, Identifier, Equals}

object normalizeEqualsArgumentOrder extends Rewriter {
  override def apply(that: AnyRef): Option[AnyRef] = topDown(instance).apply(that)

  private val instance: Rewriter = Rewriter.lift {
    // moved identifiers on equals to the left
    case predicate @ Equals(Identifier(_), _) =>
      predicate
    case predicate @ Equals(lhs, rhs @ Identifier(_)) =>
      predicate.copy(lhs = rhs, rhs = lhs)(predicate.position)

    // move id(n) on equals to the left
    case predicate @ Equals(FunctionInvocation(FunctionName("id"), _, _), _) =>
      predicate
    case predicate @ Equals(lhs, rhs @ FunctionInvocation(FunctionName("id"), _, _)) =>
      predicate.copy(lhs = rhs, rhs = lhs)(predicate.position)
  }
}
