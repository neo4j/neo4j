/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.{topDown, Rewriter}

// TODO: Support n.prop <op> m.prop, perhaps by
//  either killing this and just looking on both lhs and rhs all over the place or
//  by duplicating the predicate and somehow ignoring it during cardinality calculation
//
case object normalizeArgumentOrder extends Rewriter {

  override def apply(that: AnyRef): AnyRef = topDown(instance)(that)

  private val instance: Rewriter = Rewriter.lift {

    // move id(n) on equals to the left
    case predicate @ Equals(func@FunctionInvocation(_, _, _), _) if func.function.contains(functions.Id) =>
      predicate

    case predicate @ Equals(lhs, rhs @ FunctionInvocation(_, _, _)) if rhs.function.contains(functions.Id) =>
      predicate.copy(lhs = rhs, rhs = lhs)(predicate.position)

    // move n.prop on equals to the left
    case predicate @ Equals(Property(_, _), _) =>
      predicate

    case predicate @ Equals(lhs, rhs @ Property(_, _)) =>
      predicate.copy(lhs = rhs, rhs = lhs)(predicate.position)

    case inequality: InequalityExpression =>
      val lhsIsProperty = inequality.lhs.isInstanceOf[Property]
      val rhsIsProperty = inequality.rhs.isInstanceOf[Property]
      if (!lhsIsProperty && rhsIsProperty) {
        inequality.swapped
      } else {
        inequality
      }
  }
}


