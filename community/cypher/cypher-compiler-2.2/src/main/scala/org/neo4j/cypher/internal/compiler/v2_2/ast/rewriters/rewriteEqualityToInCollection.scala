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
package org.neo4j.cypher.internal.compiler.v2_2.ast.rewriters

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.ast._

/*
This class rewrites equality predicates into IN comparisons which can then be turned into
either index lookup or node-by-id operations
 */
case object rewriteEqualityToInCollection extends Rewriter {

  override def apply(that: AnyRef) = bottomUp(instance)(that)

  private val instance: Rewriter = Rewriter.lift {
    // a.prop = b.prop are not rewritten, since they can't be turned into index lookups anyway
    case e@Equals(_:Property, _:Property) => e

    // id(a) = id(b) are also not rewritten to IN predicates since they can't be optimized at later stages
    case e@Equals(a:FunctionInvocation, b:FunctionInvocation)
      if a.function == Some(functions.Id) && b.function == Some(functions.Id) => e

    // id(a) = b.prop is also not rewritten to IN predicates as they cannot be optimized later on
    case e@Equals(a:FunctionInvocation, b:Property)
      if a.function == Some(functions.Id) => e

    // a.prop = id(b) is also not rewritten to IN predicates as they cannot be optimized later on
    case e@Equals(a:Property, b:FunctionInvocation)
      if b.function == Some(functions.Id) => e

    // id(a) = value => id(a) IN [value]
    case predicate@Equals(func@FunctionInvocation(_, _, IndexedSeq(idExpr)), idValueExpr)
      if func.function == Some(functions.Id) =>
      In(func, Collection(Seq(idValueExpr))(idValueExpr.position))(predicate.position)

    // a.prop = value => a.prop IN [value]
    case predicate@Equals(prop@Property(id: Identifier, propKeyName), idValueExpr) =>
      In(prop, Collection(Seq(idValueExpr))(idValueExpr.position))(predicate.position)
  }
}
