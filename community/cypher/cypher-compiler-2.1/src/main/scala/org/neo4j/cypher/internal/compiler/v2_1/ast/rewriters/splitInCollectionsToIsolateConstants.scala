/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.ast._

object splitInCollectionsToIsolateConstants extends Rewriter {
  override def apply(that: AnyRef) = bottomUp(instance).apply(that)

  private val instance: Rewriter = Rewriter.lift {
    case predicate@In(func@FunctionInvocation(_, _, IndexedSeq(_)), c: Collection)
      if func.function == Some(functions.Id) =>
      split(predicate, c, func)


    case predicate@In(prop@Property(_: Identifier, _), c: Collection) =>
      split(predicate, c, prop)
  }

  private def split(original: Expression, collection: Collection, expr: Expression) = {
    val (constExpr, otherExpr) = collection.expressions.partition(ConstantExpression.unapply(_).isDefined)
    if (constExpr.isEmpty || otherExpr.isEmpty)
      original
    else {
      Or(
        In(expr, Collection(constExpr)(collection.position))(original.position),
        In(expr, Collection(otherExpr)(collection.position))(original.position)
      )(original.position)
    }
  }
}
