/*
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
package org.neo4j.cypher.internal.compiler.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, bottomUp}
import org.neo4j.cypher.internal.frontend.v3_2.ast._

case object expandCallWhere extends Rewriter {

  private val instance = bottomUp(Rewriter.lift {
    case query@SingleQuery(clauses) =>
      val newClauses = clauses.flatMap {
        case unresolved@UnresolvedCall(_, _, _, Some(result@ProcedureResult(_, optWhere@Some(where)))) =>
          val newResult = result.copy(where = None)(result.position)
          val newUnresolved = unresolved.copy(declaredResult = Some(newResult))(unresolved.position)
          val newItems = ReturnItems(includeExisting = true, Seq.empty)(where.position)
          val newWith = With(distinct = false, newItems, None, None, None, optWhere)(where.position)
          Seq(newUnresolved, newWith)

        case clause =>
          Some(clause)
      }
      query.copy(clauses = newClauses)(query.position)
  })

  override def apply(v: AnyRef): AnyRef =
    instance(v)
}
