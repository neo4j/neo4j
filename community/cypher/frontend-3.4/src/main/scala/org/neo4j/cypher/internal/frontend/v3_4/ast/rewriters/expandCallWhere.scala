/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.{Rewriter, bottomUp}

// Rewrites CALL proc WHERE <p> ==> CALL proc WITH * WHERE <p>
case object expandCallWhere extends Rewriter {

  private val instance = bottomUp(Rewriter.lift {
    case query@SingleQuery(clauses) =>
      val newClauses = clauses.flatMap {
        case unresolved@UnresolvedCall(_, _, _, Some(result@ProcedureResult(_, optWhere@Some(where)))) =>
          val newResult = result.copy(where = None)(result.position)
          val newUnresolved = unresolved.copy(declaredResult = Some(newResult))(unresolved.position)
          val newItems = ReturnItems(includeExisting = true, Seq.empty)(where.position)
          val newWith = With(distinct = false, newItems, PassAllGraphReturnItems(where.position), None, None, None, optWhere)(where.position)
          Seq(newUnresolved, newWith)

        case clause =>
          Some(clause)
      }
      query.copy(clauses = newClauses)(query.position)
  })

  override def apply(v: AnyRef): AnyRef =
    instance(v)
}
