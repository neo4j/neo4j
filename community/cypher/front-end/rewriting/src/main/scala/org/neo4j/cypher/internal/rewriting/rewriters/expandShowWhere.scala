/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.ReturnItems
import org.neo4j.cypher.internal.ast.ShowCurrentUser
import org.neo4j.cypher.internal.ast.ShowDatabase
import org.neo4j.cypher.internal.ast.ShowPrivilegeCommands
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowRoles
import org.neo4j.cypher.internal.ast.ShowUsers
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp

case object expandShowWhere extends Rewriter {

    private val instance = bottomUp(Rewriter.lift {
      case s @ ShowDatabase(_, Some(Right(where)),_) => s.copy(yieldOrWhere = Some(Left((whereToYield(where), None))))(s.position)
      case s @ ShowRoles(_, _, Some(Right(where)), _) => s.copy(yieldOrWhere = Some(Left((whereToYield(where), None))))(s.position)
      case s @ ShowPrivileges(_, Some(Right(where)),_) => s.copy(yieldOrWhere = Some(Left((whereToYield(where), None))))(s.position)
      case s @ ShowPrivilegeCommands(_, _, Some(Right(where)), _) => s.copy(yieldOrWhere = Some(Left((whereToYield(where), None))))(s.position)
      case s @ ShowUsers(Some(Right(where)),_) => s.copy(yieldOrWhere = Some(Left((whereToYield(where), None))))(s.position)
      case s @ ShowCurrentUser(Some(Right(where)),_) => s.copy(yieldOrWhere = Some(Left((whereToYield(where), None))))(s.position)
    })

    private def whereToYield(where: Where): Yield =
      Yield(ReturnItems(includeExisting = true, Seq.empty)(where.position), None, None, None, Some(where))(where.position)

  override def apply(v: AnyRef): AnyRef =
      instance(v)
  }
