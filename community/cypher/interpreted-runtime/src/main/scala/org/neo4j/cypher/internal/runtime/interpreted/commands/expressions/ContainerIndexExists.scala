/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.ExecutionContext
import org.neo4j.cypher.internal.runtime.IsNoValue
import org.neo4j.cypher.internal.runtime.interpreted.commands.AstNode
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.cypher.operations.CypherFunctions.containerIndexExists
import org.neo4j.values.storable.Values.NO_VALUE

case class ContainerIndexExists(expression: Expression, index: Expression) extends Predicate {
  override def arguments: Seq[Expression] = Seq(expression, index)

  override def children: Seq[AstNode[_]] = Seq(expression, index)

  override def isMatch(m: ExecutionContext,
                       state: QueryState): Option[Boolean] = expression(m, state) match {
        case IsNoValue() => None
        case value =>
          val idx = index(m, state)
          if (idx eq NO_VALUE) {
            None
          } else {
            Some(
              containerIndexExists(
                value,
                idx,
                state.query,
                state.cursors.nodeCursor,
                state.cursors.relationshipScanCursor,
                state.cursors.propertyCursor
              )
            )
          }
  }

  override def containsIsNull: Boolean = false
  override def rewrite(f: Expression => Expression): Expression = f(ContainerIndexExists(expression.rewrite(f), index.rewrite(f)))

}
