/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.slotted.expressions

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.Expression
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.Predicate
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values



case class NodeProperty(offset: Int, token: Int) extends Expression with SlottedExpression {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue =
    state.query.nodeOps.getProperty(ctx.getLongAt(offset), token)
}

case class NodePropertyLate(offset: Int, propKey: String) extends Expression with SlottedExpression {

  override def apply(ctx: ExecutionContext, state: QueryState): AnyValue = {
    val maybeToken = state.query.getOptPropertyKeyId(propKey)
    if (maybeToken.isEmpty)
      Values.NO_VALUE
    else
      state.query.nodeOps.getProperty(ctx.getLongAt(offset), maybeToken.get)
  }

}

case class NodePropertyExists(offset: Int, token: Int) extends Predicate with SlottedExpression {

  override def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    Some(state.query.nodeOps.hasProperty(m.getLongAt(offset), token))
  }

  override def containsIsNull = false
}

case class NodePropertyExistsLate(offset: Int, propKey: String) extends Predicate with SlottedExpression {

  override def isMatch(m: ExecutionContext, state: QueryState): Option[Boolean] = {
    val maybeToken = state.query.getOptPropertyKeyId(propKey)
    val result = if (maybeToken.isEmpty)
      false
    else
      state.query.nodeOps.hasProperty(m.getLongAt(offset), maybeToken.get)
    Some(result)
  }

  override def containsIsNull = false
}
