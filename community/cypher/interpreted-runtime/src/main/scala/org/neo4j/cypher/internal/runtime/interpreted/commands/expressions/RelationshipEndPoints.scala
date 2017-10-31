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
package org.neo4j.cypher.internal.runtime.interpreted.commands.expressions

import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.CastSupport.castOrFail
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.EdgeValue

case class RelationshipEndPoints(relExpression: Expression, start: Boolean) extends Expression {
  def apply(ctx: ExecutionContext, state: QueryState): AnyValue = relExpression(ctx, state) match {
    case v if v == Values.NO_VALUE => Values.NO_VALUE
    case value =>
      val edge = castOrFail[EdgeValue](value)
      if (start) state.query.edgeGetStartNode(edge)
      else state.query.edgeGetEndNode(edge)
  }

  def arguments = Seq(relExpression)

  def rewrite(f: (Expression) => Expression): Expression = f(RelationshipEndPoints(relExpression.rewrite(f), start))

  def symbolTableDependencies: Set[String] = relExpression.symbolTableDependencies
}
