/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.opencypher.v9_0.util.CypherTypeException
import org.neo4j.cypher.internal.runtime.interpreted.ExecutionContext
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values
import org.neo4j.values.virtual.{RelationshipValue, NodeValue}

case class IdFunction(inner: Expression) extends NullInNullOutExpression(inner) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = value match {
    case node: NodeValue => Values.longValue(node.id())
    case rel: RelationshipValue => Values.longValue(rel.id())
    case x => throw new CypherTypeException(
      "Expected `%s` to be a node or relationship, but it was `%s`".format(inner, x.getClass.getSimpleName))
  }

  def rewrite(f: (Expression) => Expression) = f(IdFunction(inner.rewrite(f)))

  def arguments = Seq(inner)

  def symbolTableDependencies = inner.symbolTableDependencies
}
