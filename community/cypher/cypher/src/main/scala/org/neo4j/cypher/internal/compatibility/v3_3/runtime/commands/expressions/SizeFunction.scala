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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.ListSupport
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryState
import org.neo4j.cypher.internal.frontend.v3_4.CypherTypeException
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{TextValue, Values}
import org.neo4j.values.virtual.PathValue

case class SizeFunction(inner: Expression)
  extends NullInNullOutExpression(inner)
    with ListSupport {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = value match {
    case _: PathValue => throw new CypherTypeException("SIZE cannot be used on paths")
    case s: TextValue => Values.longValue(s.length())
    case x => Values.longValue(makeTraversable(x).size())
  }

  def rewrite(f: (Expression) => Expression) = f(LengthFunction(inner.rewrite(f)))

  def arguments = Seq(inner)

  def symbolTableDependencies = inner.symbolTableDependencies

  override def toString = s"size($inner)"
}
