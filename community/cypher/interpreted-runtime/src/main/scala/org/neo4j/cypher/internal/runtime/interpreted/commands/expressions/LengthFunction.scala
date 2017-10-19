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
import org.neo4j.cypher.internal.runtime.interpreted.ListSupport
import org.neo4j.cypher.internal.runtime.interpreted.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.{TextValue, Values}
import org.neo4j.values.virtual.PathValue

case class LengthFunction(inner: Expression)
  extends NullInNullOutExpression(inner)
  with ListSupport {
  //NOTE all usage except for paths is deprecated
  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState): AnyValue = value match {
    case path: PathValue => Values.longValue(path.size())
    case s: TextValue  => Values.longValue(s.length())
    case x          => Values.longValue(makeTraversable(x).size())
  }

  def rewrite(f: (Expression) => Expression) = f(LengthFunction(inner.rewrite(f)))

  def arguments = Seq(inner)

  def symbolTableDependencies = inner.symbolTableDependencies

  override def toString = s"length($inner)"
}
