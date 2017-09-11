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
package org.neo4j.cypher.internal.compatibility.v3_4.runtime.commands.expressions

import org.neo4j.cypher.internal.apa.v3_4.SyntaxException
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_4.runtime.pipes.QueryState
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.{PathValue, VirtualValues}

case class NodesFunction(path: Expression) extends NullInNullOutExpression(path) {

  override def compute(value: AnyValue, m: ExecutionContext, state: QueryState) = value match {
    case p: PathValue => VirtualValues.list(p.nodes():_*)
    case x => throw new SyntaxException("Expected " + path + " to be a path.")
  }

  def rewrite(f: (Expression) => Expression) = f(NodesFunction(path.rewrite(f)))

  def arguments = Seq(path)

  def symbolTableDependencies = path.symbolTableDependencies
}
