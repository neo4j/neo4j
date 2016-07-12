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
package org.neo4j.cypher.internal.compiler.v2_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.helpers.CollectionSupport
import org.neo4j.cypher.internal.compiler.v2_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.graphdb.Path

case class SizeFunction(inner: Expression)
  extends NullInNullOutExpression(inner)
  with CollectionSupport {
  def compute(value: Any, m: ExecutionContext)(implicit state: QueryState) = value match {
    case _: Path    => throw new CypherTypeException("SIZE cannot be used on paths")
    case s: String  => s.length()
    case x          => makeTraversable(x).toSeq.length
  }

  def rewrite(f: (Expression) => Expression) = f(LengthFunction(inner.rewrite(f)))

  def arguments = Seq(inner)

  def calculateType(symbols: SymbolTable) = CTInteger

  def symbolTableDependencies = inner.symbolTableDependencies

  override def toString = s"size($inner)"
}
