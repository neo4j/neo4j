/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher.internal.commands.values

import org.neo4j.cypher.internal.commands.expressions.Expression
import org.neo4j.cypher.internal.symbols.{StringType, SymbolTable}
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.pipes.QueryState

sealed abstract class LabelValue extends Expression {
  def name: String

  def id(state: QueryState): Long

  def children = Seq.empty

  def rewrite(f: (Expression) => Expression) = f(this)

  def symbolTableDependencies = Set.empty

  def apply(ctx: ExecutionContext)(implicit state: QueryState) = ???

  protected def calculateType(symbols: SymbolTable) = StringType()
}


case class LabelName(name: String) extends LabelValue {
  def id(state: QueryState): Long = state.query.getOrCreateLabelId(name)
}

case class ResolvedLabel(name: String, labelId: Long) extends LabelValue {
  def id(state: QueryState): Long = labelId
}