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
import org.neo4j.cypher.internal.spi.{TokenContext, QueryContext}


/*
KeyTokens are things with name and id. KeyTokens makes it possible to look up the id
at compile time and embed it into the execution plan if it's available.
 */
sealed abstract class KeyToken(typ: TokenType) extends Expression {

  def name: String

  def getOrCreateId(state: QueryContext): Long
  def getIdOrFail(state: TokenContext): Long
  def getOptId(state: TokenContext): Option[Long]

  def resolve(tokenContext: TokenContext): KeyToken

  def children = Seq.empty

  def rewrite(f: (Expression) => Expression): KeyToken = f(this).asInstanceOf[KeyToken]

  def symbolTableDependencies = Set.empty

  def apply(ctx: ExecutionContext)(implicit state: QueryState) = ???

  protected def calculateType(symbols: SymbolTable) = StringType()
}

object KeyToken {

  case class Unresolved(name: String, typ: TokenType) extends KeyToken(typ) {
    def getOrCreateId(state: QueryContext): Long = typ.getOrCreateIdForName(name, state)
    def getIdOrFail(state: TokenContext): Long = typ.getIdForNameOrFail(name, state)
    def getOptId(state: TokenContext): Option[Long] = typ.getOptIdForName(name, state)

    def resolve(tokenContext: TokenContext) = getOptId(tokenContext).map(Resolved(name, _, typ)).getOrElse(this)
  }

  case class Resolved(name: String, id: Long, typ: TokenType) extends KeyToken(typ) {
    def getOrCreateId(state: QueryContext): Long = id
    def getIdOrFail(state: TokenContext): Long = id
    def getOptId(state: TokenContext): Option[Long] = Some(id)

    override def resolve(tokenContext: TokenContext): Resolved = this
  }
}

