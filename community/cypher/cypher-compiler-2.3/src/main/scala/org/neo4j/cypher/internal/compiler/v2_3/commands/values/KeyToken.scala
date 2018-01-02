/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.commands.values

import org.neo4j.cypher.internal.compiler.v2_3._
import commands.expressions.Expression
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import pipes.QueryState
import org.neo4j.cypher.internal.frontend.v2_3.symbols._
import org.neo4j.cypher.internal.compiler.v2_3.spi.{TokenContext, QueryContext}

/*
KeyTokens are things with name and id. KeyTokens makes it possible to look up the id
at compile time and embed it into the execution plan if it's available.
 */
sealed abstract class KeyToken(typ: TokenType) extends Expression {

  def name: String

  def getOrCreateId(state: QueryContext): Int
  def getIdOrFail(state: TokenContext): Int
  def getOptId(state: TokenContext): Option[Int]

  def resolve(tokenContext: TokenContext): KeyToken

  def arguments = Seq.empty

  def rewrite(f: (Expression) => Expression): KeyToken = f(this).asInstanceOf[KeyToken]

  def symbolTableDependencies = Set.empty

  def apply(ctx: ExecutionContext)(implicit state: QueryState) = ???

  protected def calculateType(symbols: SymbolTable) = CTString
}

object KeyToken {

  case class Unresolved(name: String, typ: TokenType) extends KeyToken(typ) {
    def getOrCreateId(state: QueryContext): Int = typ.getOrCreateIdForName(name, state)
    def getIdOrFail(state: TokenContext): Int = typ.getIdForNameOrFail(name, state)
    def getOptId(state: TokenContext): Option[Int] = typ.getOptIdForName(name, state)

    def resolve(tokenContext: TokenContext) = getOptId(tokenContext).map(Resolved(name, _, typ)).getOrElse(this)

    override def toString:String = name
  }

  case class Resolved(name: String, id: Int, typ: TokenType) extends KeyToken(typ) {
    def getOrCreateId(state: QueryContext): Int = id
    def getIdOrFail(state: TokenContext): Int = id
    def getOptId(state: TokenContext): Option[Int] = Some(id)

    override def resolve(tokenContext: TokenContext): Resolved = this

    override def toString:String = s"$name($id)"
  }

  object Ordering extends Ordering[KeyToken] {
    def compare(x: KeyToken, y: KeyToken): Int = implicitly[Ordering[String]].compare(x.name, y.name)
  }
}

object UnresolvedLabel {
  def apply(name: String): KeyToken = KeyToken.Unresolved(name, TokenType.Label)
}

object UnresolvedProperty {
  def apply(name: String): KeyToken = KeyToken.Unresolved(name, TokenType.PropertyKey)
}

object UnresolvedRelType {
  def apply(name: String): KeyToken = KeyToken.Unresolved(name, TokenType.RelType)
}
