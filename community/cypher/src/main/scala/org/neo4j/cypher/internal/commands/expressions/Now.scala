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
package org.neo4j.cypher.internal.commands.expressions

import org.neo4j.cypher.{CypherTypeException, UnsupportedFormatException}
import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.ExecutionContext
import org.neo4j.cypher.internal.spi.QueryContext
import org.neo4j.cypher.internal.pipes.QueryState

case class NowFunction(arg: Expression) extends Expression {
  def apply(ctx: ExecutionContext)(implicit state: QueryState):Any = {
    arg(ctx) match {
      case "ms" => System.currentTimeMillis
      case unknown:String => throw new UnsupportedFormatException(unknown)
      case _ => throw new CypherTypeException("now() format needs to be a String")
    }
  }

  def innerExpectedType = StringType()

  def children = Seq(arg)

  def rewrite(f: (Expression) => Expression) = f(NowFunction(arg.rewrite(f)))

  def calculateType(symbols: SymbolTable) = IntegerType()

  def symbolTableDependencies = arg.symbolTableDependencies
}
