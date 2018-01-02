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

import org.neo4j.cypher.internal.compiler.v2_3.spi.{TokenContext, QueryContext}

object TokenType extends Enumeration {
  case object Label extends TokenType {
    def getOptIdForName(name: String, tokenContext: TokenContext) = tokenContext.getOptLabelId(name)

    def getIdForNameOrFail(name: String, tokenContext: TokenContext) = tokenContext.getLabelId(name)

    def getOrCreateIdForName(name: String, queryContext: QueryContext) = queryContext.getOrCreateLabelId(name)
  }

  case object PropertyKey extends TokenType {
    def getOptIdForName(name: String, tokenContext: TokenContext) = tokenContext.getOptPropertyKeyId(name)

    def getIdForNameOrFail(name: String, tokenContext: TokenContext) = tokenContext.getPropertyKeyId(name)

    def getOrCreateIdForName(name: String, queryContext: QueryContext) = queryContext.getOrCreatePropertyKeyId(name)
  }

  case object RelType extends TokenType {
    def getOptIdForName(name: String, tokenContext: TokenContext) = tokenContext.getOptRelTypeId(name)

    def getIdForNameOrFail(name: String, tokenContext: TokenContext) = tokenContext.getRelTypeId(name)

    def getOrCreateIdForName(name: String, queryContext: QueryContext) = queryContext.getOrCreateRelTypeId(name)
  }
}

trait TokenType  {
  def apply(name: String) = KeyToken.Unresolved(name, this)

  def apply(name: String, id: Int) = KeyToken.Resolved(name, id, this)

  def getOptIdForName(name: String, tokenContext: TokenContext): Option[Int]

  def getIdForNameOrFail(name: String, tokenContext: TokenContext): Int

  def getOrCreateIdForName(name: String, queryContext: QueryContext): Int
}


