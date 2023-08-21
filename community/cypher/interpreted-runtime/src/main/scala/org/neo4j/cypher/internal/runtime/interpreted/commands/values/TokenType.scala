/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.values

import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.QueryContext

object TokenType extends Enumeration {

  case object Label extends TokenType {
    def getOptIdForName(name: String, tokenContext: ReadTokenContext) = tokenContext.getOptLabelId(name)

    def getIdForNameOrFail(name: String, tokenContext: ReadTokenContext) = tokenContext.getLabelId(name)

    def getOrCreateIdForName(name: String, queryContext: QueryContext) = queryContext.getOrCreateLabelId(name)
  }

  case object PropertyKey extends TokenType {
    def getOptIdForName(name: String, tokenContext: ReadTokenContext) = tokenContext.getOptPropertyKeyId(name)

    def getIdForNameOrFail(name: String, tokenContext: ReadTokenContext) = tokenContext.getPropertyKeyId(name)

    def getOrCreateIdForName(name: String, queryContext: QueryContext) = queryContext.getOrCreatePropertyKeyId(name)
  }

  case object RelType extends TokenType {
    def getOptIdForName(name: String, tokenContext: ReadTokenContext) = tokenContext.getOptRelTypeId(name)

    def getIdForNameOrFail(name: String, tokenContext: ReadTokenContext) = tokenContext.getRelTypeId(name)

    def getOrCreateIdForName(name: String, queryContext: QueryContext) = queryContext.getOrCreateRelTypeId(name)
  }
}

trait TokenType {
  def apply(name: String) = KeyToken.Unresolved(name, this)

  def apply(name: String, id: Int) = KeyToken.Resolved(name, id, this)

  def getOptIdForName(name: String, tokenContext: ReadTokenContext): Option[Int]

  def getIdForNameOrFail(name: String, tokenContext: ReadTokenContext): Int

  def getOrCreateIdForName(name: String, queryContext: QueryContext): Int
}
