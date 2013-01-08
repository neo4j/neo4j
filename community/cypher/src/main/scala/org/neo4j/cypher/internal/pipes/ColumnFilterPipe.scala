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
package org.neo4j.cypher.internal.pipes

import org.neo4j.cypher.internal.symbols._
import org.neo4j.cypher.internal.commands.expressions.Identifier
import org.neo4j.cypher.internal.commands.expressions.Identifier.isNamed
import org.neo4j.cypher.internal.commands.expressions.CachedExpression
import org.neo4j.cypher.internal.commands.ReturnItem

class ColumnFilterPipe(source: Pipe, val returnItems: Seq[ReturnItem])
  extends PipeWithSource(source) {
  val returnItemNames = returnItems.map(_.name)
  val symbols = new SymbolTable(identifiers2.toMap)

  private lazy val identifiers2: Seq[(String, CypherType)] = returnItems.
    map( ri => ri.name->ri.expression.getType(source.symbols))

  def createResults(state: QueryState) = {
    source.createResults(state).map(ctx => {
      val newMap = MutableMaps.create(ctx.size)

      returnItems.foreach {
        case ReturnItem(Identifier(oldName), newName, _) if isNamed(newName) => newMap.put(newName, ctx(oldName))
        case ReturnItem(CachedExpression(oldName, _), newName, _)            => newMap.put(newName, ctx(oldName))
        case ReturnItem(_, name, _)                                          => newMap.put(name, ctx(name))
      }

      ctx.newFrom( newMap )
    })
  }

  override def executionPlan(): String =
    "%s\r\nColumnFilter([%s] => [%s])".format(source.executionPlan(), source.symbols.keys, returnItemNames.mkString(","))

  def dependencies = Seq()

  def assertTypes(symbols: SymbolTable) {
    returnItems.foreach(_.expression.assertTypes(symbols))
  }
}