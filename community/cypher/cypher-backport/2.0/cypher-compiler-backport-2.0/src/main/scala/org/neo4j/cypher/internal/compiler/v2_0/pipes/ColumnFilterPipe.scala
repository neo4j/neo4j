/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_0.pipes

import org.neo4j.cypher.internal.compiler.v2_0._
import commands._
import commands.expressions._
import commands.expressions.Identifier.isNamed
import data.SimpleVal
import symbols._

class ColumnFilterPipe(source: Pipe, val returnItems: Seq[ReturnItem])
  extends PipeWithSource(source) {
  val returnItemNames: Seq[String] = returnItems.map(_.name)
  val symbols = SymbolTable(identifiers2.toMap)

  private lazy val identifiers2: Seq[(String, CypherType)] = returnItems.
    map( ri => ri.name->ri.expression.getType(source.symbols))

  protected def internalCreateResults(input: Iterator[ExecutionContext], state: QueryState) = {
    input.map(ctx => {
      val newMap = MutableMaps.create(ctx.size)

      returnItems.foreach {
        case ReturnItem(Identifier(oldName), newName, _) if isNamed(newName) => newMap.put(newName, ctx(oldName))
        case ReturnItem(CachedExpression(oldName, _), newName, _)            => newMap.put(newName, ctx(oldName))
        case ReturnItem(_, name, _)                                          => newMap.put(name, ctx(name))
      }

      ctx.newFrom( newMap )
    })
  }

  override def executionPlanDescription =
    source.executionPlanDescription
      .andThen(this, "ColumnFilter",
        "symKeys" -> SimpleVal.fromIterable(source.symbols.keys),
        "returnItemNames" -> SimpleVal.fromIterable(returnItemNames))

  def dependencies = Seq()
}
