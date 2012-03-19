/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import org.neo4j.cypher.internal.commands.ReturnItem
import org.neo4j.cypher.internal.symbols.SymbolTable
import collection.mutable.Map

class ColumnFilterPipe(source: Pipe, val returnItems: Seq[ReturnItem])
  extends PipeWithSource(source) {
  val returnItemNames = returnItems.map(_.columnName)
  val symbols = new SymbolTable(identifiers:_*)

  private lazy val identifiers = source.symbols.identifiers.flatMap {
    // Yay! My first monad!
    case id => returnItems.find(ri=>ri.expression.identifier.name == id.name).map( x => id )
  }
    
  def createResults[U](params: Map[String, Any]): Traversable[Map[String, Any]] = {
    source.createResults(params).map(m => {
      val newMap = Map[String,Any]()
      for (ri <- returnItems) {
        val name = ri.expression.identifier.name
        val keys= m.keys.toSeq
        newMap += (ri.columnName -> m(name))
      }
      newMap
    })
  }

  override def executionPlan(): String =
    "%s\r\nColumnFilter([%s] => [%s])".format(source.executionPlan(), source.symbols.keys, returnItemNames.mkString(","))

  def dependencies = Seq()
}