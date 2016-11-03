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
package org.neo4j.cypher.internal.compiler.v3_2.executionplan.builders

import org.neo4j.cypher.internal.compiler.v3_2._
import commands.expressions.Variable
import mutation._
import org.neo4j.cypher.internal.compiler.v3_2.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_2.symbols._

/*
Expands a query. Example:

CREATE (a {name:'A'})-[:KNOWS]->(b {name:'B'})

is expanded into:

CREATE (a {name:'A'}),
       (b {name:'B'}),
       (a)-[:KNOWS]->(b)
 */
trait UpdateCommandExpander {
  def expandCommands(commands: Seq[UpdateAction], initialSymbols: SymbolTable): Seq[UpdateAction] = {

    var symbols: SymbolTable = initialSymbols
    val actions = Seq.newBuilder[UpdateAction]

    def add(action: UpdateAction) {
      actions += action
      symbols  = symbols.add(action.variables.toMap)
    }

    def addCreateNodeIfNecessary(e: RelationshipEndpoint) =
      e.node match {
        case Variable(name) if !symbols.checkType(name, CTNode) =>
          add(CreateNode(name, e.props, e.labels))
          e.asBare

        case _  =>
          e
      }

    commands.foreach {
      case foreach: ForeachAction =>
        val expandedCommands = expandCommands(foreach.actions, foreach.addInnerVariable(symbols)).toList
        add(foreach.copy(actions = expandedCommands))

      case createRel: CreateRelationship =>
        val from = addCreateNodeIfNecessary(createRel.from)
        val to = addCreateNodeIfNecessary(createRel.to)
        add(createRel.copy(from = from, to = to))

      case x =>
        add(x)
    }

    actions.result()
  }
}
