/*
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
package org.neo4j.cypher.internal.compiler.v3_0.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_0.ast._
import org.neo4j.cypher.internal.frontend.v3_0.{InputPosition, Rewriter, SemanticState, topDown}

/*
Handles rewriting map projection elements to literal entries when possible. If the user
has used an all properties selector ( n{ .* } ), we need to do the work in runtime.
In these situations, the rewriter turns as much as possible into literal entries,
so the runtime only has two cases to handle - literal entries and the special all-props selector.
 */
case class desugarMapProjection(state: SemanticState) extends Rewriter {
  def apply(that: AnyRef): AnyRef = topDown(instance).apply(that)

  private val instance: Rewriter = Rewriter.lift {
    case e@MapProjection(id, items) =>

      def propertySelect(propertyPosition: InputPosition, name: String) = {
        val key = PropertyKeyName(name)(propertyPosition)

        val scope = state.recordedScopes(e)
        val idPos = scope.symbolTable(id.name).definition.position
        val newIdentifier = Identifier(id.name)(idPos)
        val value = Property(newIdentifier, key)(propertyPosition)
        LiteralEntry(key, value)(propertyPosition)
      }

      def identifierSelect(id: Identifier) = LiteralEntry(PropertyKeyName(id.name)(id.position), id)(id.position)

      val mapExpressionItems = items.map {
        case x: LiteralEntry => x
        case x: AllPropertiesSelector => x
        case PropertySelector(property: Identifier) => propertySelect(property.position, property.name)
        case IdentifierSelector(identifier: Identifier) => identifierSelect(identifier)
      }

      MapProjection(id, mapExpressionItems)(e.position)
  }

}
