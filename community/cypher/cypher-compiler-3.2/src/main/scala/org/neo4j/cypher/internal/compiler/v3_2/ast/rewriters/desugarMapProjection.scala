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
package org.neo4j.cypher.internal.compiler.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.ast.Expression.{SemanticContext, _}
import org.neo4j.cypher.internal.frontend.v3_2.ast._
import org.neo4j.cypher.internal.frontend.v3_2.symbols._
import org.neo4j.cypher.internal.frontend.v3_2.{InputPosition, _}

/*
Handles rewriting map projection elements to literal entries when possible. If the user
has used an all properties selector ( n{ .* } ), we need to do the work in runtime.
In these situations, the rewriter turns as much as possible into literal entries,
so the runtime only has two cases to handle - literal entries and the special all-props selector.

We can't rewrite all the way to literal maps, since map projections yield a null map when the map_variable is null,
and the same behaviour can't be mimicked with literal maps.
 */
case class desugarMapProjection(state: SemanticState) extends Rewriter {
  def apply(that: AnyRef): AnyRef = topDown(instance).apply(that)

  private val instance: Rewriter = Rewriter.lift {
    case e@MapProjection(id, items, scope) =>

      def propertySelect(propertyPosition: InputPosition, name: String): LiteralEntry = {
        val key = PropertyKeyName(name)(propertyPosition)
        val idPos = scope.symbolTable(id.name).definition.position
        val newIdentifier = Variable(id.name)(idPos)
        val value = Property(newIdentifier, key)(propertyPosition)
        LiteralEntry(key, value)(propertyPosition)
      }

      def identifierSelect(id: Variable): LiteralEntry =
        LiteralEntry(PropertyKeyName(id.name)(id.position), id)(id.position)

      var includeAllProps = false

      val mapExpressionItems = items.flatMap {
        case x: LiteralEntry => Some(x)
        case x: AllPropertiesSelector => includeAllProps = true; None
        case PropertySelector(property: Variable) => Some(propertySelect(property.position, property.name))
        case VariableSelector(identifier: Variable) => Some(identifierSelect(identifier))
      }

      DesugaredMapProjection(id, mapExpressionItems, includeAllProps)(e.position)
  }
}

case class DesugaredMapProjection(name: Variable, items: Seq[LiteralEntry], includeAllProps: Boolean)(val position: InputPosition)
  extends Expression with SimpleTyping {
  protected def possibleTypes = CTMap

  override def semanticCheck(ctx: SemanticContext) =
    items.semanticCheck(ctx) chain
      name.ensureDefined() chain
      super.semanticCheck(ctx) ifOkChain // We need to remember the scope to later rewrite this ASTNode
      recordCurrentScope
}
