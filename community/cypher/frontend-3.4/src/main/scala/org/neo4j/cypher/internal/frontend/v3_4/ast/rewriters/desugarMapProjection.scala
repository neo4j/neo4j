/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.ast.rewriters

import org.neo4j.cypher.internal.apa.v3_4.{InputPosition, Rewriter, topDown}
import org.neo4j.cypher.internal.frontend.v3_4.ast._
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticState

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

case class DesugaredMapProjection(
                                   name: Variable,
                                   items: Seq[LiteralEntry],
                                   includeAllProps: Boolean
                                 )(val position: InputPosition) extends Expression
