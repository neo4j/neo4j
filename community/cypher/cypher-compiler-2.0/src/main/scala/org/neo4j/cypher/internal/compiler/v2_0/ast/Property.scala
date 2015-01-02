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
package org.neo4j.cypher.internal.compiler.v2_0.ast

import Expression.SemanticContext
import org.neo4j.cypher.internal.compiler.v2_0._
import symbols._
import org.neo4j.helpers.ThisShouldNotHappenError

case class Property(map: Expression, identifier: Identifier)(val position: InputPosition) extends Expression with SimpleTyping {
  protected def possibleTypes = CTAny.covariant

  override def semanticCheck(ctx: SemanticContext) =
    map.semanticCheck(ctx) then
    map.expectType(CTMap.covariant) then
    super.semanticCheck(ctx)
}

object LegacyProperty {
  def apply(map: Expression, identifier: Identifier, legacyOperator: String)(position: InputPosition) =
    new Property(map, identifier)(position) {
      override def semanticCheck(ctx: SemanticContext): SemanticCheck = legacyOperator match {
        case "?" => SemanticError(s"This syntax is no longer supported (missing properties are now returned as null). Please use (not(has(<ident>.${identifier.name})) OR <ident>.${identifier.name}=<value>) if you really need the old behavior.", position)
        case "!" => SemanticError(s"This syntax is no longer supported (missing properties are now returned as null).", position)
        case _   => throw new ThisShouldNotHappenError("Stefan", s"Invalid legacy operator $legacyOperator following access to property.")
      }
    }
}
