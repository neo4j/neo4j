/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.ast

import Expression.SemanticContext
import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.perty.Doc
import symbols._
import org.neo4j.helpers.ThisShouldNotHappenError

case class Property(map: Expression, propertyKey: PropertyKeyName)(val position: InputPosition) extends Expression with FunctionTyping {

  import Doc._

  val signatures = Vector(
    Signature(argumentTypes = Vector(CTMap), outputType = CTInteger),
    Signature(argumentTypes = Vector(CTMap), outputType = CTString),
    Signature(argumentTypes = Vector(CTMap), outputType = CTBoolean),
    Signature(argumentTypes = Vector(CTMap), outputType = CTFloat),
    Signature(argumentTypes = Vector(CTMap), outputType = CTNumber),
    Signature(argumentTypes = Vector(CTMap), outputType = CTCollection(CTInteger)),
    Signature(argumentTypes = Vector(CTMap), outputType = CTCollection(CTString)),
    Signature(argumentTypes = Vector(CTMap), outputType = CTCollection(CTBoolean)),
    Signature(argumentTypes = Vector(CTMap), outputType = CTCollection(CTFloat)),
    Signature(argumentTypes = Vector(CTMap), outputType = CTCollection(CTNumber))
  )

  override def toDoc = group(map :: "." :: propertyKey)

  protected def possibleTypes = CTAny.invariant
}

object LegacyProperty {
  def apply(map: Expression, propertyKey: PropertyKeyName, legacyOperator: String)(position: InputPosition) =
    new Property(map, propertyKey)(position) {
      override def semanticCheck(ctx: SemanticContext): SemanticCheck = legacyOperator match {
        case "?" => SemanticError(s"This syntax is no longer supported (missing properties are now returned as null). Please use (not(has(<ident>.${propertyKey.name})) OR <ident>.${propertyKey.name}=<value>) if you really need the old behavior.", position)
        case "!" => SemanticError(s"This syntax is no longer supported (missing properties are now returned as null).", position)
        case _   => throw new ThisShouldNotHappenError("Stefan", s"Invalid legacy operator $legacyOperator following access to property.")
      }
    }
}
