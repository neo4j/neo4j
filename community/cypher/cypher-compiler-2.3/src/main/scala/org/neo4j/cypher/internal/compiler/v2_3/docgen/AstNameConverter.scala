/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.docgen

import org.neo4j.cypher.internal.frontend.v2_3.perty.recipe.Pretty

object AstNameConverter {
  import Pretty._

  def apply(name: String): AstNameConverter = name

  def isJavaIdentifier(name: String) =
    (name.length > 0) &&
      Character.isJavaIdentifierStart(name.charAt(0)) &&
      name.substring(1).forall(Character.isJavaIdentifierPart)

  implicit class AstNameConverter(name: String) extends Pretty.Converter {
    def unquote = if (isJavaIdentifier(name)) name else s"`$name`"
  }
}

