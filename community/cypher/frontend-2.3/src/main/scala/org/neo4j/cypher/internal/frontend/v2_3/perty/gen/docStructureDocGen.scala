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
package org.neo4j.cypher.internal.frontend.v2_3.perty.gen

import scala.reflect.runtime.universe._

import org.neo4j.cypher.internal.frontend.v2_3.perty._
import org.neo4j.cypher.internal.frontend.v2_3.perty.format.quoteString
import org.neo4j.cypher.internal.frontend.v2_3.perty.recipe.Pretty

// Print the structure of a document using syntax similar to
// the paper by C. Lindig
case object docStructureDocGen extends CustomDocGen[Doc] {

  import Pretty._

  def apply[X <: Doc : TypeTag](x: X): Option[DocRecipe[Any]] = x match {
    case ConsDoc(hd, tl)       => Pretty(pretty(hd) :: " ⸬ " :: pretty(tl))
    case NilDoc                => Pretty("ø")

    case TextDoc(value)        => Pretty(quoteString(value))
    case BreakDoc              => Pretty(breakWith("·"))
    case BreakWith(value)      => Pretty(breakWith( if (value.size == 0) "⁃" else s"·$value·" ))

    case GroupDoc(doc)         => Pretty(group("[" :: pretty(doc) :: "]"))
    case NestDoc(doc)          => Pretty(nest("<" :: pretty(doc) :: ">"))
    case NestWith(indent, doc) => Pretty(group(s"($indent)<" :: pretty(doc) :: ">"))

    case PageDoc(doc)          => Pretty(group("(|" :: pretty(doc) :: "|)"))
  }
}
