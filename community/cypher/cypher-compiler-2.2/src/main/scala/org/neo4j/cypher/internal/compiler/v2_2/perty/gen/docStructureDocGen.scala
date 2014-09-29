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
package org.neo4j.cypher.internal.compiler.v2_2.perty.gen

import scala.reflect.runtime.universe._

import org.neo4j.cypher.internal.compiler.v2_2.perty._
import org.neo4j.cypher.internal.compiler.v2_2.perty.format.quoteString
import org.neo4j.cypher.internal.compiler.v2_2.perty.ops.NewPretty

// Print the structure of a document using the same syntax
// as the paper by C. Lindig
case object docStructureDocGen extends CustomDocGen[Doc] {

  import NewPretty._

  def apply[X <: Any : TypeTag](x: X): Option[DocOps[Any]] = x match {
    case ConsDoc(hd, tl)       => NewPretty(pretty(hd) :: "·" :: pretty(tl))
    case NilDoc                => NewPretty("ø")

    case TextDoc(value)        => NewPretty(quoteString(value))
    case BreakDoc              => NewPretty(breakWith("_"))
    case BreakWith(value)      => NewPretty(breakWith(s"_${value}_"))

    case GroupDoc(doc)         => NewPretty(group("[" :: pretty(doc) :: "]"))
    case NestDoc(doc)          => NewPretty(nest("<" :: pretty(doc) :: ">"))
    case NestWith(indent, doc) => NewPretty(group(s"($indent)<" :: pretty(doc) :: ">"))

    case PageDoc(doc)          => NewPretty(group("(|" :: pretty(doc) :: "|)"))
  }
}
