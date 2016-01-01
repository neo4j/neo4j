/**
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
package org.neo4j.cypher.internal.compiler.v2_1.perty.docbuilders

import org.neo4j.cypher.internal.compiler.v2_1.perty._
import org.neo4j.cypher.internal.compiler.v2_1.perty.impl.quoteString

case object docStructureDocBuilder extends CachingDocBuilder[Doc] with TopLevelDocBuilder[Doc] {

  import Doc._

  override protected def newNestedDocGenerator = {
    case ConsDoc(hd, tl)       => (inner) => inner(hd) :: "·" :: inner(tl)
    case NilDoc                => (inner) => "ø"

    case TextDoc(value)        => (inner) => quoteString(value)
    case BreakDoc              => (inner) => breakWith("_")
    case BreakWith(value)      => (inner) => breakWith(s"_${value}_")

    case GroupDoc(doc)         => (inner) => group("[" :: inner(doc) :: "]")
    case NestDoc(doc)          => (inner) => group("<" :: inner(doc) :: ">")
    case NestWith(indent, doc) => (inner) => group(s"($indent)<" :: inner(doc) :: ">")

    case PageDoc(doc)          => (inner) => group("(|" :: inner(doc) :: "|)")
  }
}
