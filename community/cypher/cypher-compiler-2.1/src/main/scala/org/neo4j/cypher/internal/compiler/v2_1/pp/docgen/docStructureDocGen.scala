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
package org.neo4j.cypher.internal.compiler.v2_1.pp.docgen

import org.neo4j.cypher.internal.compiler.v2_1.pp._
import org.neo4j.cypher.internal.compiler.v2_1.pp.TextDoc
import org.neo4j.cypher.internal.compiler.v2_1.pp.NestDoc
import org.neo4j.cypher.internal.compiler.v2_1.pp.GroupDoc
import org.neo4j.cypher.internal.compiler.v2_1.pp.ConsDoc
import org.neo4j.cypher.internal.compiler.v2_1.pp.BreakWith

object docStructureDocGen extends DocGenerator[Doc] {
 import Doc._

 def apply(data: Doc): Doc = data match {
   case ConsDoc(hd, tl)       => cons(apply(hd), cons(TextDoc("·"), apply(tl)))
   case NilDoc                => text("ø")

   case TextDoc(value)        => text(s"${"\""}$value${"\""}")
   case BreakDoc              => breakWith("_")
   case BreakWith(value)      => breakWith(s"_${value}_")

   case GroupDoc(doc)         => group(cons(text("["), cons(apply(doc), text("]"))))
   case NestDoc(doc)          => group(cons(text("<"), cons(apply(doc), text(">"))))
   case NestWith(indent, doc) => group(cons(text(s"($indent)<"), cons(apply(doc), text(">"))))
 }
}
