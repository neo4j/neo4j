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
package org.neo4j.cypher.internal.compiler.v2_1.pprint.docgen

import org.neo4j.cypher.internal.compiler.v2_1.pprint._
import org.neo4j.cypher.internal.compiler.v2_1.pprint.TextDoc
import org.neo4j.cypher.internal.compiler.v2_1.pprint.NestDoc
import org.neo4j.cypher.internal.compiler.v2_1.pprint.GroupDoc
import org.neo4j.cypher.internal.compiler.v2_1.pprint.ConsDoc
import org.neo4j.cypher.internal.compiler.v2_1.pprint.BreakWith
import org.neo4j.cypher.internal.helpers.PartialFunctionSupport

object DocStructureDocGenerator extends NestedDocGenerator[Doc] {

  import Doc._

  protected val instance: RecursiveDocGenerator[Doc] = {
    case ConsDoc(hd, tl)       => (inner: DocGenerator[Doc]) => cons(inner(hd), cons(TextDoc("·"), inner(tl)))
    case NilDoc                => (inner: DocGenerator[Doc]) => text("ø")

    case TextDoc(value)        => (inner: DocGenerator[Doc]) => text(s"${"\""}$value${"\""}")
    case BreakDoc              => (inner: DocGenerator[Doc]) => breakWith("_")
    case BreakWith(value)      => (inner: DocGenerator[Doc]) => breakWith(s"_${value}_")

    case GroupDoc(doc)         => (inner: DocGenerator[Doc]) => group(cons(text("["), cons(inner(doc), text("]"))))
    case NestDoc(doc)          => (inner: DocGenerator[Doc]) => group(cons(text("<"), cons(inner(doc), text(">"))))
    case NestWith(indent, doc) => (inner: DocGenerator[Doc]) => group(cons(text(s"($indent)<"), cons(inner(doc), text(">"))))
  }
}
