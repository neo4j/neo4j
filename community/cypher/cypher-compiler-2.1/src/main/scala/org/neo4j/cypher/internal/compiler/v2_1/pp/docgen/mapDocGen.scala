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

import org.neo4j.cypher.internal.compiler.v2_1.pp.{Doc, DocGenerator}

case class mapDocGen[K](f: DocGenerator[Any]) extends DocGenerator[Map[K, Any]] {

  import Doc._

  def apply(m: Map[K, Any]): Doc =
    group(list(List(
      text("Map("),
      nest(group(cons(pageBreak, sepList(innerDocs(m))))),
      pageBreak,
      text(")")
    )))

  private def innerDocs(m: Map[K, Any]): List[Doc] = {
    m.map {
      case (k, v) => nest(group(breakCons(f(k), cons(text("→ "), cons(f(v))))))
    }.toList
  }
}
