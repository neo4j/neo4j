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
package org.neo4j.cypher.internal.frontend.v2_3.perty.format

import org.neo4j.cypher.internal.frontend.v2_3.perty.{ConsDoc, _}

import scala.annotation.tailrec

object LineFitter {

  def fitsDoc(width: Int, doc: Doc, mode: FormatMode = LineFormat): Boolean =
    fitsDoc(width, List(DocIndent(0, mode, doc)))

  @tailrec
  def fitsDoc(width: Int, docIndents: List[DocIndent]): Boolean =
    if (width >= 0)
      docIndents match {
        case DocIndent(indent, mode, ConsDoc(hd, tl)) :: rest =>
          fitsDoc(width, DocIndent(indent, mode, hd) :: DocIndent(indent, mode, tl) :: rest)

        case DocIndent(indent, mode, NilDoc) :: rest =>
          fitsDoc(width, rest)

        case DocIndent(indent, mode, NoBreak) :: rest =>
          fitsDoc(width, rest)

        case DocIndent(indent, mode, doc: BreakingDoc) :: rest =>
          mode == PageFormat || fitsDoc(width - doc.size, rest)

        case DocIndent(indent, mode, doc: ValueDoc) :: rest =>
          fitsDoc(width - doc.size, rest)

        case DocIndent(indent, mode, doc: ContentDoc) :: rest =>
          fitsDoc(width, DocIndent(indent, LineFormat, doc.content) :: rest)

        case nil =>
          true
    }
  else
    false
}
