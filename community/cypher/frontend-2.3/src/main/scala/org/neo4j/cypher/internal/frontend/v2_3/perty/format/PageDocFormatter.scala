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
import org.neo4j.cypher.internal.frontend.v2_3.perty.print.{PrintCommand, PrintNewLine, PrintText}

import scala.annotation.tailrec

final case class PageDocFormatter(width: Int,
                                  defaultAddIndent: Int = 2,
                                  initialMode: FormatMode = LineFormat) extends DocFormatter {

  def apply(doc: Doc): Seq[PrintCommand] =
    build(0, List(DocIndent(0, initialMode, doc)), Vector.newBuilder[PrintCommand]).result()

  @tailrec
  private def build(consumed: Int,
                    docs: List[DocIndent],
                    builder: PrintingConverter[Seq[PrintCommand]]): PrintingConverter[Seq[PrintCommand]] = docs match {

    case DocIndent(indent, mode, NilDoc) :: rest =>
      build(consumed, rest, builder)

    case DocIndent(indent, mode, NoBreak) :: rest =>
      build(consumed, rest, builder)

    case DocIndent(indent, mode, ConsDoc(hd, tl)) :: rest =>
      build(consumed, DocIndent(indent, mode, hd) :: DocIndent(indent, mode, tl) :: rest, builder)

    case DocIndent(indent, mode, doc: NestingDoc) :: rest =>
      val tail = DocIndent(indent + doc.optIndent.getOrElse(defaultAddIndent), mode, doc.content) :: rest
      build(consumed, tail, builder)

    case DocIndent(indent, PageFormat, doc: BreakingDoc) :: rest =>
      build(indent, rest, builder += PrintNewLine(indent))

    case DocIndent(indent, mode, doc: ValueDoc) :: rest =>
      build(consumed + doc.size, rest, builder += PrintText(doc.value))

    case DocIndent(indent, mode, PageDoc(content)) :: rest =>
      build(indent, DocIndent(indent, PageFormat, content) :: rest, builder)

    case DocIndent(indent, mode, doc: ContentDoc) :: rest =>
      val remaining = width - consumed
      val lineTail = DocIndent(indent, LineFormat, doc.content) :: rest

      if (LineFitter.fitsDoc(remaining, lineTail)) {
        build(consumed, lineTail, builder)
      } else {
        val pageTail = DocIndent(indent, PageFormat, doc.content) :: rest
        build(consumed, pageTail, builder)
      }

    case nil =>
      builder
  }
}
