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
package org.neo4j.cypher.internal.frontend.v2_3.perty

/**
 * Class of pretty-printable documents.
 *
 * This package implements ideas from C. Lindig: "Strictly Pretty"
 * (cf. http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.34.2200&rep=rep1&type=pdf)
 *
 */
sealed abstract class Doc extends LineDocFormatting

final case class ConsDoc(head: Doc, tail: Doc = NilDoc) extends Doc

case object NilDoc extends Doc

sealed abstract class ValueDoc extends Doc {
  def value: String
  def size = value.length
}

final case class TextDoc(value: String) extends ValueDoc

sealed abstract class BreakingDoc extends ValueDoc

case object BreakDoc extends BreakingDoc {
  def value = " "
}

case object NoBreak extends BreakingDoc {
  def value =""
}

final case class BreakWith(value: String) extends BreakingDoc

sealed abstract class ContentDoc extends Doc {
  def content: Doc
}

final case class GroupDoc(content: Doc) extends ContentDoc

sealed abstract class NestingDoc extends ContentDoc {
  def optIndent: Option[Int]
}

final case class NestDoc(content: Doc) extends NestingDoc {
  def optIndent = None
}

final case class PageDoc(content: Doc) extends ContentDoc

final case class NestWith(indent: Int, content: Doc) extends NestingDoc {
  def optIndent = Some(indent)
}




