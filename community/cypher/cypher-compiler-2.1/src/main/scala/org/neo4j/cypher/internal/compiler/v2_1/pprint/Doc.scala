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
package org.neo4j.cypher.internal.compiler.v2_1.pprint

import org.neo4j.cypher.internal.compiler.v2_1.pprint.impl.LineDocFormatter
import org.neo4j.cypher.internal.compiler.v2_1.pprint.docgen.DocStructureDocGenerator

/**
 * Class of pretty-printable documents.
 *
 * This package implements ideas from C. Lindig: "Strictly Pretty"
 * (cf. http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.34.2200&rep=rep1&type=pdf)
 *
 */
sealed abstract class Doc {
  override def toString = pformat(this, formatter = LineDocFormatter)(DocStructureDocGenerator.docGen)
}

final case class DocLiteral(doc: Doc)

object Doc {
  // sequences of docs

  def cons(head: Doc, tail: Doc = nil): Doc = ConsDoc(head, tail)
  def nil: Doc = NilDoc

  // unbreakable text doc
  implicit def text(value: String): Doc = TextDoc(value)

  // breaks are either expanded to their value or a line break

  def breakHere: Doc = BreakDoc
  def breakWith(value: String): Doc = BreakWith(value)

  // useful to force a page break if a group is in PageMode and print nothing otherwise

  def pageBreak = breakWith("")

  // *all* breaks in a group are either expanded to their value or a line break

  def group(doc: Doc): Doc = GroupDoc(doc)

  // change nesting level for inner content (used when breaks are printed as newlines)

  def nest(content: Doc): Doc = NestDoc(content)
  def nest(indent: Int, content: Doc): Doc = NestWith(indent, content)

  // helper

  implicit def list(docs: List[Doc]): Doc = docs.foldRight(nil)(cons)

  def breakList(docs: List[Doc]): Doc = docs.foldRight(nil) {
    case (hd, NilDoc) => cons(hd, nil)
    case (hd, tail)   => breakCons(hd, tail)
  }

  def sepList(docs: List[Doc], sep: Doc => Doc = frontSeparator(",")): Doc = docs.foldRight(nil) {
    case (hd, NilDoc) => cons(hd, nil)
    case (hd, tail)   => cons(hd, sep(tail))
  }

  def frontSeparator(sep: Doc): Doc => Doc =
    (tail: Doc) => breakCons(sep, tail)

  def backSeparator(sep: Doc): Doc => Doc =
    (tail: Doc) => cons(breakHere, cons(sep, tail))

  def breakCons(head: Doc, tail: Doc) = ConsDoc(head, ConsDoc(breakHere, tail))

  def scalaGroup(name: String, open: String = "(", close: String = ")")(innerDocs: List[Doc]): Doc =
    scalaDocGroup(text(name), text(open), text(close))(innerDocs)

  def scalaDocGroup(name: Doc, open: Doc = text("("), close: Doc = text(")"))(innerDocs: List[Doc]): Doc =
    group(list(List(
      name,
      open,
      nest(group(cons(pageBreak, sepList(innerDocs)))),
      pageBreak,
      close
    )))
}

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

final case class NestWith(indent: Int, content: Doc) extends NestingDoc {
  def optIndent = Some(indent)
}



