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
package org.neo4j.cypher.internal.compiler.v2_1.perty

import org.neo4j.cypher.internal.compiler.v2_1.perty.docbuilders.docStructureDocBuilder

/**
 * Class of pretty-printable documents.
 *
 * This package implements ideas from C. Lindig: "Strictly Pretty"
 * (cf. http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.34.2200&rep=rep1&type=pdf)
 *
 */
sealed abstract class Doc extends docStructureDocBuilder.AsPrettyToString with HasLineDocFormatter {

  import Doc._

  def ::(hd: Doc): Doc = cons(hd, this)
  def :/:(hd: Doc): Doc = cons(hd, cons(break, this))
  def :?:(hd: Doc): Doc = replaceIfNil(hd, this)
  def :+:(hd: Doc): Doc = appendWithBreak(hd, this)

  def isNil = false

  def toOption: Option[Doc] = Some(this)

  override def docGenerator: DocGenerator[Doc] = docStructureDocBuilder.docGenerator
}

object Doc {
  // sequences of docs

  def cons(head: Doc, tail: Doc = nil): Doc = if (head == nil) tail else ConsDoc(head, tail)
  def nil: Doc = NilDoc

  // replace nil tail with default document

  def replaceIfNil(default: Doc, tail: Doc) =
    if (tail.isNil) default else tail

  // append docs with breaks but remove any nils

  def appendWithBreak(head: Doc, tail: Doc, break: BreakingDoc = break) =
    if (head.isNil) tail else if (tail.isNil) head else ConsDoc(head, breakBefore(tail, break = break))

  // unbreakable text doc

  implicit def text(value: String): Doc = TextDoc(value)

  // breaks are either expanded to their value or a line break

  val break: BreakingDoc = BreakDoc
  val breakSilent: BreakingDoc = BreakWith("")
  def breakWith(value: String): BreakingDoc = BreakWith(value)

  // useful to force a page break if a group is in PageMode and print nothing otherwise

  def breakSilentBefore(doc: Doc): Doc = breakBefore(doc, break = breakSilent)

  def breakBefore(doc: Doc, break: BreakingDoc = break): Doc =
    if (doc.isNil) doc else break :: doc

  // *all* breaks in a group are either expanded to their value or a line break

  def group(doc: Doc): Doc = GroupDoc(doc)

  // change nesting level for inner content (used when breaks are printed as newlines)

  def nest(content: Doc): Doc = NestDoc(content)
  def nest(indent: Int, content: Doc): Doc = NestWith(indent, content)

  // force vertical layout

  def page(content: Doc): Doc = PageDoc(content)

  // literals are helpful in tests to see the actual document produced instead of how it is rendered

  def literal(doc: Doc) = DocLiteral(doc)

  // helper

  implicit def opt(optDoc: Option[Doc]) = optDoc.getOrElse(nil)

  implicit def list(docs: TraversableOnce[Doc]): Doc = docs.foldRight(nil)(cons)

  def breakList(docs: TraversableOnce[Doc], break: BreakingDoc = break): Doc = docs.foldRight(nil) {
    case (hd, NilDoc) => hd :: nil
    case (hd, tail)   => hd :: break :: tail
  }

  def sepList(docs: TraversableOnce[Doc], sep: Doc = ",", break: BreakingDoc = break): Doc = docs.foldRight(nil) {
    case (hd, NilDoc) => hd :: nil
    case (hd, tail)   => hd :: sep :: break :: tail
  }

  def block(name: Doc, open: Doc = "(", close: Doc = ")")(innerDoc: Doc): Doc =
    group(
      name ::
      open ::
      nest(group(breakSilentBefore(innerDoc))) ::
      breakSilentBefore(close)
    )

  def section(start: Doc, inner: Doc, break: BreakingDoc = break): Doc =
    if (inner.isNil) inner else group(start :: nest(breakBefore(inner, break = break)))
}

final case class ConsDoc(head: Doc, tail: Doc = NilDoc) extends Doc

case object NilDoc extends Doc {
  override def toOption = None
  override def isNil = true
}

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

final case class PageDoc(content: Doc) extends ContentDoc

final case class NestWith(indent: Int, content: Doc) extends NestingDoc {
  def optIndent = Some(indent)
}

final case class DocLiteral(doc: Doc) extends Pretty[DocLiteral] {
  override def toDoc =
    Doc.block("DocLiteral")(docStructureDocBuilder.docGenerator(doc))
}


