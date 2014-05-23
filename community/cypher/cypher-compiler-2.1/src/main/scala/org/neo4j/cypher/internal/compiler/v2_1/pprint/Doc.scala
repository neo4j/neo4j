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
import org.neo4j.cypher.internal.compiler.v2_1.pprint.docbuilders.docStructureDocBuilder
import scala.text.{DocBreak, DocText, DocCons, Document}

/**
 * Class of pretty-printable documents.
 *
 * This package implements ideas from C. Lindig: "Strictly Pretty"
 * (cf. http://citeseerx.ist.psu.edu/viewdoc/download?doi=10.1.1.34.2200&rep=rep1&type=pdf)
 *
 */
sealed abstract class Doc {

  import Doc._

  override def toString = pformat(this, formatter = LineDocFormatter)(docStructureDocBuilder.docGenerator)

  def ::(hd: Doc): Doc = cons(hd, this)
  def :/:(hd: Doc): Doc = cons(hd, cons(breakHere, this))
  def :?:(hd: Doc): Doc = replaceNil(hd, this)
  def :+:(hd: Doc): Doc = appendWithBreak(hd, this)

  def toOption: Option[Doc] = Some(this)
}

object Doc {
  // sequences of docs

  def cons(head: Doc, tail: Doc = nil): Doc = if (head == nil) tail else ConsDoc(head, tail)
  def nil: Doc = NilDoc

  // replace nil with default document

  def replaceNil(head: Doc, tail: Doc) = tail match {
    case NilDoc                => head
    case ConsDoc(NilDoc, next) => cons(head, next)
    case other                 => tail
  }

  // append doc with breaks but replace away nils

  def appendWithBreak(head: Doc, tail: Doc) =
    if (head == nil)
      tail
    else
      tail match {
        case NilDoc                => head
        case ConsDoc(NilDoc, next) => ConsDoc(head, ConsDoc(BreakDoc, next))
        case other                 => ConsDoc(head, ConsDoc(BreakDoc, other))
      }

  // unbreakable text doc

  implicit def text(value: String): Doc = TextDoc(value)

  // breaks are either expanded to their value or a line break

  def breakHere: Doc = BreakDoc
  def breakWith(value: String): Doc = BreakWith(value)

  // useful to force a page break if a group is in PageMode and print nothing otherwise

  def breakBefore(doc: Doc): Doc = if (doc == nil) nil else breakWith("") :: doc

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

  implicit def opt(optDoc: Option[Doc]) = optDoc.getOrElse(NilDoc)

  implicit def list(docs: TraversableOnce[Doc]): Doc = docs.foldRight(nil)(cons)

  def breakList(docs: TraversableOnce[Doc]): Doc = docs.foldRight(nil) {
    case (hd, NilDoc) => hd :: nil
    case (hd, tail)   => hd :/: tail
  }

  def breakBeforeList(docs: TraversableOnce[Doc]): Doc = docs.foldRight(nil) {
    case (hd, NilDoc) => hd :: nil
    case (hd, tail)   => hd :: breakBefore(tail)
  }

  def sepList(docs: TraversableOnce[Doc], sep: Doc = ","): Doc = docs.foldRight(nil) {
    case (hd, NilDoc) => hd :: nil
    case (hd, tail)   => hd :: sep :/: tail
  }

  def block(name: Doc, open: Doc = "(", close: Doc = ")")(innerDoc: Doc): Doc =
    group(
      name ::
      open ::
      nest(group(breakBefore(innerDoc))) ::
      breakBefore(close)
    )

  def section(start: Doc, inner: Doc): Doc = inner match {
    case NilDoc => nil
    case _      => group(start :/: nest(inner))
  }
}

final case class ConsDoc(head: Doc, tail: Doc = NilDoc) extends Doc

case object NilDoc extends Doc {
  override def toOption = None
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

final case class DocLiteral(doc: Doc) {
  override def toString =
    pformat(doc, formatter = DocFormatters.defaultLineFormatter)(docStructureDocBuilder.docGenerator)
}


