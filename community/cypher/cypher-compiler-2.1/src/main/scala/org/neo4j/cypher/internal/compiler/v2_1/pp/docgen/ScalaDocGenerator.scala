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
import scala.collection.immutable

object ScalaDocGenerator {

  import Doc._

  val forNestedProducts: RecursiveDocGenerator[Any] = {
    case p: Product if p.productArity == 0 => (inner: DocGenerator[Any]) =>
      text(productPrefix(p))

    case p: Product => (inner: DocGenerator[Any]) =>
      val innerDocs = p.productIterator.map(inner).toList

      group(list(List(
        text(productPrefix(p)),
        text("("),
        nest(group(cons(pageBreak, sepList(innerDocs)))),
        pageBreak,
        text(")")
      )))
  }

  private def productPrefix(p: Product) = {
    val prefix = p.productPrefix
    if (prefix.startsWith("Tuple")) "" else prefix
  }


  val forNestedMaps: RecursiveDocGenerator[Any] =  {
    case m: Map[_, _] => (inner: DocGenerator[Any]) =>
      val innerDocs = m.map { case (k, v) => nest(group(breakCons(inner(k), cons(text("→ "), cons(inner(v)))))) }.toList

      group(list(List(
        text("Map("),
        nest(group(cons(pageBreak, sepList(innerDocs)))),
        pageBreak,
        text(")")
      )))
  }

  val forNestedSets: RecursiveDocGenerator[Any] = {
    case s: collection.mutable.Set[_] => (inner: DocGenerator[Any]) =>
      val innerDocs = s.map(inner).toList
      val setType =  s.getClass.getSimpleName

      group(list(List(
        text(s"$setType("),
        nest(group(cons(pageBreak, sepList(innerDocs)))),
        pageBreak,
        text(")")
      )))

    case s: immutable.Set[_] => (inner: DocGenerator[Any]) =>
      val innerDocs = s.map(inner).toList

      group(list(List(
        text("Set("),
        nest(group(cons(pageBreak, sepList(innerDocs)))),
        pageBreak,
        text(")")
      )))
  }

  val forNestedSequences: RecursiveDocGenerator[Any] = {
    case s: Seq[_] => (inner: DocGenerator[Any]) =>
      val innerDocs = s.map(inner).toList
      val seqType = s.getClass.getSimpleName

      group(list(List(
        text(s"$seqType("),
        nest(group(cons(pageBreak, sepList(innerDocs)))),
        pageBreak,
        text(")")
      )))
  }

  val forNestedArrays: RecursiveDocGenerator[Any] = {
    case a: Array[_] => (inner: DocGenerator[Any]) =>
      val innerDocs = a.map(inner).toList

      group(list(List(
        text("Array("),
        nest(group(cons(pageBreak, sepList(innerDocs)))),
        pageBreak,
        text(")")
      )))
  }

  val forNestedLists: RecursiveDocGenerator[Any] = {
    case l: List[_] => (inner: DocGenerator[Any]) =>
      group( l.foldRight(text("nil")) { case (v, doc) => breakCons(inner(v), cons(text("⸬ "), doc)) } )
  }

  val forNestedPrimitiveValues: RecursiveDocGenerator[Any] = {
    case v: String => (inner: DocGenerator[Any]) =>
      val builder = new StringBuilder
      builder += '\"'
      var i = 0
      while (i < v.length) {
        v.charAt(i) match {
          case '\"' => builder += '\\' += '\"'
          case '\t' => builder += '\\' += 't'
          case '\b' => builder += '\\' += 'b'
          case '\n' => builder += '\\' += 'n'
          case '\r' => builder += '\\' += 'r'
          case '\\' => builder += '\\' += '\\'
          case ch   => builder += ch
        }
        i += 1
      }
      builder += '\"'
      text(builder.result())

    case ch: Char => (inner: DocGenerator[Any]) =>
      ch match {
        case '\'' => text("'\\''")
        case '\t' => text("'\\t'")
        case '\b' => text("'\\b'")
        case '\n' => text("'\\n'")
        case '\r' => text("'\\r'")
        case _    => text(s"'$ch'")
      }
  }

  val forNestedValues: RecursiveDocGenerator[Any] =
    forNestedPrimitiveValues orElse
    forNestedArrays orElse
    forNestedMaps orElse
    forNestedLists orElse
    forNestedSets orElse
    forNestedSequences orElse
    forNestedProducts
}
