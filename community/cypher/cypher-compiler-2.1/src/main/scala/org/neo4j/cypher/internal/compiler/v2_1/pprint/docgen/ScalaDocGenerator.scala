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
import scala.collection.immutable
import scala.collection.mutable
import org.neo4j.cypher.internal.compiler.v2_1.pprint.impl.{quoteString, quoteChar}

object ScalaDocGenerator {

  import Doc._

  val forNestedProducts: RecursiveDocGenerator[Any] = {
    case p: Product if p.productArity == 0 => (inner: DocGenerator[Any]) =>
      text(productPrefix(p))

    case p: Product => (inner: DocGenerator[Any]) =>
      scalaGroup(productPrefix(p))(p.productIterator.map(inner).toList)
  }

  val forNestedMaps: RecursiveDocGenerator[Any] =  {
    case m: mutable.Map[_, _] => (inner: DocGenerator[Any]) =>
      val mapType = m.getClass.getSimpleName
      val innerDocs = m.map { case (k, v) => nest(group(breakCons(inner(k), cons(text("→ "), cons(inner(v)))))) }.toList
      scalaGroup(mapType)(innerDocs)

    case m: immutable.Map[_, _] => (inner: DocGenerator[Any]) =>
      val innerDocs = m.map { case (k, v) => nest(group(breakCons(inner(k), cons(text("→ "), cons(inner(v)))))) }.toList
      scalaGroup("Map")(innerDocs)
  }

  val forNestedSets: RecursiveDocGenerator[Any] = {
    case s: mutable.Set[_] => (inner: DocGenerator[Any]) =>
      val setType = s.getClass.getSimpleName
      val innerDocs = s.map(inner).toList
      scalaGroup(setType)(innerDocs)

    case s: immutable.Set[_] => (inner: DocGenerator[Any]) =>
      val innerDocs = s.map(inner).toList
      scalaGroup("Set")(innerDocs)
  }

  val forNestedSequences: RecursiveDocGenerator[Any] = {
    case s: Seq[_] => (inner: DocGenerator[Any]) =>
      val seqType = s.getClass.getSimpleName
      val innerDocs = s.map(inner).toList
      scalaGroup(seqType)(innerDocs)
  }

  val forNestedArrays: RecursiveDocGenerator[Any] = {
    case a: Array[_] => (inner: DocGenerator[Any]) =>
      val innerDocs = a.map(inner).toList
      scalaGroup("Array")(innerDocs)
  }

  val forNestedLists: RecursiveDocGenerator[Any] = {
    case l: List[_] => (inner: DocGenerator[Any]) =>
      group( l.foldRight(text("nil")) { case (v, doc) => breakCons(inner(v), cons(text("⸬ "), doc)) } )
  }

  val forNestedPrimitiveValues: RecursiveDocGenerator[Any] = {
    case v: String => (inner: DocGenerator[Any]) =>
      text(quoteString(v))

    case ch: Char => (inner: DocGenerator[Any]) =>
      text(quoteChar(ch))
  }

  val forNestedValues: RecursiveDocGenerator[Any] =
    forNestedPrimitiveValues orElse
    forNestedArrays orElse
    forNestedMaps orElse
    forNestedLists orElse
    forNestedSets orElse
    forNestedSequences orElse
    forNestedProducts

  private def productPrefix(p: Product) = {
    val prefix = p.productPrefix
    if (prefix.startsWith("Tuple")) "" else prefix
  }

  private def scalaGroup(name: String)(innerDocs: List[Doc]) =
    group(list(List(
      text(s"$name("),
      nest(group(cons(pageBreak, sepList(innerDocs)))),
      pageBreak,
      text(")")
    )))
}

