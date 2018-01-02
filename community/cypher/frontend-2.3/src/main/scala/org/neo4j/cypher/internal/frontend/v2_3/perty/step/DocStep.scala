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
package org.neo4j.cypher.internal.frontend.v2_3.perty.step

import org.neo4j.cypher.internal.frontend.v2_3.perty.helpers.{LazyVal, StrictVal, TypedVal}
import org.neo4j.cypher.internal.frontend.v2_3.perty.{Doc, Extractor}

import scala.reflect.runtime.universe.TypeTag

sealed trait DocStep[+T]

// Values that needs to be converted to DocOps dynamically
//
// Uses lazy field content to be able to recover from errors
// during pretty-printing conversion
//
final class AddPretty[T](content: TypedVal[T]) extends DocStep[T] {
  def value = content.value
  def tpe = content.tag.tpe

  def apply[I >: T, O](extractor: Extractor[I, O]): Option[O] = content(extractor)

  // for debugging
  override def toString = content.toString
}

case object AddPretty {
  def apply[T : TypeTag](value: T) = new AddPretty[T](StrictVal(value))
}

case object AddPrettyLazy {
  def apply[T : TypeTag](value: => T) = new AddPretty[T](LazyVal(value))
}

sealed trait PrintableDocStep extends DocStep[Nothing]

sealed case class AddDoc(doc: Doc) extends PrintableDocStep

sealed case class AddText(text: String) extends PrintableDocStep

sealed case class AddBreak(breakWith: Option[String] = None) extends PrintableDocStep

object AddBreak extends AddBreak(None)

case object AddNoBreak extends PrintableDocStep

sealed trait PushFrame extends PrintableDocStep

case object PushGroupFrame extends PushFrame

case object PushPageFrame extends PushFrame

sealed case class PushNestFrame(indent: Option[Int] = None) extends PushFrame

object PushNestFrame extends PushNestFrame(None)

case object PopFrame extends PrintableDocStep


