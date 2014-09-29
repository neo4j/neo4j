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
package org.neo4j.cypher.internal.compiler.v2_2.perty.ops

import org.neo4j.cypher.internal.compiler.v2_2.perty.Extractor

import scala.reflect.runtime.universe.TypeTag

sealed trait DocOp[+T]

sealed abstract class AddContent[T](implicit val tag: TypeTag[T]) extends DocOp[T] {
  def content: T
  def apply[I >: T, O](extractor: Extractor[I, O]) = extractor(content)
}

case object AddContent {
  def apply[T : TypeTag](value: => T) = new AddContent[T] {
    def content: T = value
  }
}

sealed trait BaseDocOp extends DocOp[Nothing]

sealed case class AddText(text: String) extends BaseDocOp

sealed case class AddBreak(breakWith: Option[String] = None) extends BaseDocOp
object AddBreak extends AddBreak(None)

case object AddNoBreak extends BaseDocOp

sealed trait PushFrame extends BaseDocOp
case object PushGroupFrame extends PushFrame
case object PushPageFrame extends PushFrame
sealed case class PushNestFrame(indent: Option[Int] = None) extends PushFrame
object PushNestFrame extends PushNestFrame(None)

case object PopFrame extends BaseDocOp


