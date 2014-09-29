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

import org.neo4j.cypher.internal.compiler.v2_2.perty.{Doc, BaseDocOps, DocOps}

import scala.reflect.runtime.universe._

// DSL for the easy and well-formed construction of DocOps
//
// The DSL constructs and combines Appenders (functions from DocOps[T] => DocOps[T]).
//
// This approach removes otherwise annoying differences between DocOp[T] and Seq[DocOp[T]]
// as well as minimizes the amount of sequence concatenation.
//
// The DSL uses the stack' however stack depth shouldn't be a concern here as we support
// AddContent on this level and everything is flattened away by expandDocOps
//
trait NewPretty[T] {
  def apply(appender: Appender) = appender.asOps

  def group(ops: Appender): Appender =
    new Appender {
      def apply(append: DocOps[T]) = PushGroupFrame +: ops(PopFrame +: append)
    }

  def nest(ops: Appender): Appender =
    new Appender {
      def apply(append: DocOps[T]) = PushNestFrame +: ops(PopFrame +: append)
    }

  def page(ops: Appender): Appender =
    new Appender {
      def apply(append: DocOps[T]) = PushPageFrame +: ops(PopFrame +: append)
    }

  def break: Appender =
    new Appender {
      def apply(append: DocOps[T]) = AddBreak +: append
    }

  def breakWith(text: String): Appender =
    new Appender {
      def apply(append: DocOps[T]) = AddBreak(Some(text)) +: append
    }

  def pretty[S <: T : TypeTag](content: => S): Appender =
    new Appender {
      def apply(append: DocOps[T]) = AddPretty(content) +: append
    }

  def empty: Appender =
    new Appender {
      def apply(append: DocOps[T]) = append
    }

  def doc(doc: Doc): Appender =
    new Appender {
      def apply(append: DocOps[T]) = AddDoc(doc) +: append
    }

  implicit def text(text: String): Appender =
    new Appender {
      def apply(append: DocOps[T]) = AddText(text) +: append
    }

  implicit def putDoxOpsIntoSome(opts: DocOps[T]) = Some(opts)

  abstract sealed protected class Appender extends (DocOps[T] => DocOps[T]) {
    self =>

    def ::(hd: Appender) = new Appender {
      def apply(append: DocOps[T]) = hd(self(append))
    }

    def :/:(hd: Appender) = new Appender {
      def apply(append: DocOps[T]) = hd(AddBreak +: self(append))
    }

    def asOps: DocOps[T] = apply(Seq.empty)
  }
}

case object NewPretty extends NewPretty[Any]
