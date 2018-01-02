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
package org.neo4j.cypher.internal.compiler.v2_3.helpers

import scala.reflect.ClassTag

object PartialFunctionSupport {

  def reduceAnyDefined[A, B](functions: Seq[PartialFunction[A, B]])(init: B)(combine: (B, B) => B) = foldAnyDefined[A, B, B](functions)(init)(combine)

  def foldAnyDefined[A, B, C](functions: Seq[PartialFunction[A, B]])(init: C)(combine: (C, B) => C): PartialFunction[A, C] = new PartialFunction[A, C] {
    override def isDefinedAt(v: A) = functions.exists(_.isDefinedAt(v))
    override def apply(v: A) = functions.foldLeft(init)((acc, pf) => if (pf.isDefinedAt(v)) combine(acc, pf(v)) else acc)
  }

  def composeIfDefined[A](functions: Seq[PartialFunction[A, A]]) = new PartialFunction[A, A] {
    override def isDefinedAt(v: A): Boolean = functions.exists(_.isDefinedAt(v))
    override def apply(init: A): A = functions.foldLeft[Option[A]](None)({ (current: Option[A], pf: PartialFunction[A, A]) =>
      val v = current.getOrElse(init)
      if (pf.isDefinedAt(v)) Some(pf(v)) else current
    }).get
  }

  def uplift[A: ClassTag, B, S >: A](f: PartialFunction[A, B]): PartialFunction[S, B] = {
    case v: A if f.isDefinedAt(v) => f(v)
  }

  def fix[A, B](f: PartialFunction[A, PartialFunction[A, B] => B]): PartialFunction[A, B] = new fixedPartialFunction(f)

  class fixedPartialFunction[-A, +B](f: PartialFunction[A, PartialFunction[A, B] => B]) extends PartialFunction[A, B] {
    def isDefinedAt(v: A) = f.isDefinedAt(v)
    def apply(v: A) = f(v)(this)
  }
}
