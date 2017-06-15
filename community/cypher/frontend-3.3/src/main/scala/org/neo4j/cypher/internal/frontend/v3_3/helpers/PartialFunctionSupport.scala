/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_3.helpers

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
