/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.util.helpers

object PartialFunctionSupport {

  def reduceAnyDefined[A, B](functions: Seq[PartialFunction[A, B]])(init: B)(combine: (B, B) => B)
    : PartialFunction[A, B] =
    foldAnyDefined[A, B, B](functions)(init)(combine)

  def foldAnyDefined[A, B, C](functions: Seq[PartialFunction[A, B]])(init: C)(combine: (C, B) => C)
    : PartialFunction[A, C] = new PartialFunction[A, C] {
    override def isDefinedAt(v: A): Boolean = functions.exists(_.isDefinedAt(v))

    override def apply(v: A): C =
      functions.foldLeft(init)((acc, pf) => if (pf.isDefinedAt(v)) combine(acc, pf(v)) else acc)
  }

  def composeIfDefined[A](functions: Seq[PartialFunction[A, A]]): PartialFunction[A, A] = new PartialFunction[A, A] {
    override def isDefinedAt(v: A): Boolean = functions.exists(_.isDefinedAt(v))

    override def apply(init: A): A = functions.foldLeft[Option[A]](None)({
      (current: Option[A], pf: PartialFunction[A, A]) =>
        val v = current.getOrElse(init)
        if (pf.isDefinedAt(v)) Some(pf(v)) else current
    }).get
  }
}
