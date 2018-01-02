/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.util.v3_4

sealed trait ZeroOneOrMany[+T]

object ZeroOneOrMany {
  def apply[T](elts: Seq[T]): ZeroOneOrMany[T] = elts match {
    case Seq() => Zero
    case Seq(one) => One(one)
    case many => Many(many)
  }
}

case object Zero extends ZeroOneOrMany[Nothing]
final case class One[T](value: T) extends ZeroOneOrMany[T]
final case class Many[T](values: Seq[T]) extends ZeroOneOrMany[T]
