/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.helpers

object SeqCombiner {
  /**
    * Combines each element in the inner Seq's with one element of every other inner Seq.
    *
    * scala> combine(List(List("A", "B", "C"), List(1,2,3), List("x", "y", "z")))
      res0: Seq[Seq[Any]] = List(
          List(A, 1, x),
          List(A, 1, y),
          List(A, 1, z),
          List(A, 2, x),
          List(A, 2, y),
          List(A, 2, z),
          List(A, 3, x),
          List(A, 3, y),
          List(A, 3, z),
          List(B, 1, x),
          List(B, 1, y),
          List(B, 1, z),
          List(B, 2, x),
          List(B, 2, y),
          List(B, 2, z),
          List(B, 3, x),
          List(B, 3, y),
          List(B, 3, z),
          List(C, 1, x),
          List(C, 1, y),
          List(C, 1, z),
          List(C, 2, x),
          List(C, 2, y),
          List(C, 2, z),
          List(C, 3, x),
          List(C, 3, y),
          List(C, 3, z))
    */
  def combine[A](xs: Traversable[Traversable[A]]): Seq[Seq[A]] =
    xs.foldLeft(Seq(Seq.empty[A])) {
      (x, y) => for (a <- x; b <- y) yield a :+ b
    }

}
