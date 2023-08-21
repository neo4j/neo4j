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
package org.neo4j.cypher.internal.frontend.helpers

import scala.reflect.ClassTag

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
  def combine[A](xs: Iterable[Iterable[A]]): Seq[Seq[A]] =
    xs.foldLeft(Seq(Seq.empty[A])) {
      (x, y) => for (a <- x; b <- y) yield a :+ b
    }

  /**
   * As above but for arrays.
   *
   * Works by first making on pass over the outer array to figure out the size and in the next pass
   * if fills in the array,
   */
  def combine[A: ClassTag](xs: Array[Array[A]]): Array[Array[A]] = {
    val ncols = xs.length

    // figure out nrows by multiplying all lengths
    var nrows = 1
    var col = 0
    while (col < ncols) {
      nrows *= xs(col).length
      col += 1
    }

    // array to return
    val array = Array.ofDim[A](nrows, ncols)

    // keep track of index per column, note that indices will be set to zero
    val indices = new Array[Int](ncols)

    // start with the innermost column,
    col = ncols - 1
    var row = 0
    while (row < nrows) {
      var i = 0
      while (i < ncols) {
        val x = xs(i)
        val index = indices(i)
        array(row)(i) = x(index)
        i += 1
      }

      // increase all index from col to last column
      var j = ncols - 1
      while (j >= 0 && j < ncols) {
        val x = xs(j)
        indices(j) += 1
        // check if we exhausted column
        if (indices(j) >= x.length) {
          indices(j) = 0
          if (j == col) {
            col -= 1
          }
          j -= 1
        } else {
          // instead of break
          j = -1
        }
      }
      row += 1
    }
    array
  }
}
