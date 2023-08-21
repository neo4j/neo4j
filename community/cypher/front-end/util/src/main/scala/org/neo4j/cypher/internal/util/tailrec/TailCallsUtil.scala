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
package org.neo4j.cypher.internal.util.tailrec

import scala.util.control.TailCalls.TailRec
import scala.util.control.TailCalls.done
import scala.util.control.TailCalls.tailcall

object TailCallsUtil {

  def traverse[A, B](as: scala.collection.Seq[A])(f: A => TailRec[B]): TailRec[scala.collection.Seq[B]] =
    if (as.isEmpty) done(scala.collection.Seq.empty)
    else
      for {
        b <- tailcall(f(as.head))
        bs <- tailcall(traverse(as.tail)(f))
      } yield b +: bs

  def map2[A, B, C](recA: TailRec[A], recB: TailRec[B])(f: (A, B) => C): TailRec[C] =
    for {
      a <- recA
      b <- recB
    } yield f(a, b)

}
