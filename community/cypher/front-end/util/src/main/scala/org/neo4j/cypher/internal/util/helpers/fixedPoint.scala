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

import org.neo4j.cypher.internal.util.CancellationChecker

import scala.annotation.tailrec

object fixedPoint {

  def apply[A](f: A => A): A => A = inner(f, _)

  @tailrec
  private def inner[A](f: A => A, that: A): A = {
    val t = f(that)
    if (t == that)
      t
    else
      inner(f, t)
  }

  def apply[A](cancellation: CancellationChecker)(f: A => A): A => A = innerWithCancel(f, _, cancellation)

  @tailrec
  private def innerWithCancel[A](f: A => A, that: A, cancellation: CancellationChecker): A = {
    cancellation.throwIfCancelled()
    val t = f(that)
    if (t == that)
      t
    else
      innerWithCancel(f, t, cancellation)
  }
}
