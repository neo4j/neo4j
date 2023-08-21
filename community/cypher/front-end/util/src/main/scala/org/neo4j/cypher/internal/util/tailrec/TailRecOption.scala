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

import scala.util.control.TailCalls
import scala.util.control.TailCalls.TailRec

/**
 * Wrapper around `TailRec[Option[A]]` which makes manipulating them easier without having to unwrap everything manually.
 *
 * @param run unwrapped value
 */
case class TailRecOption[A](run: TailRec[Option[A]]) {

  def flatMap[B](f: A => TailRecOption[B]): TailRecOption[B] =
    TailRecOption(run.flatMap {
      case Some(a) => TailCalls.tailcall(f(a).run)
      case None    => TailCalls.done(None)
    })

  def map[B](f: A => B): TailRecOption[B] =
    flatMap(a => TailRecOption.some(f(a)))

  def result: Option[A] =
    run.result

  def getOrElse(a: => A): A =
    result.getOrElse(a)
}

object TailRecOption {

  def some[A](a: A): TailRecOption[A] =
    TailRecOption(TailCalls.done(Some(a)))

  def none[A]: TailRecOption[A] =
    TailRecOption(TailCalls.done(None))

  def tailcall[A](a: => TailRecOption[A]): TailRecOption[A] =
    TailRecOption(TailCalls.tailcall(a.run))

  def traverse[A, B](as: scala.collection.Seq[A])(f: A => TailRecOption[B]): TailRecOption[scala.collection.Seq[B]] =
    if (as.isEmpty) some(scala.collection.Seq.empty)
    else
      for {
        b <- tailcall(f(as.head))
        bs <- tailcall(traverse(as.tail)(f))
      } yield b +: bs

  def map2[A, B, C](recA: TailRecOption[A], recB: TailRecOption[B])(f: (A, B) => C): TailRecOption[C] =
    for {
      a <- recA
      b <- recB
    } yield f(a, b)
}
