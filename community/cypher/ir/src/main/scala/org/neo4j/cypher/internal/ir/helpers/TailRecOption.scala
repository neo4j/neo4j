/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ir.helpers

import scala.util.control.TailCalls
import scala.util.control.TailCalls.TailRec

/**
 * Wrapper around TailRec[Option[A]] which makes manipulating them easier without having to unwrap everything manually.
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
