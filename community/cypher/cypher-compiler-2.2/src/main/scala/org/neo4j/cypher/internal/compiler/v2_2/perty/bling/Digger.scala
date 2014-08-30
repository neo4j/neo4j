/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.perty.bling

import scala.reflect.ClassTag

/**
 * A digger converts a value of type A into value of type O using
 * the given extractor function to convert values at each "layer"
 * of it's drill
 */
trait Digger[-A, M, O] extends (Extractor[M, O] => Extractor[A, O]) {
  def drill: Drill[A, M, O]
  def fixPoint: Extractor[M, O]

  val asExtractor = apply(fixPoint)

  def lift[A0 : ClassTag]: Digger[A0, M, O]

  // mapping functions for all parts
  def mapInput[A0 : ClassTag](f: PartialFunction[A0, A]): Digger[A0, M, O]
  def mapMembers[M1](f: M => M1): Digger[A, M1, O]
  def mapOutput[O1 <: O](f: O => O1): Digger[A, M, O1]

  // generic composition
  def orElse[A1 <: A : ClassTag](other: Digger[A1, M, O]): Digger[A1, M, O]

  // total converter
  val asConverter: (A => O) = (v: A) =>
    asExtractor(v).getOrElse(throw new IllegalArgumentException("Could not convert given value"))
}
