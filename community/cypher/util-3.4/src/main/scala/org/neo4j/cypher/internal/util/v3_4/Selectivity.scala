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
package org.neo4j.cypher.internal.util.v3_4

case class Selectivity private(factor: Double) extends Ordered[Selectivity] {
  assert(factor >= 0 && factor <= 1.0)
  def *(other: Selectivity): Selectivity = Selectivity(other.factor * factor)
  def ^(a: Int): Selectivity = Selectivity(Math.pow(factor, a))
  def negate: Selectivity = {
    val f = 1.0 - factor
    if (factor == 0 || f < 1)
      Selectivity(f)
    else
      Selectivity.CLOSEST_TO_ONE
  }

  def compare(that: Selectivity) = factor.compare(that.factor)
}

object Selectivity {

  def of(value: Double): Option[Selectivity] = if (value.isInfinite || value.isNaN || value < 0.0 || value > 1.0) None else Some(Selectivity(value))

  val ZERO = Selectivity(0.0d)
  val ONE = Selectivity(1.0d)
  val CLOSEST_TO_ONE = Selectivity(1 - 5.56e-17)    // we can get closer, but this is close enough


  implicit def turnSeqIntoSingleSelectivity(p: Seq[Selectivity]): Selectivity =
    p.reduceOption(_ * _).getOrElse(Selectivity(1))
}
