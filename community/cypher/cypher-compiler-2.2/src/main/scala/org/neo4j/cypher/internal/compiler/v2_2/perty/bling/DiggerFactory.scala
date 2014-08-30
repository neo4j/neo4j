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

trait DiggerFactory {
  type DiggerImpl[-A, M, O] <: AbstractDigger[A, M, O]

  final def apply[A : ClassTag, M, O](newDrills: Drill[A, M, O]*): DiggerImpl[A, M, O] =
    fromDrillSeq[A, M, O](newDrills)

  def empty[A : ClassTag, M, O]: DiggerImpl[A, M, O] =
    newEmpty[A, M, O]

  def fromSingleDrill[A : ClassTag, M, O](drill: Drill[A, M, O]): DiggerImpl[A, M, O] =
    newFromSingle[A, M, O](drill)

  def fromDrillSeq[A : ClassTag, M, O](drills: Seq[Drill[A, M, O]]): DiggerImpl[A, M, O] =
    if (drills.isEmpty)
      newEmpty[A, M, O]
    else
    if (drills.tails.isEmpty) newFromSingle[A, M, O](drills.head) else newFromMany[A, M, O](drills)

  protected def newEmpty[A : ClassTag, M, O]: DiggerImpl[A, M, O]
  protected def newFromSingle[A : ClassTag, M, O](drill: Drill[A, M, O]): DiggerImpl[A, M, O]
  protected def newFromMany[A : ClassTag, M, O](drills: Seq[Drill[A, M, O]]): DiggerImpl[A, M, O]

  /**
   * A digger converts a value of type A into value of type O using
   * the given extractor function to convert values at each "layer"
   * of it's drill
   */
  abstract class AbstractDigger[-A : ClassTag, M, O] extends Digger[A, M, O] {
    override def drill: Drill[A, M, O]
    override def lift[A0 : ClassTag]: DiggerImpl[A0, M, O]
    override def mapInput[A0 : ClassTag](f: PartialFunction[A0, A]): DiggerImpl[A0, M, O]
    override def mapMembers[M1](f: M => M1): DiggerImpl[A, M1, O]
    override def mapOutput[O1 <: O](f: O => O1): DiggerImpl[A, M, O1]
    override def orElse[A1 <: A : ClassTag](other: Digger[A1, M, O]): DiggerImpl[A1, M, O]

    // implementation specific composition
    def ++[A1 <: A : ClassTag](other: DiggerImpl[A1, M, O]): DiggerImpl[A1, M, O]
  }
}
