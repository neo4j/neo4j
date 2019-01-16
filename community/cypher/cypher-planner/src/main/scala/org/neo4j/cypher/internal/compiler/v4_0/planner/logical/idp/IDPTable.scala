/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.v4_0.planner.logical.idp

import scala.collection.mutable

// Table used by IDPSolver to record optimal plans found so far
//
class IDPTable[P, O](private val map: mutable.Map[(Goal, O), P] = mutable.Map.empty[(Goal, O), P]) extends IDPCache[P, O] {

  def size: Int = map.size

  def put(goal: Goal, o: O, product: P): Unit = {
    map.put((goal, o), product)
  }

  def apply(goal: Goal, o: O): Option[P] = map.get((goal, o))

  def apply(goal: Goal): Seq[(O, P)] = map.collect {
    case ((key, o), p) if key == goal => (o, p)
  }.toSeq

  def contains(goal: Goal, o: O): Boolean = map.contains((goal, o))

  def plansOfSize(k: Int): Iterator[((Goal, O), P)] = map.iterator.filter(_._1._1.size == k)

  def plans: Iterator[((Goal, O), P)] = map.iterator

  def removeAllTracesOf(goal: Goal): Unit = {
    val toDrop = map.keysIterator.filter { case (entry, _) => (entry & goal).nonEmpty }
    toDrop.foreach(map.remove)
  }

  override def toString(): String = s"IDPPlanTable(numberOfPlans=$size, largestSolved=${map.keySet.map(_._1.size).max})"
}

object IDPTable {
  def apply[X, O, P](registry: IdRegistry[X], seed: Seed[X, O, P]): IDPTable[P, O] = {
    val builder = mutable.Map.newBuilder[(Goal, O), P]
    if (seed.hasDefiniteSize)
      builder.sizeHint(seed.size)
    seed.foreach { case ((goal, o), product) => builder += (registry.registerAll(goal), o) -> product }
    new IDPTable[P, O](builder.result())
  }
}
