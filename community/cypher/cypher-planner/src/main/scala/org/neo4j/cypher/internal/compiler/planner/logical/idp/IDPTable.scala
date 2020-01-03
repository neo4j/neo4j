/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// Table used by IDPSolver to record optimal plans found so far
//
class IDPTable[Result, Attribute](private val map: mutable.Map[(Goal, Attribute), Result] = mutable.Map.empty[(Goal, Attribute), Result]) extends IDPCache[Result, Attribute] {

  def size: Int = map.size

  def put(goal: Goal, attribute: Attribute, result: Result): Unit = {
    map.put((goal, attribute), result)
  }

  def apply(goal: Goal): Seq[(Attribute, Result)] = {
    val buffer = new ArrayBuffer[(Attribute, Result)]()
    map.foreach {
      goal_attr_result =>
        if (sameGoal(goal, goal_attr_result._1._1)) {
          buffer += ((goal_attr_result._1._2, goal_attr_result._2))
        }
    }
    buffer
  }

  def contains(goal: Goal, attribute: Attribute): Boolean = map.contains((goal, attribute))

  def plansOfSize(k: Int): Iterator[((Goal, Attribute), Result)] = map.iterator.filter(_._1._1.size == k)

  def plans: Iterator[((Goal, Attribute), Result)] = map.iterator

  def removeAllTracesOf(goal: Goal): Unit = {
    val toDrop = map.keysIterator.filter { case (entry, _) => (entry & goal).nonEmpty }
    toDrop.foreach(map.remove)
  }

  private def sameGoal(goalA: Goal, goalB: Goal) = {
    (goalA eq goalB) ||
      (goalA.size == goalB.size && goalA.subsetOf(goalB))
  }

  override def toString: String = s"IDPPlanTable(numberOfPlans=$size, largestSolved=${map.keySet.map(_._1.size).max})"
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
