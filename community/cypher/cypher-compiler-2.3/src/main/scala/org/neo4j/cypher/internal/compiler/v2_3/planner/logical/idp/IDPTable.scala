/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
/**
* Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp

import collection.mutable

// Table used by IDPSolver to record optimal plans found so far
//
class IDPTable[P](private val map: mutable.Map[Goal, P] = mutable.Map.empty[Goal, P]) extends IDPCache[P] {

  def size = map.size

  def put(goal: Goal, product: P): Unit = {
    map.put(goal, product)
  }

  def apply(goal: Goal): Option[P] = map.get(goal)

  def contains(goal: Goal): Boolean = map.contains(goal)

  def plansOfSize(k: Int) = map.iterator.filter(_._1.size == k)

  def plans = map.iterator

  def removeAllTracesOf(goal: Goal) = {
    val toDrop = map.keysIterator.filter(entry => (entry & goal).nonEmpty)
    toDrop.foreach(map.remove)
  }

  override def toString(): String = s"IDPPlanTable(numberOfPlans=$size, largestSolved=${map.keySet.map(_.size).max})"
}

object IDPTable {
  def apply[X, P](registry: IdRegistry[X], seed: Seed[X, P]) = {
    val builder = mutable.Map.newBuilder[Goal, P]
    if (seed.hasDefiniteSize)
      builder.sizeHint(seed.size)
    seed.foreach { case (goal, product) => builder += registry.registerAll(goal) -> product }
    new IDPTable[P](builder.result())
  }
}
