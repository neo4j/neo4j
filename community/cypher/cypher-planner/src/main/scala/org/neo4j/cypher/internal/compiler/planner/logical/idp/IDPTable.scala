/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.planner.logical.idp

import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPCache.Results
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPTable.SORTED_BIT
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPTable.SortedGoal
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPTable.asGoal
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPTable.extractSort
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPTable.incorporateSort

import scala.collection.immutable.BitSet
import scala.collection.mutable

// Table used by IDPSolver to record optimal plans found so far
//
class IDPTable[Result] private (private val map: mutable.Map[SortedGoal, Result] =
  mutable.Map.empty[SortedGoal, Result]) extends IDPCache[Result] {

  def size: Int = map.size

  def put(goal: Goal, sorted: Boolean, result: Result): Unit = {
    map.put(incorporateSort(goal, sorted), result)
  }

  def apply(goal: Goal): Results[Result] = {
    Results(map.get(SortedGoal(goal.bitSet)), map.get(SortedGoal(goal.bitSet + SORTED_BIT)))
  }

  def contains(goal: Goal, sorted: Boolean): Boolean = map.contains(incorporateSort(goal, sorted))

  def unsortedPlansOfSize(k: Int): Iterator[(Goal, Result)] = map.iterator.collect {
    case (sortedGoal, result) if !sortedGoal.isSorted && sortedGoal.bitSet.size == k => (asGoal(sortedGoal), result)
  }

  def plans: Iterator[((Goal, Boolean), Result)] = map.iterator.map {
    case (sortedGoal, result) => (extractSort(sortedGoal), result)
  }

  def removeAllTracesOf(goal: Goal): Unit = {
    // It is OK and required not to convert the goal to a sorted goal here.
    // We want to drop the entries which solve a subset of what goal solves,
    // regardless ordering.
    val toDrop = map.keysIterator.filter { entry => (entry.bitSet & goal.bitSet).nonEmpty }
    toDrop.foreach(map.remove)
  }

  override def toString: String =
    s"IDPPlanTable(numberOfPlans=$size, largestSolved=${map.keySet.map(sortedGoal => asGoal(sortedGoal).size).max})"
}

object IDPTable {
  val SORTED_BIT = 0

  private case class SortedGoal(bitSet: BitSet) {
    def isSorted: Boolean = bitSet(SORTED_BIT)

    override def equals(obj: Any): Boolean = {
      obj match {
        case that: SortedGoal => BitSetEquality.equalBitSets(this.bitSet, that.bitSet)
        case _                => false
      }
    }
    override def hashCode(): Int = BitSetEquality.hashCode(this.bitSet)
  }

  def apply[Solvable, Result](registry: IdRegistry[Solvable], seed: Seed[Solvable, Result]): IDPTable[Result] = {
    val builder = mutable.Map.newBuilder[SortedGoal, Result]
    if (seed.hasDefiniteSize)
      builder.sizeHint(seed.size)
    seed.foreach { case ((goal, sorted), product) =>
      builder += incorporateSort(Goal(registry.registerAll(goal)), sorted) -> product
    }
    new IDPTable[Result](builder.result())
  }

  def empty[Result]: IDPTable[Result] = new IDPTable[Result]()

  private def incorporateSort(goal: Goal, sorted: Boolean): SortedGoal = {
    if (sorted) SortedGoal(goal.bitSet + SORTED_BIT) else SortedGoal(goal.bitSet)
  }

  private def extractSort(goal: SortedGoal): (Goal, Boolean) = {
    (asGoal(goal), goal.isSorted)
  }

  private def asGoal(goal: SortedGoal): Goal = {
    Goal(goal.bitSet - SORTED_BIT)
  }
}

case class Goal(bitSet: BitSet) {
  def apply(i: Int): Boolean = bitSet(i)
  def size: Int = bitSet.size
  def subGoals: Iterator[Goal] = bitSet.subsets().map(Goal)
  def subGoals(size: Int): Iterator[Goal] = bitSet.subsets(size).map(Goal)
  def exists(p: Int => Boolean): Boolean = bitSet.exists(p)
  def diff(that: Goal): Goal = Goal(bitSet &~ that.bitSet)

  /**
   * @return all pairs of non-empty, non-overlapping goals that together cover this goal.
   */
  def coveringSplits: Iterator[(Goal, Goal)] = for {
    leftSize <- (1 until size).iterator // leave out leftSize == 0 and leftSize == size
    leftGoal <- subGoals(leftSize)
    rightGoal = diff(leftGoal)
  } yield (leftGoal, rightGoal)

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: Goal => BitSetEquality.equalBitSets(this.bitSet, that.bitSet)
      case _          => false
    }
  }
  override def hashCode(): Int = BitSetEquality.hashCode(this.bitSet)
}
