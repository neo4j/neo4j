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
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPCache.SatisfiedExtraRequirements
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPTable.ExtraRequirementGoal
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPTable.asGoal
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPTable.extractExtraRequirements
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPTable.incorporateExtraRequirements

import scala.collection.immutable.BitSet
import scala.collection.mutable

// Table used by IDPSolver to record optimal plans found so far
//
class IDPTable[Result] private (private val map: mutable.Map[ExtraRequirementGoal, Result] =
  mutable.Map.empty[ExtraRequirementGoal, Result]) extends IDPCache[Result] {

  def size: Int = map.size

  def put(goal: Goal, sorted: Boolean, hasExtraProperties: Boolean, result: Result): Unit = {
    map.put(incorporateExtraRequirements(goal, sorted, hasExtraProperties), result)
  }

  def apply(goal: Goal): Results[Result] = {
    lazy val sortedWithExtraProperties =
      map.get(ExtraRequirementGoal(goal.bitSet, isSorted = true, containsExtraProperties = true))
    Results(
      map.get(ExtraRequirementGoal(goal.bitSet, isSorted = false, containsExtraProperties = false)),
      map.get(ExtraRequirementGoal(
        goal.bitSet,
        isSorted = true,
        containsExtraProperties = false
      )).orElse(sortedWithExtraProperties),
      map.get(ExtraRequirementGoal(
        goal.bitSet,
        isSorted = false,
        containsExtraProperties = true
      )).orElse(sortedWithExtraProperties)
    )
  }

  def contains(goal: Goal, sorted: Boolean, extraProperties: Boolean): Boolean =
    map.contains(incorporateExtraRequirements(goal, sorted, extraProperties))

  def unsortedPlansOfSize(k: Int): Iterator[(Goal, Result)] = map.iterator.collect {
    case (goal, result)
      if !goal.isSorted && (goal.bitSet.size == k) =>
      (asGoal(goal), result)
  }

  def plans: Iterator[((Goal, SatisfiedExtraRequirements), Result)] = map.iterator.map {
    case (sortedGoal, result) => (extractExtraRequirements(sortedGoal), result)
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

  private case class ExtraRequirementGoal(bitSet: BitSet, isSorted: Boolean, containsExtraProperties: Boolean) {

    override def equals(obj: Any): Boolean = {
      obj match {
        case that: ExtraRequirementGoal => BitSetEquality.equalBitSets(
            this.bitSet,
            that.bitSet
          ) && this.isSorted == that.isSorted && this.containsExtraProperties == that.containsExtraProperties
        case _ => false
      }
    }

    override def hashCode(): Int = {
      var result = BitSetEquality.hashCode(this.bitSet)
      result = 31 * result + isSorted.##
      31 * result + containsExtraProperties.##
    }
  }

  def apply[Solvable, Result](registry: IdRegistry[Solvable], seed: Seed[Solvable, Result]): IDPTable[Result] = {
    val builder = mutable.Map.newBuilder[ExtraRequirementGoal, Result]
    if (seed.hasDefiniteSize)
      builder.sizeHint(seed.size)
    seed.foreach { case (SolvableItemWithExtraRequirements(goal, sorted, hasExtraProperties), product) =>
      builder += incorporateExtraRequirements(Goal(registry.registerAll(goal)), sorted, hasExtraProperties) -> product
    }
    new IDPTable[Result](builder.result())
  }

  def empty[Result]: IDPTable[Result] = new IDPTable[Result]()

  private def incorporateExtraRequirements(
    goal: Goal,
    sorted: Boolean,
    hasExtraProperties: Boolean
  ): ExtraRequirementGoal = {
    ExtraRequirementGoal(goal.bitSet, isSorted = sorted, hasExtraProperties)
  }

  private def extractExtraRequirements(goal: ExtraRequirementGoal): (Goal, SatisfiedExtraRequirements) = {
    (asGoal(goal), SatisfiedExtraRequirements(goal.isSorted, goal.containsExtraProperties))
  }

  private def asGoal(goal: ExtraRequirementGoal): Goal = {
    Goal(goal.bitSet)
  }
}

case class Goal(bitSet: BitSet) {
  def apply(i: Int): Boolean = bitSet(i)
  def size: Int = bitSet.size
  def subGoals: Iterator[Goal] = bitSet.subsets().map(Goal.apply)
  def subGoals(size: Int): Iterator[Goal] = bitSet.subsets(size).map(Goal.apply)
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
