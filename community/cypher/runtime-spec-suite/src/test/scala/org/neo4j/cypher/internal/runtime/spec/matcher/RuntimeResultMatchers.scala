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
package org.neo4j.cypher.internal.runtime.spec.matcher

import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.Prober
import org.neo4j.cypher.internal.notification.InternalNotification
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.matcher.QueryStatisticsMatcher
import org.neo4j.cypher.internal.runtime.matcher.QueryStatisticsMatcher.ExpectedRange
import org.neo4j.cypher.internal.runtime.matcher.QueryStatisticsMatcher.ExpectedRange.Exactly
import org.neo4j.cypher.internal.runtime.matcher.QueryStatisticsMatcher.ExpectedRange.NoExpectation
import org.neo4j.cypher.internal.runtime.spec.AnyRowsMatcher
import org.neo4j.cypher.internal.runtime.spec.Ascending
import org.neo4j.cypher.internal.runtime.spec.CustomRowsMatcher
import org.neo4j.cypher.internal.runtime.spec.Descending
import org.neo4j.cypher.internal.runtime.spec.DisallowValues
import org.neo4j.cypher.internal.runtime.spec.EqualInAnyOrder
import org.neo4j.cypher.internal.runtime.spec.EqualInOrder
import org.neo4j.cypher.internal.runtime.spec.EqualInPartialOrder
import org.neo4j.cypher.internal.runtime.spec.GroupBy
import org.neo4j.cypher.internal.runtime.spec.NoRowsMatcher
import org.neo4j.cypher.internal.runtime.spec.OneToOneSortedPathsMatcher
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RowCount
import org.neo4j.cypher.internal.runtime.spec.RowOrderMatcher
import org.neo4j.cypher.internal.runtime.spec.RowsDontMatch
import org.neo4j.cypher.internal.runtime.spec.RowsMatch
import org.neo4j.cypher.internal.runtime.spec.RowsMatcher
import org.neo4j.cypher.internal.runtime.spec.RuntimeSpecSuiteTestSuite
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSupport
import org.neo4j.cypher.result.QueryProfile
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.lock.LockType
import org.neo4j.lock.ResourceType
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue
import org.scalactic.Equality
import org.scalactic.TolerantNumerics
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import java.util
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import scala.jdk.CollectionConverters.ListHasAsScala

trait RuntimeResultMatchers[CONTEXT <: RuntimeContext] {
  self: RuntimeSpecSuiteTestSuite =>

  protected def runtimeTestSupport: RuntimeTestSupport[CONTEXT]

  private val doubleEquality: Equality[Double] = TolerantNumerics.tolerantDoubleEquality(0.0001)

  def beColumns(columns: String*): RuntimeResultMatcher =
    new RuntimeResultMatcher(columns)

  def inOrder(rows: Iterable[Array[_]], listInAnyOrder: Boolean = false): RowsMatcher = {
    val anyValues = rows.map(row => row.map(ValueUtils.asAnyValue)).toIndexedSeq
    EqualInOrder(anyValues, listInAnyOrder)
  }

  def inAnyOrder(rows: Iterable[Array[_]], listInAnyOrder: Boolean = false): RowsMatcher = {
    val anyValues = rows.map(row => row.map(ValueUtils.asAnyValue)).toIndexedSeq
    EqualInAnyOrder(anyValues, listInAnyOrder)
  }

  def inPartialOrder(rowGroups: Iterable[Iterable[Array[_]]], listInAnyOrder: Boolean = false): RowsMatcher = {
    val anyValues = rowGroups.map(rows => rows.map(row => row.map(ValueUtils.asAnyValue)).toIndexedSeq).toIndexedSeq
    EqualInPartialOrder(anyValues, listInAnyOrder)
  }

  def singleColumn(values: Iterable[Any], listInAnyOrder: Boolean = false): RowsMatcher = {
    val anyValues = values.map(x => Array(ValueUtils.asAnyValue(x))).toIndexedSeq
    EqualInAnyOrder(anyValues, listInAnyOrder)
  }

  def singleColumnInOrder(values: Iterable[Any], listInAnyOrder: Boolean = false): RowsMatcher = {
    val anyValues = values.map(x => Array(ValueUtils.asAnyValue(x))).toIndexedSeq
    EqualInOrder(anyValues, listInAnyOrder)
  }

  def singleRow(values: Any*): RowsMatcher = {
    val anyValues = Array(values.toArray.map(ValueUtils.asAnyValue))
    EqualInAnyOrder(anyValues)
  }

  def rowCount(value: Int): RowsMatcher = {
    RowCount(value)
  }

  def disallowValues(columnValuePredicates: Seq[(String, AnyValue => Boolean)]): RowsMatcher = {
    DisallowValues(columnValuePredicates)
  }

  def matching(func: PartialFunction[Any, _]): RowsMatcher = {
    CustomRowsMatcher(matchPattern(func))
  }

  def groupedBy(columns: String*): RowOrderMatcher = new GroupBy(None, None, columns: _*)

  def groupedBy(nGroups: Int, groupSize: Int, columns: String*): RowOrderMatcher =
    new GroupBy(Some(nGroups), Some(groupSize), columns: _*)

  def sortedAsc(column: String): RowOrderMatcher = new Ascending(column)

  def sortedDesc(column: String): RowOrderMatcher = new Descending(column)

  def tolerantEquals(expected: Double, x: Number): Boolean =
    doubleEquality.areEqual(expected, x.doubleValue())

  def oneToOneSortedPaths(
    pathVar: String,
    rows: Seq[Array[Object]]
  ): OneToOneSortedPathsMatcher = {
    val anyValues = rows.map(row => row.map(ValueUtils.asAnyValue)).toIndexedSeq
    OneToOneSortedPathsMatcher(pathVar, anyValues)
  }

  class RuntimeResultMatcher(expectedColumns: Seq[String]) extends Matcher[RecordingRuntimeResult] {

    private var rowsMatcher: RowsMatcher = AnyRowsMatcher
    private var maybeStatistics: Option[QueryStatisticsMatcher] = None
    private var maybeLockedNodes: Option[LockResourceMatcher] = None
    private var maybeLockedRelationships: Option[LockResourceMatcher] = None

    private var maybeLocks: Option[LockMatcher] = None
    private var maybeNotifications: Option[NotificationsMatcher] = None
    private var maybeIndexProfilesMatcher: Option[IndexProfilesMatcher] = None

    def withNoUpdates(): RuntimeResultMatcher = withStatistics()

    def withStatistics(
      nodesCreated: Int = 0,
      nodesDeleted: Int = 0,
      relationshipsCreated: Int = 0,
      relationshipsDeleted: Int = 0,
      labelsAdded: Int = 0,
      labelsRemoved: Int = 0,
      propertiesSet: Int = 0,
      transactionsStarted: Int = 1,
      transactionsCommitted: Int = 1,
      transactionsRolledBack: Int = 0,
      fileLinesRead: Int = 0
    ): RuntimeResultMatcher = {
      maybeStatistics = Some(new QueryStatisticsMatcher(
        Exactly(nodesCreated),
        Exactly(nodesDeleted),
        Exactly(relationshipsCreated),
        Exactly(relationshipsDeleted),
        Exactly(labelsAdded),
        Exactly(labelsRemoved),
        Exactly(propertiesSet),
        Exactly(transactionsStarted),
        Exactly(transactionsCommitted),
        Exactly(transactionsRolledBack),
        Exactly(fileLinesRead)
      ))
      this
    }

    def withPartialStatistics(
      nodesCreated: Int = -1,
      nodesDeleted: Int = -1,
      relationshipsCreated: Int = -1,
      relationshipsDeleted: Int = -1,
      labelsAdded: Int = -1,
      labelsRemoved: Int = -1,
      propertiesSet: Int = -1,
      transactionsStarted: Int = -1,
      transactionsCommitted: Int = -1,
      transactionsRolledBack: Int = -1,
      fileLinesRead: Int = -1
    ): RuntimeResultMatcher = {
      maybeStatistics = Some(new QueryStatisticsMatcher(
        if (nodesCreated < 0) NoExpectation else Exactly(nodesCreated),
        if (nodesDeleted < 0) NoExpectation else Exactly(nodesDeleted),
        if (relationshipsCreated < 0) NoExpectation else Exactly(relationshipsCreated),
        if (relationshipsDeleted < 0) NoExpectation else Exactly(relationshipsDeleted),
        if (labelsAdded < 0) NoExpectation else Exactly(labelsAdded),
        if (labelsRemoved < 0) NoExpectation else Exactly(labelsRemoved),
        if (propertiesSet < 0) NoExpectation else Exactly(propertiesSet),
        if (transactionsStarted < 0) NoExpectation else Exactly(transactionsStarted),
        if (transactionsCommitted < 0) NoExpectation else Exactly(transactionsCommitted),
        if (transactionsRolledBack < 0) NoExpectation else Exactly(transactionsRolledBack),
        if (fileLinesRead < 0) NoExpectation else Exactly(fileLinesRead)
      ))
      this
    }

    def withStatisticsInRange(
      nodesCreated: ExpectedRange = NoExpectation,
      nodesDeleted: ExpectedRange = NoExpectation,
      relationshipsCreated: ExpectedRange = NoExpectation,
      relationshipsDeleted: ExpectedRange = NoExpectation,
      labelsAdded: ExpectedRange = NoExpectation,
      labelsRemoved: ExpectedRange = NoExpectation,
      propertiesSet: ExpectedRange = NoExpectation,
      transactionsStarted: ExpectedRange = NoExpectation,
      transactionsCommitted: ExpectedRange = NoExpectation,
      transactionsRolledBack: ExpectedRange = NoExpectation,
      fileLinesRead: ExpectedRange = NoExpectation
    ): RuntimeResultMatcher = {
      maybeStatistics = Some(new QueryStatisticsMatcher(
        nodesCreated,
        nodesDeleted,
        relationshipsCreated,
        relationshipsDeleted,
        labelsAdded,
        labelsRemoved,
        propertiesSet,
        transactionsStarted,
        transactionsCommitted,
        transactionsRolledBack,
        fileLinesRead
      ))
      this
    }

    def withLockedNodes(nodeIds: Set[Long], onlyCheckContains: Boolean = false): RuntimeResultMatcher = {
      maybeLockedNodes = Some(new LockResourceMatcher(nodeIds, ResourceType.NODE, onlyCheckContains))
      this
    }

    def withLockedRelationships(relationshipId: Set[Long], onlyCheckContains: Boolean = false): RuntimeResultMatcher = {
      maybeLockedRelationships =
        Some(new LockResourceMatcher(relationshipId, ResourceType.RELATIONSHIP, onlyCheckContains))
      this
    }

    def withLocks(locks: (LockType, ResourceType)*): RuntimeResultMatcher = {
      maybeLocks = Some(new LockMatcher(locks))
      this
    }

    def withNotifications(notifications: InternalNotification*): RuntimeResultMatcher = {
      maybeNotifications = Some(new NotificationsMatcher(notifications))
      this
    }

    def withNoNotifications(): RuntimeResultMatcher = {
      maybeNotifications = Some(new NotificationsMatcher(Seq.empty))
      this
    }

    def withSingleRow(values: Any*): RuntimeResultMatcher = withRows(singleRow(values: _*))

    def withRows(rows: Iterable[Array[_]], listInAnyOrder: Boolean = false): RuntimeResultMatcher =
      withRows(inAnyOrder(rows, listInAnyOrder))

    def withNoRows(): RuntimeResultMatcher = withRows(NoRowsMatcher)

    def withRows(rowsMatcher: RowsMatcher): RuntimeResultMatcher = {
      if (this.rowsMatcher != AnyRowsMatcher)
        throw new IllegalArgumentException("RowsMatcher already set")
      this.rowsMatcher = rowsMatcher
      this
    }

    def usingIndexes(operatorId: Int, indexNames: String*): RuntimeResultMatcher = {
      this.withIndexProfile(operatorId, IndexProfilesMatcher.allOf(indexNames: _*))
    }

    def usingAnyIndexes(operatorId: Int, indexNames: String*): RuntimeResultMatcher = {
      this.withIndexProfile(operatorId, IndexProfilesMatcher.anyOf(indexNames: _*))
    }

    def notUsingIndexes(operatorId: Int, indexNames: String*): RuntimeResultMatcher = {
      this.withIndexProfile(operatorId, IndexProfilesMatcher.not(IndexProfilesMatcher.anyOf(indexNames: _*)))
    }

    def withIndexProfile(operatorId: Int, profileMatcher: IndexProfilesMatcher.MatchOption): RuntimeResultMatcher = {
      if (this.maybeIndexProfilesMatcher.isEmpty) {
        this.maybeIndexProfilesMatcher = Some(new IndexProfilesMatcher())
      }
      this.maybeIndexProfilesMatcher.get.addOne(operatorId, profileMatcher)
      this
    }

    override def apply(left: RecordingRuntimeResult): MatchResult = {
      val columns = left.runtimeResult.fieldNames().toIndexedSeq
      if (columns != expectedColumns) {
        MatchResult(matches = false, s"Expected result columns $expectedColumns, got $columns", "")
      } else {
        val rows = left.awaitAll()
        maybeStatistics
          .map(s => s.apply(left.runtimeResult.queryStatistics()))
          .filter(_.matches == false)
          .orElse {
            maybeLocks
              .map(_.apply(()))
              .filter(_.matches == false)
          }
          .orElse {
            maybeLockedNodes
              .map(_.apply(()))
              .filter(_.matches == false)
          }
          .orElse {
            maybeLockedRelationships
              .map(_.apply(()))
              .filter(_.matches == false)
          }
          .orElse {
            maybeNotifications
              .map(_.apply(left.notifications))
              .filter(_.matches == false)
          }
          .orElse {
            maybeIndexProfilesMatcher
              .map(_.apply(left.runtimeResult.queryProfile()))
              .filter(_.matches == false)
          }
          .getOrElse {
            rowsMatcher.matches(columns, rows) match {
              case RowsMatch          => MatchResult(matches = true, "", "")
              case RowsDontMatch(msg) => MatchResult(matches = false, msg, "")
            }
          }
      }
    }
  }

  /*
   * locks.accept() does not keep the order of when the locks was taken, therefore we don't assert on the order of the locks.
   */
  class LockResourceMatcher(expectedLocked: Set[Long], expectedResourceType: ResourceType, onlyCheckContains: Boolean)
      extends Matcher[Unit] {

    override def apply(left: Unit): MatchResult = {
      val locksList = new util.ArrayList[Long]
      runtimeTestSupport.locks.accept(
        (_: LockType, resourceType: ResourceType, _: Long, resourceId: Long, _: String, _: Long, _: Long) => {
          if (resourceType == expectedResourceType) locksList.add(resourceId)
        }
      )

      val actualLocked = locksList.asScala.toSet
      val setsMatches = if (onlyCheckContains) expectedLocked.subsetOf(actualLocked) else actualLocked == expectedLocked
      MatchResult(
        matches = setsMatches,
        rawFailureMessage = s"expected ${expectedResourceType.name} locked=$expectedLocked but was $actualLocked",
        rawNegatedFailureMessage = ""
      )
    }
  }

  class LockMatcher(expectedLocked: Seq[(LockType, ResourceType)]) extends Matcher[Unit] {

    private val ordering = new Ordering[(LockType, ResourceType)] {

      override def compare(x: (LockType, ResourceType), y: (LockType, ResourceType)): Int = {
        val (xLock, xType) = x
        val (yLock, yType) = y
        val comparison = xLock.compareTo(yLock)
        if (comparison == 0) {
          Integer.compare(xType.typeId(), yType.typeId())
        } else {
          comparison
        }
      }
    }

    override def apply(left: Unit): MatchResult = {
      val actualLockList = ArrayBuffer.empty[(LockType, ResourceType)]
      runtimeTestSupport.locks.accept(
        (lockType: LockType, resourceType: ResourceType, _: Long, _: Long, _: String, _: Long, _: Long) =>
          actualLockList.append((lockType, resourceType))
      )

      MatchResult(
        matches = expectedLocked.sorted(ordering) == actualLockList.sorted(ordering),
        rawFailureMessage = s"expected locks ${expectedLocked.mkString(", ")} but got ${actualLockList.mkString(", ")}",
        rawNegatedFailureMessage = ""
      )
    }
  }

  class NotificationsMatcher(expectedNotifications: Seq[InternalNotification])
      extends Matcher[Seq[InternalNotification]] {

    override def apply(left: Seq[InternalNotification]): MatchResult = {
      val expectedNotificationsString = if (expectedNotifications.isEmpty) "no notifications"
      else "notifications " + expectedNotifications.mkString(", ")
      val actualNotificationsString = if (left.isEmpty) "no notifications" else left.mkString(", ")
      MatchResult(
        matches = left.sortBy(_.notificationName) == expectedNotifications.sortBy(_.notificationName),
        rawFailureMessage = s"expected $expectedNotificationsString but got $actualNotificationsString.",
        rawNegatedFailureMessage = ""
      )

    }
  }

  def failProbe(failAfterRowCount: Int, fail: String => Throwable = msg => new RuntimeException(msg)): Prober.Probe =
    new Prober.Probe {
      val c = new AtomicInteger(0)

      override def onRow(row: AnyRef, state: AnyRef): Unit = {
        if (c.incrementAndGet() == failAfterRowCount) {
          throw fail(s"Probe failed as expected (row count=$c)")
        }
      }
    }

  def failOnVariableProbe(
    variable: String,
    predicate: AnyValue => Boolean,
    fail: String => Throwable = msg => new RuntimeException(msg)
  ): Prober.Probe =
    new Prober.Probe {

      override def onRow(row: AnyRef, state: AnyRef): Unit = {
        val cypherRow = row.asInstanceOf[CypherRow]
        val variableValue = cypherRow.getByName(variable)
        if (predicate(variableValue)) {
          throw fail(s"Probe failed as expected")
        }
      }
    }
}

// The declarations below are not nested in the trait above because the Scala 2.13 TASTy reader
// cannot resolve objects (including synthetic companions) nested in a Scala 3 trait.
case class DiffItem(missingRow: ListValue, fromA: Boolean)

class IndexProfilesMatcher extends Matcher[QueryProfile] {
  private lazy val matchers: mutable.Map[Int, IndexProfilesMatcher.MatchOption] = mutable.Map()

  def addOne(operatorId: Int, profileMatcher: IndexProfilesMatcher.MatchOption): Unit = {
    matchers.updateWith(operatorId) {
      case Some(value) =>
        Some(IndexProfilesMatcher.All(Seq(value, profileMatcher)))
      case None =>
        Some(profileMatcher)
    }
  }

  def indexProfilesMatch(
    matchers: Map[Int, IndexProfilesMatcher.MatchOption],
    actual: Map[Int, Seq[String]]
  ): Boolean = {
    matchers.keySet == actual.keySet &&
    matchers.forall { case (id, matcher) =>
      matcher.doesMatch(actual(id))
    }
  }

  override def apply(left: QueryProfile): MatchResult = {
    val actualIndexesUsed = matchers.map {
      case (id: Int, _) =>
        id -> left.operatorProfile(id).indexesUsed().map(_.getName).toSeq
    }
    MatchResult(
      matches = indexProfilesMatch(matchers, actualIndexesUsed),
      rawFailureMessage = s"expected to use indexes $matchers but found $actualIndexesUsed",
      rawNegatedFailureMessage = ""
    )
  }
}

object IndexProfilesMatcher {
  def anyOf(indexNames: String*): IndexProfilesMatcher.MatchOption = Any(indexNames.map(Unary(_)))
  def allOf(indexNames: String*): IndexProfilesMatcher.MatchOption = All(indexNames.map(Unary(_)))
  def not(matcher: IndexProfilesMatcher.MatchOption): IndexProfilesMatcher.MatchOption = Not(matcher)

  sealed trait MatchOption {
    def doesMatch(targets: Seq[String]): Boolean
  }

  case class Unary(name: String) extends MatchOption {
    override def doesMatch(targets: Seq[String]): Boolean = targets.contains(name)
  }

  case class Any(anyMatches: Seq[MatchOption]) extends MatchOption {
    override def doesMatch(targets: Seq[String]): Boolean = anyMatches.exists(_.doesMatch(targets))
  }

  case class All(allMatches: Seq[MatchOption]) extends MatchOption {
    override def doesMatch(targets: Seq[String]): Boolean = allMatches.forall(_.doesMatch(targets))
  }

  case class Not(negativeMatch: MatchOption) extends MatchOption {
    override def doesMatch(targets: Seq[String]): Boolean = !negativeMatch.doesMatch(targets)
  }
}
