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
import org.neo4j.cypher.internal.runtime.CypherRow
import org.neo4j.cypher.internal.runtime.spec._
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.lock.LockType
import org.neo4j.lock.ResourceType
import org.neo4j.values.AnyValue
import org.neo4j.values.virtual.ListValue
import org.scalactic.Equality
import org.scalactic.TolerantNumerics
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.should.Matchers

import java.util
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters.ListHasAsScala

trait RuntimeResultMatchers[CONTEXT <: RuntimeContext] {
  self: Matchers =>

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
      transactionsRolledBack: Int = 0
    ): RuntimeResultMatcher = {
      maybeStatistics = Some(new QueryStatisticsMatcher(
        Some(nodesCreated),
        Some(nodesDeleted),
        Some(relationshipsCreated),
        Some(relationshipsDeleted),
        Some(labelsAdded),
        Some(labelsRemoved),
        Some(propertiesSet),
        Some(transactionsStarted),
        Some(transactionsCommitted),
        Some(transactionsRolledBack)
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
      transactionsRolledBack: Int = -1
    ): RuntimeResultMatcher = {
      maybeStatistics = Some(new QueryStatisticsMatcher(
        if (nodesCreated < 0) None else Some(nodesCreated),
        if (nodesDeleted < 0) None else Some(nodesDeleted),
        if (relationshipsCreated < 0) None else Some(relationshipsCreated),
        if (relationshipsDeleted < 0) None else Some(relationshipsDeleted),
        if (labelsAdded < 0) None else Some(labelsAdded),
        if (labelsRemoved < 0) None else Some(labelsRemoved),
        if (propertiesSet < 0) None else Some(propertiesSet),
        if (transactionsStarted < 0) None else Some(transactionsStarted),
        if (transactionsCommitted < 0) None else Some(transactionsCommitted),
        if (transactionsRolledBack < 0) None else Some(transactionsRolledBack)
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

  class QueryStatisticsMatcher(
    nodesCreated: Option[Int],
    nodesDeleted: Option[Int],
    relationshipsCreated: Option[Int],
    relationshipsDeleted: Option[Int],
    labelsAdded: Option[Int],
    labelsRemoved: Option[Int],
    propertiesSet: Option[Int],
    transactionsStarted: Option[Int],
    transactionsCommitted: Option[Int],
    transactionsRolledBack: Option[Int]
  ) extends Matcher[QueryStatistics] {

    override def apply(left: QueryStatistics): MatchResult = {
      def matchFailed(ourValue: Option[Int], leftValue: Int): Boolean = {
        ourValue.isDefined && ourValue.get != leftValue
      }

      def transactionsStatsDoNotMatch: Option[MatchResult] = {
        left match {
          case qs: org.neo4j.cypher.internal.runtime.ExtendedQueryStatistics =>
            // FIXME: we currently do not account for the outermost transaction because that is out of cypher's control
            if (matchFailed(transactionsCommitted, qs.getTransactionsCommitted + 1)) {
              Some(MatchResult(
                matches = false,
                s"expected transactionsCommitted=${transactionsCommitted.get} but was ${qs.getTransactionsCommitted + 1}",
                ""
              ))
            } else if (matchFailed(transactionsStarted, qs.getTransactionsStarted + 1)) {
              Some(MatchResult(
                matches = false,
                s"expected transactionsStarted=${transactionsStarted.get} but was ${qs.getTransactionsStarted + 1}",
                ""
              ))
            } else if (matchFailed(transactionsRolledBack, qs.getTransactionsRolledBack)) {
              Some(MatchResult(
                matches = false,
                s"expected transactionsRolledBack=${transactionsRolledBack.get} but was ${qs.getTransactionsRolledBack}",
                ""
              ))
            } else {
              None
            }
          case _ =>
            if (matchFailed(transactionsCommitted, 1)) {
              Some(MatchResult(
                matches = false,
                s"expected transactionsCommitted=${transactionsCommitted.get} but can only match on org.neo4j.cypher.internal.runtime.QueryStatistics and was $left",
                ""
              ))
            } else {
              None
            }
        }
      }

      if (matchFailed(nodesCreated, left.getNodesCreated)) {
        MatchResult(matches = false, s"expected nodesCreated=${nodesCreated.get} but was ${left.getNodesCreated}", "")
      } else if (matchFailed(nodesDeleted, left.getNodesDeleted)) {
        MatchResult(matches = false, s"expected nodesDeleted=${nodesDeleted.get} but was ${left.getNodesDeleted}", "")
      } else if (matchFailed(relationshipsCreated, left.getRelationshipsCreated)) {
        MatchResult(
          matches = false,
          s"expected relationshipCreated=${relationshipsCreated.get} but was ${left.getRelationshipsCreated}",
          ""
        )
      } else if (matchFailed(relationshipsDeleted, left.getRelationshipsDeleted)) {
        MatchResult(
          matches = false,
          s"expected relationshipsDeleted=${relationshipsDeleted.get} but was ${left.getRelationshipsDeleted}",
          ""
        )
      } else if (matchFailed(labelsAdded, left.getLabelsAdded)) {
        MatchResult(matches = false, s"expected labelsAdded=${labelsAdded.get} but was ${left.getLabelsAdded}", "")
      } else if (matchFailed(labelsRemoved, left.getLabelsRemoved)) {
        MatchResult(
          matches = false,
          s"expected labelsRemoved=${labelsRemoved.get} but was ${left.getLabelsRemoved}",
          ""
        )
      } else if (matchFailed(propertiesSet, left.getPropertiesSet)) {
        MatchResult(
          matches = false,
          s"expected propertiesSet=${propertiesSet.get} but was ${left.getPropertiesSet}",
          ""
        )
      } else if (transactionsStatsDoNotMatch.nonEmpty) {
        transactionsStatsDoNotMatch.get
      } else {
        MatchResult(matches = true, "", "")
      }
    }
  }

  case class DiffItem(missingRow: ListValue, fromA: Boolean)

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
