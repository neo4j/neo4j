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
package org.neo4j.cypher.internal.runtime.matcher

import org.neo4j.cypher.internal.runtime.matcher.QueryStatisticsMatcher.ExpectedRange
import org.neo4j.cypher.internal.runtime.matcher.QueryStatisticsMatcher.ExpectedRange.NoExpectation
import org.neo4j.graphdb.QueryStatistics
import org.neo4j.kernel.api.query.ExtendedQueryStatistics
import org.scalatest.Assertions.withClue
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher
import org.scalatest.matchers.must.Matchers.be
import org.scalatest.matchers.should.Matchers.should
import org.scalatest.matchers.should.Matchers.shouldBe

object QueryStatisticsMatcher {

  def apply(
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
  ): QueryStatisticsMatcher = new QueryStatisticsMatcher(
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
  )

  sealed trait ExpectedRange {
    def assert(value: Long, clue: String = ""): Unit
    def matches(value: Long): Boolean
  }

  object ExpectedRange {

    case object NoExpectation extends ExpectedRange {
      def assert(value: Long, clue: String = ""): Unit = {}
      def matches(value: Long): Boolean = true
    }

    case class Exactly(n: Long) extends ExpectedRange {

      def assert(value: Long, clue: String = ""): Unit = {
        withClue(s"Expected exactly $n, but was $value $clue: ") {
          value shouldBe n
        }
      }
      def matches(value: Long): Boolean = value == n
    }

    case class AtLeast(n: Long) extends ExpectedRange {

      def assert(value: Long, clue: String = ""): Unit = {
        withClue(s"Expected at least $n, but was $value $clue: ") {
          value should be >= n
        }
      }
      def matches(value: Long): Boolean = value >= n
    }

    case class AtMost(n: Long) extends ExpectedRange {

      def assert(value: Long, clue: String = ""): Unit = {
        withClue(s"Expected at most $n, but was $value $clue: ") {
          value should be <= n
        }
      }
      def matches(value: Long): Boolean = value <= n
    }

    case class Between(min: Long, max: Long) extends ExpectedRange {

      def assert(value: Long, clue: String = ""): Unit = {
        withClue(s"Expected between $min and $max, but was $value $clue: ") {
          value should (be <= max and be >= min)
        }
      }
      def matches(value: Long): Boolean = value >= min && value <= max
    }
  }
}

class QueryStatisticsMatcher(
  nodesCreated: ExpectedRange,
  nodesDeleted: ExpectedRange,
  relationshipsCreated: ExpectedRange,
  relationshipsDeleted: ExpectedRange,
  labelsAdded: ExpectedRange,
  labelsRemoved: ExpectedRange,
  propertiesSet: ExpectedRange,
  transactionsStarted: ExpectedRange,
  transactionsCommitted: ExpectedRange,
  transactionsRolledBack: ExpectedRange,
  fileLinesRead: ExpectedRange
) extends Matcher[QueryStatistics] {

  override def apply(left: QueryStatistics): MatchResult = {
    def matchFailed(ourValue: ExpectedRange, leftValue: Long): Boolean = {
      !ourValue.matches(leftValue)
    }

    def transactionsStatsDoNotMatch: Option[MatchResult] = {
      left match {
        case qs: ExtendedQueryStatistics =>
          // NOTE: We currently do not account for the outermost transaction because that is out of Cypher's control
          if (matchFailed(transactionsCommitted, qs.getTransactionsCommitted + 1)) {
            Some(MatchResult(
              matches = false,
              s"expected transactionsCommitted ${transactionsCommitted} but was ${qs.getTransactionsCommitted + 1}",
              ""
            ))
          } else if (matchFailed(transactionsStarted, qs.getTransactionsStarted + 1)) {
            Some(MatchResult(
              matches = false,
              s"expected transactionsStarted ${transactionsStarted} but was ${qs.getTransactionsStarted + 1}",
              ""
            ))
          } else if (matchFailed(transactionsRolledBack, qs.getTransactionsRolledBack)) {
            Some(MatchResult(
              matches = false,
              s"expected transactionsRolledBack ${transactionsRolledBack} but was ${qs.getTransactionsRolledBack}",
              ""
            ))
          } else {
            None
          }
        case _ =>
          if (matchFailed(transactionsCommitted, 1)) {
            Some(MatchResult(
              matches = false,
              s"expected transactionsCommitted ${transactionsCommitted} but can only match on org.neo4j.cypher.internal.runtime.QueryStatistics and was $left",
              ""
            ))
          } else {
            None
          }
      }
    }

    if (matchFailed(nodesCreated, left.getNodesCreated)) {
      MatchResult(matches = false, s"expected nodesCreated ${nodesCreated} but was ${left.getNodesCreated}", "")
    } else if (matchFailed(nodesDeleted, left.getNodesDeleted)) {
      MatchResult(matches = false, s"expected nodesDeleted ${nodesDeleted} but was ${left.getNodesDeleted}", "")
    } else if (matchFailed(relationshipsCreated, left.getRelationshipsCreated)) {
      MatchResult(
        matches = false,
        s"expected relationshipCreated ${relationshipsCreated} but was ${left.getRelationshipsCreated}",
        ""
      )
    } else if (matchFailed(relationshipsDeleted, left.getRelationshipsDeleted)) {
      MatchResult(
        matches = false,
        s"expected relationshipsDeleted ${relationshipsDeleted} but was ${left.getRelationshipsDeleted}",
        ""
      )
    } else if (matchFailed(labelsAdded, left.getLabelsAdded)) {
      MatchResult(matches = false, s"expected labelsAdded ${labelsAdded} but was ${left.getLabelsAdded}", "")
    } else if (matchFailed(labelsRemoved, left.getLabelsRemoved)) {
      MatchResult(
        matches = false,
        s"expected labelsRemoved ${labelsRemoved} but was ${left.getLabelsRemoved}",
        ""
      )
    } else if (matchFailed(propertiesSet, left.getPropertiesSet)) {
      MatchResult(
        matches = false,
        s"expected propertiesSet ${propertiesSet} but was ${left.getPropertiesSet}",
        ""
      )
    } else if (transactionsStatsDoNotMatch.nonEmpty) {
      transactionsStatsDoNotMatch.get
    } else {
      MatchResult(matches = true, "", "")
    }
  }
}
