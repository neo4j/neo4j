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
package org.neo4j.cypher

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Node, PropertyContainer, Result}
import org.neo4j.kernel.api.exceptions.Status
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._

abstract class ExecutionEngineFunSuite
  extends CypherFunSuite with GraphDatabaseTestSupport with ExecutionEngineTestSupport with QueryPlanTestSupport {

  case class haveProperty(propName: String) extends Matcher[PropertyContainer] {
    def apply(left: PropertyContainer): MatchResult = {

      val result = graph.inTx {
        left.hasProperty(propName)
      }

      MatchResult(
        result,
        s"Didn't have expected property `$propName`",
        s"Has property $propName, expected it not to"
      )
    }

    def withValue(value: Any) = this and new Matcher[PropertyContainer] {
      def apply(left: PropertyContainer): MatchResult = {
        val propValue = graph.inTx(left.getProperty(propName))
        val result = propValue == value
        MatchResult(
          result,
          s"Property `$propName` didn't have expected value. Expected: $value\nbut was: $propValue",
          s"Expected `$propName` not to have value `$value`, but it does."
        )
      }
    }
  }

  case class haveLabels(expectedLabels: String*) extends Matcher[Node] {
    def apply(left: Node): MatchResult = {

      val labels = graph.inTx {
        left.getLabels.asScala.map(_.name()).toSet
      }

      val result = expectedLabels.forall(labels)

      MatchResult(
        result,
        s"Expected node to have labels $expectedLabels, but it was ${labels.mkString}",
        s"Expected node to not have labels $expectedLabels, but it did."
      )
    }
  }

  def shouldHaveWarnings(result: Result, statusCodes: List[Status]) {
    val resultCodes = result.getNotifications.asScala.map(_.getCode)
    statusCodes.foreach(statusCode => resultCodes should contain(statusCode.code.serialize()))
  }

  def shouldHaveWarning(result: Result, notification: Status) {
    shouldHaveWarnings(result, List(notification))
  }

  def shouldHaveNoWarnings(result: Result) {
    shouldHaveWarnings(result, List())
  }
}
