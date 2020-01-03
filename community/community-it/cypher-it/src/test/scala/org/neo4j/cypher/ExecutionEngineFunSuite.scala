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
package org.neo4j.cypher

import org.neo4j.cypher.internal.v4_0.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.{Entity, Node, Relationship, Result}
import org.neo4j.kernel.api.exceptions.Status
import org.scalatest.matchers.{MatchResult, Matcher}

import scala.collection.JavaConverters._

abstract class ExecutionEngineFunSuite
  extends CypherFunSuite with GraphDatabaseTestSupport with ExecutionEngineTestSupport with QueryPlanTestSupport {

  case class haveProperty(propName: String) extends Matcher[Entity] {
    def apply(left: Entity): MatchResult = {
      val result = graph.withTx( tx => {
        val entity = left match {
          case _: Node => tx.getNodeById(left.getId)
          case _: Relationship => tx.getRelationshipById(left.getId)
        }
        entity.hasProperty(propName)
      } )

      MatchResult(
        result,
        s"Didn't have expected property `$propName`",
        s"Has property $propName, expected it not to"
      )
    }

    def withValue(value: Any) = this and new Matcher[Entity] {
      def apply(left: Entity): MatchResult = {
        val propValue = graph.withTx( tx => {
          val entity = left match {
            case _: Node => tx.getNodeById(left.getId)
            case _: Relationship => tx.getRelationshipById(left.getId)
          }
          entity.getProperty(propName)
        } )
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
