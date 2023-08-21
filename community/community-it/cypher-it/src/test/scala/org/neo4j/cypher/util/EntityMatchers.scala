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
package org.neo4j.cypher.util

import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import scala.jdk.CollectionConverters.IterableHasAsScala

trait EntityMatchers {
  self: GraphDatabaseTestSupport =>

  def haveProperty(name: String): HaveProperty = new HaveProperty(name)
  def haveLabels(expectedLabels: String*): Matcher[Node] = new HaveLabels(expectedLabels)

  class HaveProperty(propName: String) extends Matcher[Entity] {

    def apply(left: Entity): MatchResult = {
      val result = graph.withTx(tx => {
        val entity = left match {
          case _: Node         => tx.getNodeById(left.getId)
          case _: Relationship => tx.getRelationshipById(left.getId)
          case e =>
            throw new IllegalStateException(s"This method only supports Node and Relationship, but got ${e.getClass}")
        }
        entity.hasProperty(propName)
      })

      MatchResult(
        result,
        s"Didn't have expected property `$propName`",
        s"Has property $propName, expected it not to"
      )
    }

    def withValue(value: Any) = this and new Matcher[Entity] {

      def apply(left: Entity): MatchResult = {
        val propValue = graph.withTx(tx => {
          val entity = left match {
            case _: Node         => tx.getNodeById(left.getId)
            case _: Relationship => tx.getRelationshipById(left.getId)
            case e =>
              throw new IllegalStateException(s"This method only supports Node and Relationship, but got ${e.getClass}")
          }
          entity.getProperty(propName)
        })
        val result = propValue == value
        MatchResult(
          result,
          s"Property `$propName` didn't have expected value. Expected: $value\nbut was: $propValue",
          s"Expected `$propName` not to have value `$value`, but it does."
        )
      }
    }
  }

  private class HaveLabels(expectedLabels: Seq[String]) extends Matcher[Node] {

    def apply(left: Node): MatchResult = {

      val labels = graph.withTx { tx =>
        tx.getNodeById(left.getId).getLabels.asScala.map(_.name()).toSet
      }

      val result = expectedLabels.forall(labels)

      MatchResult(
        result,
        s"Expected node to have labels $expectedLabels, but it was ${labels.mkString(":")}",
        s"Expected node to not have labels $expectedLabels, but it did."
      )
    }
  }
}
