/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.feature.parser

import cucumber.api.DataTable
import org.neo4j.graphdb.{Entity, GraphDatabaseService}
import org.opencypher.tools.tck.InvalidFeatureFormatException
import org.opencypher.tools.tck.constants.TCKSideEffects._

import scala.collection.JavaConverters._

object SideEffects {

  def expect(expectations: DataTable): Values = {
    val keys = expectations.transpose().topCells().asScala
    val values = expectations.transpose().cells(1).asScala.head

    val expected = keys.zipWithIndex.foldLeft(Values()) {
      case (v, (ADDED_NODES, index)) =>
        v.copy(nodesCreated = values.get(index).toInt)
      case (v, (DELETED_NODES, index)) =>
        v.copy(nodesDeleted = values.get(index).toInt)
      case (v, (ADDED_RELATIONSHIPS, index)) =>
        v.copy(relsCreated = values.get(index).toInt)
      case (v, (DELETED_RELATIONSHIPS, index)) =>
        v.copy(relsDeleted = values.get(index).toInt)
      case (v, (ADDED_LABELS, index)) =>
        v.copy(labelsCreated = values.get(index).toInt)
      case (v, (DELETED_LABELS, index)) =>
        v.copy(labelsDeleted = values.get(index).toInt)
      case (v, (ADDED_PROPERTIES, index)) =>
        v.copy(propsCreated = values.get(index).toInt)
      case (v, (DELETED_PROPERTIES, index)) =>
        v.copy(propsDeleted = values.get(index).toInt)
      case (_, (sideEffect, _)) => throw InvalidFeatureFormatException(
        s"Invalid side effect: $sideEffect. Valid ones are: ${ALL.mkString(",")}")
    }

    expected
  }

  case class Values(nodesCreated: Int = 0,
                    nodesDeleted: Int = 0,
                    relsCreated: Int = 0,
                    relsDeleted: Int = 0,
                    labelsCreated: Int = 0,
                    labelsDeleted: Int = 0,
                    propsCreated: Int = 0,
                    propsDeleted: Int = 0) {
    override def toString = {
      s"""Values(
          |  +nodes:  $nodesCreated
          |  -nodes:  $nodesDeleted
          |  +rels:   $relsCreated
          |  -rels:   $relsDeleted
          |  +labels: $labelsCreated
          |  -labels: $labelsDeleted
          |  +props:  $propsCreated
          |  -props:  $propsDeleted)""".stripMargin
    }
  }

  case class State(nodes: Set[Long] = Set.empty,
                   rels: Set[Long] = Set.empty,
                   labels: Set[String] = Set.empty,
                   props: Set[(Entity, String, AnyRef)] = Set.empty) {

    /**
      * Computes the difference in between this state and a later state (the argument).
      * The difference is a set of side effects in the form of a Values instance.
      *
      * @param later the later state to compare against.
      * @return the side effect difference, as a Values instance.
      */
    def diff(later: State): Values = {
      val nodesCreated = (later.nodes diff nodes).size
      val nodesDeleted = (nodes diff later.nodes).size
      val relsCreated = (later.rels diff rels).size
      val relsDeleted = (rels diff later.rels).size
      val labelsCreated = (later.labels diff labels).size
      val labelsDeleted = (labels diff later.labels).size
      val propsCreated = (later.props diff props).size
      val propsDeleted = (props diff later.props).size

      Values(
        nodesCreated, nodesDeleted,
        relsCreated, relsDeleted,
        labelsCreated, labelsDeleted,
        propsCreated, propsDeleted
      )
    }
  }

  private val prefix = "CYPHER 3.3 runtime=interpreted"

  private val nodesQuery =
    s"""$prefix MATCH (n) RETURN id(n) AS node"""

  private val relsQuery =
    s"""$prefix MATCH ()-[r]->() RETURN id(r) AS rel"""

  private val labelsQuery =
    s"""$prefix MATCH (n)
       |UNWIND labels(n) AS label
       |RETURN DISTINCT label""".stripMargin

  private val propsQuery =
    s"""$prefix MATCH (n)
       |UNWIND keys(n) AS key
       |WITH properties(n) AS properties, key, n
       |RETURN n AS entity, key, properties[key] AS value
       |UNION ALL
       |MATCH ()-[r]->()
       |UNWIND keys(r) AS key
       |WITH properties(r) AS properties, key, r
       |RETURN r AS entity, key, properties[key] AS value""".stripMargin

  def measureState(db: GraphDatabaseService): State = {
    val nodes = db.execute(nodesQuery).columnAs[Long]("node").asScala.toSet
    val rels = db.execute(relsQuery).columnAs[Long]("rel").asScala.toSet
    val labels = db.execute(labelsQuery).columnAs[String]("label").asScala.toSet
    val props = db.execute(propsQuery).asScala.foldLeft(Set.empty[(Entity, String, AnyRef)]) {
      case (acc, map) =>
        val triple = (map.get("entity").asInstanceOf[Entity], map.get("key").asInstanceOf[String], convertArrays(map.get("value")))

        acc + triple
    }

    State(nodes, rels, labels, props)
  }

  def convertArrays(value: AnyRef): AnyRef = {
    if (value.getClass.isArray)
      value.asInstanceOf[Array[_]].toIndexedSeq
    else
      value
  }
}
