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
package org.neo4j.cypher.internal.runtime.spec.fuzz

import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.ProduceResult
import org.neo4j.cypher.internal.runtime.spec.GraphCreation
import org.neo4j.cypher.internal.runtime.spec.fuzz.FuzzTestGraphCreation.GraphType.GraphType
import org.neo4j.graphdb.Entity
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.values.storable.RandomValues
import org.neo4j.values.storable.ValueType
import org.scalacheck.rng.Seed

import scala.jdk.StreamConverters.StreamHasToScala

object FuzzTestGraphCreation {

  private val defaultPropertyConfig: Map[String, Seq[ValueType]] = Map(
    "long" -> Seq(ValueType.LONG),
    "boolean" -> Seq(ValueType.BOOLEAN),
    "string" -> Seq(ValueType.STRING),
    "rand" -> ValueType.ALL_TYPES.filter(v => v != ValueType.UUID && v != ValueType.UUID_ARRAY)
  )

  object GraphType extends Enumeration {
    type GraphType = Value
    val Empty, TwoNodes, TwoConnectedNodes, Grid, Lollipop, NestedStar, BipartiteWithExtraNodes = Value
  }

  case class GraphConfig(
    graphType: GraphType,
    seed: Long,
    propertyConfig: Map[String, Seq[ValueType]] = defaultPropertyConfig
  )

  def graphWithRandProps[CONTEXT <: RuntimeContext](
    config: GraphConfig,
    graphCreation: GraphCreation[CONTEXT]
  ): (Seq[Node], Seq[Relationship]) = {
    val (nodes, rels) = graph(config, graphCreation)
    addRandomProperties(config, nodes ++ rels)
    (nodes, rels)
  }

  def generateReproducibleTestCode(
    config: GraphConfig,
    plan: LogicalPlan,
    morselSize: Int,
    parameters: Map[String, Any],
    seed: Seed
  ): String = {
    val columns =
      plan.asInstanceOf[ProduceResult].columns
        .map(c => "\"" + c.name + "\"")
        .mkString(", ")
    val params = {
      if (parameters.isEmpty) "Map.empty[String, Any]"
      else {
        parameters
          .map {
            case (key, value: String) => s"\"$key\" -> \"${value.replace("\"", "\\\"")}\""
            case (key, value: Int)    => s"\"$key\" -> $value"
            case (key, value: Long)   => s"\"$key\" -> ${value}L"
            case (key, value: Float)  => s"\"$key\" -> ${value}f"
            case (key, value: Double) => s"\"$key\" -> ${value}d"
            case (key, value)         => s"\"$key\" -> $value"
          }
          .mkString(s"Map[String, Any](\n    ", ",\n    ", "\n  )")
      }
    }

    s"""
       |// Copy this test case to FuzzTestFailuresTestBase
       |// Morsel size: $morselSize
       |// Self-contained repro: the plan is inlined below and the graph is rebuilt from
       |// its graph-type seed in randomGraph, so no ScalaCheck seed is needed to replay.
       |// Plan seed (provenance only - does not reproduce on its own, as the plan and
       |// graph-property seeds are independent): $seed
       |test("Fuzz Test Reproduction: ${config.seed}") {
       |  givenGraph(randomGraph(GraphType.${config.graphType}, ${config.seed}L))
       |
       |  val params = $params
       |  val query = new LogicalQueryBuilder(this)
       |  ${plan.toString.trim.lines().toScala(Seq).mkString("  ", "\n    ", "")}
       |  val rewrittenQuery = rewriteQuery(query)
       |
       |  val baseRuntime = org.neo4j.cypher.internal.CommunityInterpretedRuntime // Change if necessary
       |  val expected = execute(rewrittenQuery, baseRuntime, params).awaitAll()
       |  execute(rewrittenQuery, runtime, params) should beColumns($columns)
       |    .withRows(inAnyOrder(expected, listInAnyOrder = true))
       |}
       |""".stripMargin
  }

  private def graph[CONTEXT <: RuntimeContext](
    config: GraphConfig,
    graphCreation: GraphCreation[CONTEXT]
  ): (Seq[Node], Seq[Relationship]) = {
    config.graphType match {
      case GraphType.Empty             => (Seq.empty, Seq.empty)
      case GraphType.TwoNodes          => (graphCreation.nodeGraph(2, "Label"), Seq.empty)
      case GraphType.TwoConnectedNodes => graphCreation.lineGraph(2, "AB", "Label")
      case GraphType.Grid              => graphCreation.gridGraph()
      case GraphType.Lollipop          => graphCreation.lollipopGraph()
      case GraphType.NestedStar =>
        val (nodes, rels, _) = graphCreation.nestedStarGraph(3, 3, "C", "R")
        (nodes, rels)
      case GraphType.BipartiteWithExtraNodes =>
        val (nodesLeft, nodesRight, relsLeft, relsRight) =
          graphCreation.bidirectionalBipartiteGraph(5, "A", "B", "AB", "BA")
        val nodes = nodesLeft ++ nodesRight ++
          graphCreation.nodeGraph(10, "A") ++
          graphCreation.nodeGraph(12, "B") ++
          graphCreation.nodeGraph(5, "C")

        (nodes, relsLeft ++ relsRight)
    }
  }

  private def addRandomProperties(config: GraphConfig, entities: Seq[Entity]): Unit = {
    val rand = RandomValues.create(new java.util.Random(config.seed))
    for {
      entity <- entities
      (propertyKey, types) <- config.propertyConfig
    } {
      entity.setProperty(propertyKey, rand.nextValueOfTypes(types: _*).asObject())
    }
  }
}
