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
package org.neo4j.internal.kernel.api.helpers.traversal.ppbfs

import org.neo4j.cypher.internal.logical.plans.TraversalPathMode
import org.neo4j.cypher.internal.runtime.RuntimeUtilTestSuite
import org.neo4j.cypher.internal.util.test_helpers.GraphOperations
import org.neo4j.cypher.internal.util.test_helpers.InMemoryGraph
import org.neo4j.cypher.internal.util.test_helpers.InMemoryGraph.StringMapped.GraphOps
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.GeneratedPGPathPropagatingBFSTest.CobwebGraph
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.GeneratedPGPathPropagatingBFSTest.DewyCobwebGraph
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.GeneratedPGPathPropagatingBFSTest.HierarchicalCluster
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.GeneratedPGPathPropagatingBFSTest.testGraphs
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTestBase.Nfa
import org.neo4j.internal.kernel.api.helpers.traversal.ppbfs.PGPathPropagatingBFSTestBase.nfa

import scala.language.postfixOps
import scala.util.Random

class GeneratedPGPathPropagatingBFSTest extends RuntimeUtilTestSuite with PGPathPropagatingBFSTestBase {

  test(
    s"running the algorithm gives the same results as naive search"
  ) {
    for {
      nfa <- Seq(
        `(s) ((a)-->(b))* (t)`,
        `(s) ((a)-->(b))+ (t)`,
        `(s) ((a)--(b))+ (t)`,
        `(s) ((a)--(b)--(c))* (t)`
      )
      graph <- testGraphs
      into <- Seq(true, false)
      grouped <- Seq(true, false)
      pathMode <- Seq(TraversalPathMode.Trail, TraversalPathMode.Walk, TraversalPathMode.Acyclic)
      k <- if (pathMode == TraversalPathMode.Trail || pathMode == TraversalPathMode.Acyclic) Seq(Int.MaxValue, 1, 2)
      else Seq(1, 2, 5)
    } {
      var f = fixture()
        .withGraph(graph.graph)
        .from(graph.source)
        .withNfa(nfa)
        .withPathMode(pathMode)

      if (pathMode == TraversalPathMode.Walk) {
        f = f.withMaxDepth(3)
      }

      if (into) {
        f = f.into(graph.target)
      }

      if (grouped) {
        f = f.grouped
      }

      if (k != Int.MaxValue) {
        f = f.withK(k)
      }

      withClue(s"\ngraph=${graph.render}\nnfa=$nfa\ninto=$into\npathMode=$pathMode\ngrouped=$grouped\nk=$k\n") {
        f.assertExpected()
      }
    }
  }

  for {
    template <- Seq(CobwebGraph, DewyCobwebGraph, HierarchicalCluster)
    pathMode <- Seq(TraversalPathMode.Trail, TraversalPathMode.Acyclic)
  } {
    val seed = Random.nextLong()
    val localRandom = new Random(seed)

    val builder = new InMemoryGraph.StringMapped.Builder
    template.createGraph(localRandom, new GraphOps(builder))
    val graph = builder.build()
    val sources = graph.nodesWithLabel("S")

    for {
      query <- template.queries(graph)
    } {
      test(s"graph: ${template.name}, pattern: ${query.nfa}, k: ${query.k}, pathMode: ${pathMode}, seed: $seed") {
        for { source <- sources } {
          fixture()
            .withGraph(graph.graph)
            .withNfa(query.nfa)
            .from(source)
            .withK(query.k)
            .withPathMode(pathMode)
            .assertExpected()
        }
      }

    }
  }

}

object GeneratedPGPathPropagatingBFSTest {

  private case class NamedGraph(render: String, graph: InMemoryGraph, source: Long, target: Long)

  /** a generated series of graphs consisting of a variable length chain of relationships from (start) to (end),
   * with another variable length chain connecting two nodes from the original */
  private lazy val testGraphs: Seq[NamedGraph] = {
    for {
      mainLength <- 1 to 4
      secondaryLength <- 1 to 3
      n1i <- 0 until mainLength
      n2i <- 0 until mainLength
    } yield {
      val builder = new InMemoryGraph.Builder

      val nodes = builder.line(mainLength)
      val start = nodes.head
      val end = nodes.last

      val n1 = nodes(n1i)
      val n2 = nodes(n2i)
      builder.chainRel(n1, n2, secondaryLength)

      NamedGraph(
        s"(n$start)-$mainLength->(n$end), (n$n1)-$secondaryLength->(n$n2)",
        builder.build(),
        start,
        end
      )
    }
  }

  case class Query(nfa: Nfa, k: Int)

  import NfaDsl.Implicits._

  sealed trait FuzzyGraphWithQueries {
    def createGraph(random: Random, graph: GraphOperations): Unit
    def queries(graph: InMemoryGraph.StringMapped): Seq[Query]
    val name: String
  }

  /**
   * Creates a cobweb graph consisting of spokes (line graphs) originating from the same central node,
   * where nodes at the same depth in neighboring spokes are related, forming a cobweb like graph.
   *
   * The length of the spokes is randomized, and we also relate some completely random nodes
   * as well.
   */
  object CobwebGraph extends FuzzyGraphWithQueries {

    val nSpokes = 4
    val minSpokeLength = 3
    val maxSpokeLength = 5
    val nRandomRels = 2

    override def createGraph(random: Random, graph: GraphOperations): Unit = {
      val OUT = "OUT"
      val SIDEWAYS = "SIDEWAYS"
      val RANDOM = "RANDOM"

      val targetLabel = "T"

      val centerNode = graph.createNode("0,0", "S")

      // Create spokes
      val spokeBuilder = Seq.newBuilder[Seq[graph.Node]]
      for (spokeNumber <- 0 until nSpokes) {
        val spokeLength = random.between(minSpokeLength, maxSpokeLength + 1)
        val spoke = (0 until spokeLength).map(depth => graph.createNode(s"$spokeNumber,$depth"))
        var prevNode = centerNode
        for (node <- spoke) {
          prevNode.createRelationshipTo(node, OUT)
          prevNode = node
        }
        spoke.last.addLabel(targetLabel)
        spokeBuilder.addOne(spoke)
      }
      val spokes = spokeBuilder.result()

      // Relate nodes at same depth in neighbouring spokes
      for (depth <- 0 until maxSpokeLength) {
        var prevSpoke = spokes.last
        for (spokeNumber <- 0 until nSpokes) {
          val currSpoke = spokes(spokeNumber)

          if (currSpoke.size > depth && prevSpoke.size > depth) {
            prevSpoke(depth).createRelationshipTo(currSpoke(depth), SIDEWAYS)
          }

          prevSpoke = currSpoke
        }
      }

      // Relate random nodes
      val nodes = centerNode +: spokes.flatten
      for (_ <- 0 until nRandomRels) {
        val i1 = random.between(0, nodes.size)
        val i2 = random.between(0, nodes.size)
        nodes(i1).createRelationshipTo(nodes(i2), RANDOM)
      }
    }

    override def queries(graph: InMemoryGraph.StringMapped): Seq[Query] = {
      val OUT = graph relTypes "OUT"
      val SIDEWAYS = graph relTypes "SIDEWAYS"
      val RANDOM = graph relTypes "RANDOM"

      val `(s:S)` = "s" where graph.hasLabel("S")
      val `(t:T)` = "t" where graph.hasLabel("T")

      Seq(
        Query(
          nfa(`(s:S)` |> (() - OUT -> () - SIDEWAYS - () - OUT - () +) |> `(t:T)`),
          2
        ),
        Query(
          nfa(`(s:S)` |> (() - OUT - () +) |> () - RANDOM - () |> (() - OUT - () +) |> `(t:T)`),
          3
        ),
        Query(
          nfa(`(s:S)` |> (() - (OUT :| RANDOM) -> () - (SIDEWAYS :| RANDOM) - () - (OUT :| RANDOM) - () +) |> `(t:T)`),
          2
        ),
        Query(
          nfa(`(s:S)` |> (() -- () rep (0, 8)) |> `(t:T)`),
          2
        ),
        Query(
          nfa(`(s:S)` |> (() - (OUT :| RANDOM) -> () +) |> `(t:T)`),
          2
        ),
        Query(
          nfa(
            `(s:S)` |> (() - OUT -> () - SIDEWAYS - () - (SIDEWAYS :| RANDOM) - () - SIDEWAYS - () - OUT -> () +) |> `(t:T)`
          ),
          2
        )
      )
    }
    override val name: String = "Cobweb Graph"
  }

  /**
   * Like the cobweb graph but with one length loops on ~half of nodes
   */
  object DewyCobwebGraph extends FuzzyGraphWithQueries {

    val nSpokes = 4
    val minSpokeLength = 3
    val maxSpokeLength = 5
    val nRandomRels = 2

    override def createGraph(random: Random, graph: GraphOperations): Unit = {
      val OUT = "OUT"
      val SIDEWAYS = "SIDEWAYS"
      val RANDOM = "RANDOM"
      val DEW_DROP = "DEW_DROP"

      val targetLabel = "T"

      val centerNode = graph.createNode("0,0", "S")

      // Create spokes
      val spokeBuilder = Seq.newBuilder[Seq[graph.Node]]
      for (spokeNumber <- 0 until nSpokes) {
        val spokeLength = random.between(minSpokeLength, maxSpokeLength + 1)
        val spoke = (0 until spokeLength).map(depth => graph.createNode(s"$spokeNumber,$depth"))
        var prevNode = centerNode
        for (node <- spoke) {
          prevNode.createRelationshipTo(node, OUT)
          prevNode = node
        }
        spoke.last.addLabel(targetLabel)
        spokeBuilder.addOne(spoke)
      }
      val spokes = spokeBuilder.result()

      // Relate nodes at same depth in neighbouring spokes
      for (depth <- 0 until maxSpokeLength) {
        var prevSpoke = spokes.last
        for (spokeNumber <- 0 until nSpokes) {
          val currSpoke = spokes(spokeNumber)

          if (currSpoke.size > depth && prevSpoke.size > depth) {
            prevSpoke(depth).createRelationshipTo(currSpoke(depth), SIDEWAYS)
          }

          prevSpoke = currSpoke
        }
      }

      val nodes = centerNode +: spokes.flatten
      // Create dew-drop loops
      for (node <- nodes) {
        if (random.between(0, 2) == 0) {
          node.createRelationshipTo(node, DEW_DROP)
        }
      }

      // Relate random nodes
      for (_ <- 0 until nRandomRels) {
        val i1 = random.between(0, nodes.size)
        val i2 = random.between(0, nodes.size)
        nodes(i1).createRelationshipTo(nodes(i2), RANDOM)
      }
    }

    override def queries(graph: InMemoryGraph.StringMapped): Seq[Query] = {
      val OUT = graph relTypes "OUT"
      val SIDEWAYS = graph relTypes "SIDEWAYS"
      val RANDOM = graph relTypes "RANDOM"
      val DEW_DROP = graph relTypes "DEW_DROP"
      val `(s:S)` = "s" where graph.hasLabel("S")
      val `(t:T)` = "t" where graph.hasLabel("T")

      Seq(
        Query(
          nfa(`(s:S)` |> (() - OUT -> () - SIDEWAYS - () - OUT - () - DEW_DROP - () +) |> `(t:T)`),
          2
        ),
        Query(
          nfa(`(s:S)` |> (() - OUT - () +) |> () - RANDOM - () |> (() - OUT - () +) |> ("mid" where graph.hasLabel(
            "T"
          )) |> (() - DEW_DROP - ()*) |> "t"),
          3
        ),
        Query(
          nfa(
            `(s:S)` |> (() - OUT - () +) |> () - RANDOM - () |> (() - OUT - () +) |> (() - DEW_DROP - ()*) |> `(t:T)`
          ),
          3
        ),
        Query(
          nfa(
            `(s:S)` |> (() - (OUT :| RANDOM :| DEW_DROP) -> () - (SIDEWAYS :| RANDOM) - () - (OUT :| RANDOM) -> () +) |> `(t:T)`
          ),
          2
        ),
        Query(
          nfa(`(s:S)` |> (() -- () rep (0, 8)) |> `(t:T)`),
          2
        ),
        Query(
          nfa(`(s:S)` |> (() - (OUT :| RANDOM :| DEW_DROP) -> () +) |> `(t:T)`),
          2
        )
      )
    }
    override val name: String = "Dewy Cobweb Graph"
  }

  object HierarchicalCluster extends FuzzyGraphWithQueries {

    val nodesInClusterLowerBound = 2
    val nodesInClusterUpperBound = 4
    val nClustersLowerBound = 2
    val nClustersUpperBound = 3
    val relsBetweenClusters = 2
    val nSourceNodes = 3
    val nTargetNodes = 2

    override def createGraph(random: Random, graph: GraphOperations): Unit = {
      val INTER = "INTER"
      val INTRA = "INTRA"

      val sourceLabel = "S"
      val targetLabel = "T"

      val nClusters = random.between(nClustersLowerBound, nClustersUpperBound + 1)

      def createCluster(size: Int): Seq[graph.Node] = {
        val numberOfPossibleRels = (size * (size - 1)) / 2
        val nRels = random.nextInt(numberOfPossibleRels) + 1 // we always want to create at least one rel
        val nodes = (0 until size).map(_ => graph.createNode())

        for (_ <- 0 until nRels) {
          val i1 = random.nextInt(nodes.size)
          val i2 = random.nextInt(nodes.size)
          nodes(i1).createRelationshipTo(nodes(i2), INTRA)
        }
        nodes
      }

      val clusters = (0 until nClusters).map(_ => {
        val size = random.between(nodesInClusterLowerBound, nodesInClusterUpperBound + 1)
        createCluster(size)
      })

      for {
        i <- 0 until nClusters
        c1 = clusters(i)
        j <- i + 1 until nClusters
        c2 = clusters(j)
        _ <- 0 until relsBetweenClusters
      } {

        val n1 = c1(random.nextInt(c1.size))
        val n2 = c2(random.nextInt(c2.size))

        if (random.nextInt(2) == 0) {
          n1.createRelationshipTo(n2, INTER)
        } else {
          n2.createRelationshipTo(n1, INTER)
        }
      }

      val nodes = clusters.flatten
      for (_ <- 0 until nSourceNodes) {
        nodes(random.nextInt(nodes.size)).addLabel(sourceLabel)
      }
      for (_ <- 0 until nTargetNodes) {
        nodes(random.nextInt(nodes.size)).addLabel(targetLabel)
      }
    }

    override def queries(graph: InMemoryGraph.StringMapped): Seq[Query] = {
      val INTER = graph relTypes "INTER"
      val INTRA = graph relTypes "INTRA"
      val `(s:S)` = "s" where graph.hasLabel("S")
      val `(t:T)` = "t" where graph.hasLabel("T")
      Seq(
        Query(
          nfa(`(s:S)` |> (() - INTRA - () - INTER - () - INTRA - () rep (0, 5)) |> `(t:T)`),
          2
        ),
        Query(
          nfa(`(s:S)` |> (() - INTRA -> () - INTER - () -<- INTRA - ()*) |> `(t:T)`),
          2
        ),
        Query(
          nfa(`(s:S)` |> (() - INTER -> () - INTRA - () -<- INTER - ()*) |> `(t:T)`),
          2
        ),
        Query(
          nfa(`(s:S)` |> (() - INTRA -> () rep (0, 4)) |> () - INTER - () |> (() - INTRA -> () rep (
            0,
            2
          )) |> () - INTER - () |> (() - INTRA -> () rep (0, 4)) |> `(t:T)`),
          3
        ),
        Query(
          nfa(`(s:S)` |> (() - INTRA - () rep (0, 1)) |> (() - INTER - ()*) |> (() - INTRA - () rep (
            0,
            1
          )) |> `(t:T)`),
          1
        )
      )
    }
    override val name: String = "Hierarchical Cluster Graph"
  }
}
