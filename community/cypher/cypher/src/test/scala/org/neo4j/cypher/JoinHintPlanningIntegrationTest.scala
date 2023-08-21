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
package org.neo4j.cypher

import org.neo4j.cypher.internal.compiler.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.planner.logical.QueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.idp.DefaultIDPSolverConfig
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolver
import org.neo4j.cypher.internal.compiler.planner.logical.idp.IDPQueryGraphSolverMonitor
import org.neo4j.cypher.internal.compiler.planner.logical.idp.SingleComponentPlanner
import org.neo4j.cypher.internal.compiler.planner.logical.idp.cartesianProductsOrValueJoins
import org.neo4j.cypher.internal.compiler.planner.logical.steps.ExistsSubqueryPlannerWithCaching
import org.neo4j.cypher.internal.ir.RegularSinglePlannerQuery
import org.neo4j.cypher.internal.logical.plans.LogicalPlan
import org.neo4j.cypher.internal.logical.plans.NodeHashJoin
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.scalacheck.Gen

import scala.util.Random

class JoinHintPlanningIntegrationTest extends CypherFunSuite with PatternGen with LogicalPlanningTestSupport2 {

  test("NodeHashJoin is planned in IDP planner") {
    val monitor = mock[IDPQueryGraphSolverMonitor]
    val planner1 = SingleComponentPlanner(solverConfig = DefaultIDPSolverConfig)(monitor)
    val solver =
      IDPQueryGraphSolver(planner1, cartesianProductsOrValueJoins, ExistsSubqueryPlannerWithCaching())(monitor)

    testPlanner(solver)
  }

  def testPlanner(solver: QueryGraphSolver) = {
    forAll(patterns) { pattern =>
      // reset naming sequence number
      nameSeq.set(0)

      val patternString = pattern.map(_.string).mkString

      val joinNode = findJoinNode(pattern)

      whenever(joinNode.isDefined) {
        val query =
          s"""MATCH $patternString
             |USING JOIN ON ${joinNode.get}
             |RETURN count(*)""".stripMargin

        val plan = logicalPlan(query, solver)
        joinSymbolsIn(plan) should contain(Set(joinNode.get))
      }
    }
  }

  def logicalPlan(cypherQuery: String, solver: QueryGraphSolver) = {
    val semanticPlan = new given {
      cardinality = mapCardinality {
        // expand - cheap
        case RegularSinglePlannerQuery(queryGraph, _, _, _, _) if queryGraph.patternRelationships.size == 1 => 100.0
        // everything else - expensive
        case _ => Double.MaxValue
      }

      queryGraphSolver = solver
    }.getLogicalPlanFor(cypherQuery)

    semanticPlan._1
  }

  def joinSymbolsIn(plan: LogicalPlan) = {
    val flattenedPlan = plan.folder.treeFold(Seq.empty[LogicalPlan]) {
      case plan: LogicalPlan => acc => TraverseChildren(acc :+ plan)
    }

    flattenedPlan.collect {
      case nhj: NodeHashJoin => nhj.nodes.map(_.name)
    }
  }

  def findJoinNode(elements: List[Element]): Option[String] = {
    if (numberOfNamedNodes(elements) < 3) {
      return None
    }

    val firstNodeName = findFirstNodeName(elements).getOrElse(None)
    val lastNodeName = findFirstNodeName(elements.reverse).getOrElse(None)

    var joinNodeName: String = null
    do {
      joinNodeName = findFirstNodeName(Random.shuffle(elements)).get
    } while (joinNodeName == firstNodeName || joinNodeName == lastNodeName)

    Some(joinNodeName)
  }

  def relGen = Gen.oneOf(
    emptyRelGen,
    emptyRelWithLengthGen,
    namedRelGen,
    namedRelWithLengthGen,
    typedRelGen,
    typedRelWithLengthGen,
    namedTypedRelGen,
    namedTypedRelWithLengthGen
  )

  def nodeGen = Gen.oneOf(emptyNodeGen, namedNodeGen, labeledNodeGen, namedLabeledNodeGen)

}
