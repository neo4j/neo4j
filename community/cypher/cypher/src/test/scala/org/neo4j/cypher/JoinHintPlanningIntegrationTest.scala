/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.neo4j.cypher.internal.compiler.v3_4.planner.LogicalPlanningTestSupport2
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.QueryGraphSolver
import org.neo4j.cypher.internal.compiler.v3_4.planner.logical.idp._
import org.neo4j.cypher.internal.frontend.v3_4.Foldable.FoldableAny
import org.neo4j.cypher.internal.apa.v3_4.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.ir.v3_4.{IdName, RegularPlannerQuery}
import org.neo4j.cypher.internal.v3_3.logical.plans.{LogicalPlan, NodeHashJoin}
import org.scalacheck.Gen

import scala.util.Random

class JoinHintPlanningIntegrationTest extends CypherFunSuite with PatternGen with LogicalPlanningTestSupport2 {

  test("NodeHashJoin is planned in IDP planner") {
    val monitor = mock[IDPQueryGraphSolverMonitor]
    val planner1 = SingleComponentPlanner(monitor, solverConfig = DefaultIDPSolverConfig)
    val solver = IDPQueryGraphSolver(planner1, cartesianProductsOrValueJoins, monitor)

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
        joinSymbolsIn(plan) should contain(Set(IdName(joinNode.get)))
      }
    }
  }

  def logicalPlan(cypherQuery: String, solver: QueryGraphSolver) = {
    val semanticPlan = new given {
      cardinality = mapCardinality {
        // expand - cheap
        case RegularPlannerQuery(queryGraph, _, _) if queryGraph.patternRelationships.size == 1 => 100.0
        // everything else - expensive
        case _ => Double.MaxValue
      }

      queryGraphSolver = solver
    }.getLogicalPlanFor(cypherQuery)

    semanticPlan._2
  }


  def joinSymbolsIn(plan: LogicalPlan) = {
    val flattenedPlan = plan.treeFold(Seq.empty[LogicalPlan]) {
      case plan: LogicalPlan => acc => (acc :+ plan, Some(identity))
    }

    flattenedPlan.collect {
      case nhj: NodeHashJoin => nhj.nodes
    }
  }

  def findJoinNode(elements: List[Element]): Option[String] = {
    if (numberOfNamedNodes(elements) < 3) {
      return None
    }

    val firstNodeName = findFirstNodeName(elements).getOrElse(return None)
    val lastNodeName = findFirstNodeName(elements.reverse).getOrElse(return None)

    var joinNodeName: String = null
    do {
      joinNodeName = findFirstNodeName(Random.shuffle(elements)).get
    } while (joinNodeName == firstNodeName || joinNodeName == lastNodeName)

    Some(joinNodeName)
  }

  def relGen = Gen.oneOf(emptyRelGen, emptyRelWithLengthGen, namedRelGen, namedRelWithLengthGen, typedRelGen,
    typedRelWithLengthGen, namedTypedRelGen, namedTypedRelWithLengthGen)

  def nodeGen = Gen.oneOf(emptyNodeGen, namedNodeGen, labeledNodeGen, namedLabeledNodeGen)

}
