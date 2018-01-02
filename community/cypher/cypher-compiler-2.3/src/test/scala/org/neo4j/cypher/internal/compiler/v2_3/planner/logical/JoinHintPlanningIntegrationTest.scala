/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner.logical

import java.util.concurrent.atomic.AtomicInteger

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.greedy.{GreedyQueryGraphSolver, expandsOrJoins}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.idp.{IDPQueryGraphSolver, IDPQueryGraphSolverMonitor}
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, LogicalPlan, NodeHashJoin}
import org.neo4j.cypher.internal.compiler.v2_3.planner.{LogicalPlanningTestSupport2, PlannerQuery}
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection
import org.neo4j.cypher.internal.frontend.v2_3.SemanticDirection.{OUTGOING, INCOMING, BOTH}
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite
import org.scalacheck.Gen._
import org.scalacheck.{Gen, Shrink}
import org.scalatest.prop.PropertyChecks
import org.neo4j.cypher.internal.frontend.v2_3.Foldable.FoldableAny

import scala.util.Random

class JoinHintPlanningIntegrationTest extends CypherFunSuite with PropertyChecks with LogicalPlanningTestSupport2 {

  val MinPatternLength = 2
  val MaxPatternLength = 8
  val NumberOfTestRuns = 100
  val MaxDiscardedInputs = 500

  override implicit val generatorDrivenConfig = PropertyCheckConfig(
    minSuccessful = NumberOfTestRuns, maxDiscarded = MaxDiscardedInputs
  )

  val nameSeq = new AtomicInteger

  implicit val shrinkPattern: Shrink[List[Element]] = Shrink { elements =>
    // use the default utility for shrinking of lists
    Shrink.shrinkContainer[List, Element].shrink(elements).map { shrink =>
      // find standalone node
      val standaloneNode = shrink.collectFirst {
        case nr: Node => nr
      }

      if (standaloneNode.isDefined) {
        // either move it to the end of pattern
        shrink.filter(_ != standaloneNode.get) :+ standaloneNode.get
      } else {
        // or strip trailing relationship
        shrink.dropRight(1) :+ shrink.last.asInstanceOf[NodeWithRelationship].node
      }
    }
  }

  test("NodeHashJoin is planned in greedy planner") {
    val solver = new GreedyQueryGraphSolver(expandsOrJoins)

    testPlanner(solver)
  }

  test("NodeHashJoin is planned in IDP planner") {
    val solver = IDPQueryGraphSolver(mock[IDPQueryGraphSolverMonitor])

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
        case PlannerQuery(queryGraph, _, _) if queryGraph.patternRelationships.size == 1 => 100.0
        // everything else - expensive
        case _ => Double.MaxValue
      }

      queryGraphSolver = solver
    }.planFor(cypherQuery)

    semanticPlan.plan
  }

  def joinSymbolsIn(plan: LogicalPlan) = {
    val flattenedPlan = plan.treeFold(Seq.empty[LogicalPlan]) {
      case plan: LogicalPlan => (acc, r) => r(acc :+ plan)
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

  def numberOfNamedNodes(elements: List[Element]): Int = elements.count {
    // either pattern with named node
    case NodeWithRelationship(NamedNode(_), _) => true
    case NodeWithRelationship(NamedLabeledNode(_, _), _) => true
    // or single named node
    case nn: NamedNode => true
    case nln: NamedLabeledNode => true
    case _ => false
  }

  def findFirstNodeName(elements: List[Element]): Option[String] = elements.collectFirst {
    // either name from node that participates in pattern
    case NodeWithRelationship(NamedNode(name), _) => name
    case NodeWithRelationship(NamedLabeledNode(name, _), _) => name
    // or name from single node
    case NamedNode(name) => name
    case NamedLabeledNode(name, _) => name
  }

  sealed trait Element {
    val string: String
  }

  sealed trait Node extends Element

  sealed trait Relationship extends Element

  case class NodeWithRelationship(node: Node, rel: Relationship) extends Element {
    val string = node.string + rel.string
  }

  case class EmptyNode() extends Node {
    val string = "()"
  }

  case class NamedNode(name: String) extends Node {
    val string = s"($name)"
  }

  case class LabeledNode(label: String) extends Node {
    val string = s"(:$label)"
  }

  case class NamedLabeledNode(name: String, label: String) extends Node {
    val string = s"($name:$label)"
  }

  case class EmptyRelationship(direction: SemanticDirection) extends Relationship {
    val string = formatRelationship("", direction)
  }

  case class EmptyRelationshipWithLength(length: String, direction: SemanticDirection) extends Relationship {
    val string = formatRelationship(s"[$length]", direction)
  }

  case class NamedRelationship(name: String, direction: SemanticDirection) extends Relationship {
    val string = formatRelationship(s"[$name]", direction)
  }

  case class NamedRelationshipWithLength(name: String, length: String, direction: SemanticDirection) extends Relationship {
    val string = formatRelationship(s"[$name $length]", direction)
  }

  case class TypedRelationship(relType: String, direction: SemanticDirection) extends Relationship {
    val string = formatRelationship(s"[:$relType]", direction)
  }

  case class TypedRelationshipWithLength(relType: String, length: String, direction: SemanticDirection) extends Relationship {
    val string = formatRelationship(s"[:$relType $length]", direction)
  }

  case class NamedTypedRelationship(name: String, relType: String, direction: SemanticDirection) extends Relationship {
    val string = formatRelationship(s"[$name:$relType]", direction)
  }

  case class NamedTypedRelationshipWithLength(name: String, relType: String, length: String, direction: SemanticDirection) extends Relationship {
    val string = formatRelationship(s"[$name:$relType $length]", direction)
  }

  def formatRelationship(definition: String, direction: SemanticDirection) = direction match {
    case BOTH => s"-$definition-"
    case INCOMING => s"<-$definition-"
    case OUTGOING => s"-$definition->"
  }

  def patterns = Gen.choose(MinPatternLength, MaxPatternLength).flatMap(patternGen)

  def patternGen(maxLength: Int): Gen[List[Element]] =
    if (maxLength == 0)
      nodeGen.map(f => List(f))
    else
      for {
        node <- nodeGen
        rel <- relGen
        other <- patternGen(maxLength - 1)
      } yield NodeWithRelationship(node, rel) :: other

  def relGen = Gen.oneOf(emptyRelGen, emptyRelWithLengthGen, namedRelGen, namedRelWithLengthGen, typedRelGen,
    typedRelWithLengthGen, namedTypedRelGen, namedTypedRelWithLengthGen)

  def emptyRelGen = relDirection.map(EmptyRelationship)

  def emptyRelWithLengthGen =
    for {
      direction <- relDirection
      length <- relLength
    } yield EmptyRelationshipWithLength(length, direction)

  def namedRelGen =
    for {
      name <- relName
      direction <- relDirection
    } yield NamedRelationship(name, direction)

  def namedRelWithLengthGen =
    for {
      name <- relName
      length <- relLength
      direction <- relDirection
    } yield NamedRelationshipWithLength(name, length, direction)

  def typedRelGen =
    for {
      relType <- relTypeName
      direction <- relDirection
    } yield TypedRelationship(relType, direction)

  def typedRelWithLengthGen =
    for {
      relType <- relTypeName
      length <- relLength
      direction <- relDirection
    } yield TypedRelationshipWithLength(relType, length, direction)

  def namedTypedRelGen =
    for {
      name <- relName
      relType <- relTypeName
      direction <- relDirection
    } yield NamedTypedRelationship(name, relType, direction)

  def namedTypedRelWithLengthGen =
    for {
      name <- relName
      relType <- relTypeName
      length <- relLength
      direction <- relDirection
    } yield NamedTypedRelationshipWithLength(name, relType, length, direction)


  def nodeGen = Gen.oneOf(emptyNodeGen, namedNodeGen, labeledNodeGen, namedLabeledNodeGen)

  def emptyNodeGen = Gen.const(EmptyNode())

  def namedNodeGen = nodeName.map(NamedNode)

  def labeledNodeGen = labelName.map(LabeledNode)

  def namedLabeledNodeGen =
    for {
      name <- nodeName
      label <- labelName
    } yield NamedLabeledNode(name, label)

  def nodeName = alphaLowerChar.map(c => s"n${nameSeq.getAndIncrement()}")

  def labelName = alphaUpperChar.map(c => s"L${nameSeq.getAndIncrement()}")

  def relName = alphaLowerChar.map(c => s"r${nameSeq.getAndIncrement()}")

  def relTypeName = alphaUpperChar.map(c => s"T${nameSeq.getAndIncrement()}")

  def relLength = Gen.oneOf(relLengthWithoutBounds, relLengthWithLowerBound, relLengthWithUpperBound,
    relLengthWithLowerAndUpperBound)

  def relLengthWithoutBounds = Gen.const("*")

  def relLengthWithLowerBound = relLengthBound.map(b => s"*$b")

  def relLengthWithUpperBound = relLengthBound.map(b => s"*..$b")

  def relLengthWithLowerAndUpperBound =
    for {
      lower <- relLengthBound
      upper <- relLengthBound
    } yield s"*$lower..$upper"

  def relLengthBound = Gen.choose(1, 10)

  def relDirection = Gen.oneOf(BOTH, INCOMING, OUTGOING)
}
