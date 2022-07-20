/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.cypher.internal.ir.helpers

import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.FixedQuantifier
import org.neo4j.cypher.internal.expressions.GraphPatternQuantifier
import org.neo4j.cypher.internal.expressions.IntervalQuantifier
import org.neo4j.cypher.internal.expressions.LabelExpression.getRelTypes
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PlusQuantifier
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.ShortestPaths
import org.neo4j.cypher.internal.expressions.SimplePattern
import org.neo4j.cypher.internal.expressions.StarQuantifier
import org.neo4j.cypher.internal.ir.NodeBinding
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.ir.QueryGraph
import org.neo4j.cypher.internal.ir.Selections
import org.neo4j.cypher.internal.ir.ShortestPathPattern
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.RangeConvertor
import org.neo4j.cypher.internal.ir.helpers.PatternConverters.PathConcatenationDestructor.FoldState
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Repetition
import org.neo4j.cypher.internal.util.UpperBound
import org.neo4j.exceptions.InternalException

object PatternConverters {

  object DestructResult { def empty: DestructResult = DestructResult(Seq.empty, Seq.empty, Seq.empty, Seq.empty) }

  case class DestructResult(
    nodeIds: Seq[String],
    rels: Seq[PatternRelationship],
    quantifiedPathPatterns: Seq[QuantifiedPathPattern],
    shortestPaths: Seq[ShortestPathPattern]
  ) {
    def addNodeId(newId: String*): DestructResult = copy(nodeIds = nodeIds ++ newId)
    def addRel(r: PatternRelationship*): DestructResult = copy(rels = rels ++ r)
    def addShortestPaths(r: ShortestPathPattern*): DestructResult = copy(shortestPaths = shortestPaths ++ r)

    def addQuantifiedPathPattern(qpp: QuantifiedPathPattern*): DestructResult =
      copy(quantifiedPathPatterns = quantifiedPathPatterns ++ qpp)

    def merge(other: DestructResult): DestructResult = {
      DestructResult(
        nodeIds = nodeIds ++ other.nodeIds,
        rels = rels ++ other.rels,
        quantifiedPathPatterns = quantifiedPathPatterns ++ other.quantifiedPathPatterns,
        shortestPaths = shortestPaths ++ other.shortestPaths
      )
    }
  }

  implicit class PatternElementDestructor(val pattern: PatternElement) extends AnyVal {

    def destructed: DestructResult = pattern match {
      case relchain: RelationshipChain => relchain.destructedRelationshipChain
      case node: NodePattern           => node.destructedNodePattern
      case pc: PathConcatenation       => pc.destructedPathConcatenation
    }
  }

  implicit class NodePatternConverter(val node: NodePattern) extends AnyVal {

    def destructedNodePattern: DestructResult =
      DestructResult(nodeIds = Seq(node.variable.get.name), Seq.empty, Seq.empty, Seq.empty)
  }

  implicit class RelationshipChainDestructor(val chain: RelationshipChain) extends AnyVal {

    def destructedRelationshipChain: DestructResult = {
      val rightNodeName =
        chain.rightNode.variable.getOrElse(throw new IllegalArgumentException("Missing variable in node pattern")).name

      val relationshipName = chain.relationship.variable.getOrElse(
        throw new IllegalArgumentException("Missing variable in relationship pattern")
      ).name
      val relationshipDirection = chain.relationship.direction
      val relationshipTypes = getRelTypes(chain.relationship.labelExpression)
      val relationshipLength = chain.relationship.length.asPatternLength

      chain.element match {
        // (a)->[r]->(b)
        case leftNode: NodePattern =>
          val leftNodeName =
            leftNode.variable.getOrElse(throw new IllegalArgumentException("Missing variable in node pattern")).name
          val relationship = PatternRelationship(
            relationshipName,
            (leftNodeName, rightNodeName),
            relationshipDirection,
            relationshipTypes,
            relationshipLength
          )
          DestructResult(Seq(leftNodeName, rightNodeName), Seq(relationship), Seq.empty, Seq.empty)

        // ...->[r]->(b)
        case leftChain: RelationshipChain =>
          val destructed = leftChain.destructedRelationshipChain
          val leftNodeName = destructed.rels.last.right
          val newRelationship = PatternRelationship(
            relationshipName,
            (leftNodeName, rightNodeName),
            relationshipDirection,
            relationshipTypes,
            relationshipLength
          )
          destructed.addNodeId(rightNodeName).addRel(newRelationship)
      }
    }
  }

  implicit class PathConcatenationDestructor(val concatenation: PathConcatenation) extends AnyVal {

    def destructedPathConcatenation: DestructResult = {
      val (_, result) = concatenation.factors.foldLeft((FoldState.initial, DestructResult.empty)) {

        case ((FoldState.Initial | FoldState.EncounteredNode(_), acc), pattern: SimplePattern) =>
          (FoldState.EncounteredNode(rightNodeName(pattern)), acc.merge(pattern.destructed))

        case ((FoldState.EncounteredNode(nodeName), acc), qp: QuantifiedPath) =>
          (FoldState.EncounteredQuantifiedPath(nodeName, qp), acc)

        case ((FoldState.EncounteredQuantifiedPath(leftNode, quantifiedPath), acc), rightPattern: SimplePattern) =>
          val qpp = {
            // _left_ node of the _rightPattern_ becomes _right_ outer node of the QPP
            val rightNode = leftNodeName(rightPattern)
            makeQuantifiedPathPattern(leftNode, quantifiedPath, rightNode)
          }

          // _right_ node of the _rightPattern_ becomes _left_ outer node of the next QPP
          val nextState = FoldState.EncounteredNode(rightNodeName(rightPattern))
          val nextAcc = acc.merge(rightPattern.destructed).addQuantifiedPathPattern(qpp)
          (nextState, nextAcc)
      }
      result
    }

    private def leftNodeName(pattern: SimplePattern): String = pattern match {
      case node: NodePattern      => node.variable.get.name
      case rel: RelationshipChain => rel.leftNode.variable.get.name
    }

    private def rightNodeName(pattern: SimplePattern): String = pattern match {
      case node: NodePattern      => node.variable.get.name
      case rel: RelationshipChain => rel.rightNode.variable.get.name
    }

    private def makeQuantifiedPathPattern(
      outerLeft: String,
      quantifiedPath: QuantifiedPath,
      outerRight: String
    ): QuantifiedPathPattern = {
      val (innerLeft, innerRight) = quantifiedPath.part.element match {
        case node: NodePattern      => (node.variable.get.name, node.variable.get.name)
        case rel: RelationshipChain => (rel.leftNode.variable.get.name, rel.rightNode.variable.get.name)
      }

      val qg = {
        val content = quantifiedPath.part.element.destructed
        QueryGraph(
          patternNodes = content.nodeIds.toSet,
          patternRelationships = content.rels.toSet,
          argumentIds = Set.empty,
          selections = Selections.empty
        )
      }

      QuantifiedPathPattern(
        leftBinding = NodeBinding(outerLeft, innerLeft),
        rightBinding = NodeBinding(outerRight, innerRight),
        pattern = qg,
        repetition = quantifiedPath.quantifier.toRepetition
      )
    }
  }

  object PathConcatenationDestructor {
    sealed private trait FoldState

    private object FoldState {
      def initial: FoldState = Initial

      case object Initial extends FoldState
      final case class EncounteredNode(name: String) extends FoldState
      final case class EncounteredQuantifiedPath(leftNodeName: String, qpp: QuantifiedPath) extends FoldState
    }
  }

  implicit class PatternDestructor(val pattern: Pattern) extends AnyVal {

    def destructed(anonymousVariableNameGenerator: AnonymousVariableNameGenerator): DestructResult = {
      pattern.patternParts.foldLeft(DestructResult.empty) {
        case (acc, NamedPatternPart(ident, sps @ ShortestPaths(element, single))) =>
          val destructedElement: DestructResult = element.destructed
          val pathName = ident.name
          val newShortest = ShortestPathPattern(Some(pathName), destructedElement.rels.head, single)(sps)
          acc.addNodeId(destructedElement.nodeIds: _*).addShortestPaths(newShortest)

        case (acc, sps @ ShortestPaths(element, single)) =>
          val destructedElement = element.destructed
          val newShortest =
            ShortestPathPattern(Some(anonymousVariableNameGenerator.nextName), destructedElement.rels.head, single)(sps)
          acc.addNodeId(destructedElement.nodeIds: _*).addShortestPaths(newShortest)

        case (acc, everyPath: EveryPath) =>
          val destructedElement = everyPath.element.destructed
          acc.merge(destructedElement)

        case p =>
          throw new InternalException(s"Unknown pattern element encountered $p")
      }

    }
  }

  implicit class GraphPatternQuantifierToRepetitionSyntax(gpq: GraphPatternQuantifier) {

    def toRepetition: Repetition = {
      gpq match {
        case PlusQuantifier()       => Repetition(min = 1, max = UpperBound.Unlimited)
        case StarQuantifier()       => Repetition(min = 0, max = UpperBound.Unlimited)
        case FixedQuantifier(value) => Repetition(min = value.value, max = UpperBound.Limited(value.value))
        case IntervalQuantifier(lower, upper) =>
          Repetition(
            min = lower.fold(0L)(_.value),
            max = upper.fold(UpperBound.unlimited)(x => UpperBound.Limited(x.value))
          )
      }
    }
  }
}
