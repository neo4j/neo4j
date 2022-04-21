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
import org.neo4j.cypher.internal.expressions.InvalidNodePattern
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.ShortestPaths
import org.neo4j.cypher.internal.ir.PatternRelationship
import org.neo4j.cypher.internal.ir.ShortestPathPattern
import org.neo4j.cypher.internal.ir.helpers.ExpressionConverters.RangeConvertor
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.exceptions.InternalException

object PatternConverters {

  object DestructResult { def empty: DestructResult = DestructResult(Seq.empty, Seq.empty, Seq.empty) }

  case class DestructResult(
    nodeIds: Seq[String],
    rels: Seq[PatternRelationship],
    shortestPaths: Seq[ShortestPathPattern]
  ) {
    def addNodeId(newId: String*): DestructResult = copy(nodeIds = nodeIds ++ newId)
    def addRel(r: PatternRelationship*): DestructResult = copy(rels = rels ++ r)
    def addShortestPaths(r: ShortestPathPattern*): DestructResult = copy(shortestPaths = shortestPaths ++ r)
  }

  implicit class PatternElementDestructor(val pattern: PatternElement) extends AnyVal {

    def destructed: DestructResult = pattern match {
      case relchain: RelationshipChain => relchain.destructedRelationshipChain
      case node: NodePattern           => node.destructedNodePattern
    }
  }

  implicit class NodePatternConverter(val node: NodePattern) extends AnyVal {

    def destructedNodePattern: DestructResult =
      DestructResult(nodeIds = Seq(node.variable.get.name), Seq.empty, Seq.empty)
  }

  implicit class RelationshipChainDestructor(val chain: RelationshipChain) extends AnyVal {

    def destructedRelationshipChain: DestructResult = {
      val rightNodeName =
        chain.rightNode.variable.getOrElse(throw new IllegalArgumentException("Missing variable in node pattern")).name

      val relationshipName = chain.relationship.variable.getOrElse(
        throw new IllegalArgumentException("Missing variable in relationship pattern")
      ).name
      val relationshipDirection = chain.relationship.direction
      val relationshipTypes = chain.relationship.types
      val relationshipLength = chain.relationship.length.asPatternLength

      chain.element match {
        case _: InvalidNodePattern => throw new IllegalArgumentException("Invalid node pattern")

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
          DestructResult(Seq(leftNodeName, rightNodeName), Seq(relationship), Seq.empty)

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
          acc.addNodeId(destructedElement.nodeIds: _*).addRel(destructedElement.rels: _*)

        case p =>
          throw new InternalException(s"Unknown pattern element encountered $p")
      }

    }
  }
}
