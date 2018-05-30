/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.cypher.internal.ir.v3_5.helpers

import org.opencypher.v9_0.util.{FreshIdNameGenerator, InternalException}
import org.neo4j.cypher.internal.ir.v3_5.helpers.ExpressionConverters._
import org.neo4j.cypher.internal.ir.v3_5.{PatternRelationship, ShortestPathPattern}
import org.opencypher.v9_0.expressions._

object PatternConverters {

  object DestructResult { def empty = DestructResult(Seq.empty, Seq.empty, Seq.empty) }

  case class DestructResult(nodeIds: Seq[String], rels: Seq[PatternRelationship], shortestPaths: Seq[ShortestPathPattern]) {
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
    def destructedNodePattern =
      DestructResult(nodeIds = Seq(node.variable.get.name), Seq.empty, Seq.empty)
  }

  //RelationshipChain(
  // NodePattern(Some(Variable(  UNNAMED6)),List(),Some(Variable(  UNNAMED7))),
  // RelationshipPattern(Some(Variable(r)),List(),None,None,OUTGOING,false),
  // NodePattern(Some(Variable(  UNNAMED8)),List(),Some(Variable(  UNNAMED9))))
  implicit class RelationshipChainDestructor(val chain: RelationshipChain) extends AnyVal {
    def destructedRelationshipChain: DestructResult = chain match {
      // (a)->[r]->(b)
      case RelationshipChain(NodePattern(Some(leftNodeId), Seq(), None),
                             RelationshipPattern(Some(relId), relTypes, length, None, direction, _),
                             NodePattern(Some(rightNodeId), Seq(), None)) =>
        val leftNode = leftNodeId.name
        val rightNode = rightNodeId.name
        val r = PatternRelationship(relId.name, (leftNode, rightNode), direction, relTypes, length.asPatternLength)
        DestructResult(Seq(leftNode, rightNode), Seq(r), Seq.empty)

      // ...->[r]->(b)
      case RelationshipChain(relChain: RelationshipChain, RelationshipPattern(Some(relId), relTypes, length, None, direction, _), NodePattern(Some(rightNodeId), Seq(), None)) =>
        val destructed = relChain.destructedRelationshipChain
        val leftNode = destructed.rels.last.right
        val rightNode = rightNodeId.name
        val newRel = PatternRelationship(relId.name, (leftNode, rightNode), direction, relTypes, length.asPatternLength)
        destructed.
          addNodeId(rightNode).
          addRel(newRel)
    }
  }

  implicit class PatternDestructor(val pattern: Pattern) extends AnyVal {
    def destructed: DestructResult = {
      pattern.patternParts.foldLeft(DestructResult.empty) {
        case (acc, NamedPatternPart(ident, sps@ShortestPaths(element, single))) =>
          val destructedElement: DestructResult = element.destructed
          val pathName = ident.name
          val newShortest = ShortestPathPattern(Some(pathName), destructedElement.rels.head, single)(sps)
          acc.
            addNodeId(destructedElement.nodeIds:_*).
            addShortestPaths(newShortest)

        case (acc, sps@ShortestPaths(element, single)) =>
          val destructedElement = element.destructed
          val newShortest = ShortestPathPattern(Some(FreshIdNameGenerator.name(sps.position)), destructedElement.rels.head, single)(sps)
          acc.
            addNodeId(destructedElement.nodeIds:_*).
            addShortestPaths(newShortest)

        case (acc, everyPath: EveryPath) =>
          val destructedElement = everyPath.element.destructed
          acc.
            addNodeId(destructedElement.nodeIds:_*).
            addRel(destructedElement.rels:_*)

        case p =>
          throw new InternalException(s"Unknown pattern element encountered $p")
      }

    }
  }
}
