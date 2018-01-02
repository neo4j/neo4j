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
package org.neo4j.cypher.internal.compiler.v2_3.ast.convert.plannerQuery

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.plannerQuery.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_3.planner.CantHandleQueryException
import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.plans.{IdName, PatternRelationship, ShortestPathPattern}

object PatternConverters {

  object DestructResult { def empty = DestructResult(Seq.empty, Seq.empty, Seq.empty) }

  case class DestructResult(nodeIds: Seq[IdName], rels: Seq[PatternRelationship], shortestPaths: Seq[ShortestPathPattern]) {
    def addNodeId(newId: IdName*) = copy(nodeIds = nodeIds ++ newId)
    def addRel(r: PatternRelationship*) = copy(rels = rels ++ r)
    def addShortestPaths(r: ShortestPathPattern*) = copy(shortestPaths = shortestPaths ++ r)
  }

  implicit class PatternElementDestructor(val pattern: PatternElement) extends AnyVal {
    def destructed: DestructResult = pattern match {
      case relchain: RelationshipChain => relchain.destructedRelationshipChain
      case node: NodePattern           => node.destructedNodePattern
    }
  }

  implicit class NodePatternConverter(val node: NodePattern) extends AnyVal {
    def destructedNodePattern =
      DestructResult(nodeIds = Seq(IdName(node.identifier.get.name)), Seq.empty, Seq.empty)
  }

  implicit class RelationshipChainDestructor(val chain: RelationshipChain) extends AnyVal {
    def destructedRelationshipChain: DestructResult = chain match {
      // (a)->[r]->(b)
      case RelationshipChain(NodePattern(Some(leftNodeId), Seq(), None, _), RelationshipPattern(Some(relId), _, relTypes, length, None, direction), NodePattern(Some(rightNodeId), Seq(), None, _)) =>
        val leftNode = IdName(leftNodeId.name)
        val rightNode = IdName(rightNodeId.name)
        val r = PatternRelationship(IdName(relId.name), (leftNode, rightNode), direction, relTypes, length.asPatternLength)
        DestructResult(Seq(leftNode, rightNode), Seq(r), Seq.empty)

      // ...->[r]->(b)
      case RelationshipChain(relChain: RelationshipChain, RelationshipPattern(Some(relId), _, relTypes, length, None, direction), NodePattern(Some(rightNodeId), Seq(), None, _)) =>
        val destructed = relChain.destructedRelationshipChain
        val leftNode = IdName(destructed.rels.last.right.name)
        val rightNode = IdName(rightNodeId.name)
        val newRel = PatternRelationship(IdName(relId.name), (leftNode, rightNode), direction, relTypes, length.asPatternLength)
        destructed.
          addNodeId(rightNode).
          addRel(newRel)

      case _ =>
        throw new CantHandleQueryException
    }
  }

  implicit class PatternDestructor(val pattern: Pattern) extends AnyVal {
    def destructed: DestructResult = {
      pattern.patternParts.foldLeft(DestructResult.empty) {
        case (acc, NamedPatternPart(ident, sps@ShortestPaths(element, single))) =>
          val desctructedElement: DestructResult = element.destructed
          val pathName = IdName(ident.name)
          val newShortest = ShortestPathPattern(Some(pathName), desctructedElement.rels.head, single)(sps)
          acc.
            addNodeId(desctructedElement.nodeIds:_*).
            addShortestPaths(newShortest)

        case (acc, sps@ShortestPaths(element, single)) =>
          val destructedElement = element.destructed
          val newShortest = ShortestPathPattern(None, destructedElement.rels.head, single)(sps)
          acc.
            addNodeId(destructedElement.nodeIds:_*).
            addShortestPaths(newShortest)

        case (acc, everyPath: EveryPath) =>
          val destructedElement = everyPath.element.destructed
          acc.
            addNodeId(destructedElement.nodeIds:_*).
            addRel(destructedElement.rels:_*)

        case _ =>
          throw new CantHandleQueryException
      }

    }
  }
}
