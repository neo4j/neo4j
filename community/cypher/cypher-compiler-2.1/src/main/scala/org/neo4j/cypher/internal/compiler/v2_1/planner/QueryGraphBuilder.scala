/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.{CantHandleQueryException, ast}
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.ast.NodePattern
import org.neo4j.cypher.internal.compiler.v2_1.ast.EveryPath
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier

object QueryGraphBuilder {
  def build(query: ast.Query): QueryGraph = query match {
    case ast.SingleQuery(clauses) =>
      clauses.head match {
        case ast.Match(_, Pattern(patterns: Seq[PatternPart]), _, _) =>

          var knownNodes = Map[String, Id]()
          var lastNode = -1

          def extractElement(pattern: PatternPart) = pattern match {
            case EveryPath(element) => element
          }

          def addNode(node: NodePattern): (Id, Seq[(Id, Selection)]) = {
            val (id: Id, labels: Seq[Identifier]) = node match {
              case NodePattern(None, labels, _, _) =>
                lastNode = lastNode + 1
                (Id(lastNode), labels)

              case NodePattern(Some(Identifier(nodeName)), labels, _, _) => knownNodes.getOrElse(nodeName, {
                lastNode = lastNode + 1
                val nodeId = Id(lastNode)
                knownNodes = knownNodes + (nodeName -> nodeId)
                (nodeId, labels)
              })
            }
            val selections = labels.map(label =>
              (id, NodeLabelSelection(Label(label.name)))
            )
            (id, selections)
          }

          def extractNodesAndEdges(pattern: PatternElement): (Id, Seq[(Id, Selection)], Seq[GraphRelationship]) = pattern match {
            case node: NodePattern =>
              val (id, selections) = addNode(node)
              (id, selections, Seq.empty)

            case RelationshipChain(element, rel, rNode: NodePattern) =>
              val (lhs, lhsSelections, relationships) = extractNodesAndEdges(element)
              val (rhs, rhsSelections) = addNode(rNode)
              (rhs, lhsSelections ++ rhsSelections, relationships ++ Seq(GraphRelationship(lhs, rhs, rel.direction, Seq.empty)))
          }

          val (edges: Seq[Seq[GraphRelationship]], selections: Seq[Seq[(Id, Selection)]]) = patterns.map(p => {
            val (_, selections, relationships) = extractNodesAndEdges(extractElement(p))
            (relationships, selections)
          }).unzip

          QueryGraph(Id(lastNode), edges.flatten, selections.flatten, Seq.empty)
        case _ => throw new CantHandleQueryException()
      }

    case _ =>
      throw new CantHandleQueryException()
  }
}
