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

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.{PatternRelationship, IdName}
import org.neo4j.cypher.internal.compiler.v2_1.ast.convert.ExpressionConverters._

class SimpleQueryGraphBuilder extends QueryGraphBuilder {

  object PatternDestructuring {

    def destruct(pattern: Pattern): (Set[IdName], Set[PatternRelationship]) =
      pattern.patternParts.foldLeft((Set.empty[IdName], Set.empty[PatternRelationship])) {
        case ((accIdNames, accRels), everyPath: EveryPath) =>
          val (idNames, rels) = destruct(everyPath.element)
          (accIdNames ++ idNames, accRels ++ rels)
        case _ => throw new CantHandleQueryException
      }

    private def destruct(element: PatternElement): (Set[IdName], Set[PatternRelationship]) = element match {
      case relchain: RelationshipChain => destruct(relchain)
      case node: NodePattern => destruct(node)
    }

    private def destruct(chain: RelationshipChain): (Set[IdName], Set[PatternRelationship]) = chain match {
      case RelationshipChain(NodePattern(Some(leftNodeId), Seq(), None, _), RelationshipPattern(Some(relId), _, relTypes, None, None, direction), NodePattern(Some(rightNodeId), Seq(), None, _)) =>
        val leftNode = IdName(leftNodeId.name)
        val rightNode = IdName(rightNodeId.name)
        val r = PatternRelationship(IdName(relId.name), (leftNode, rightNode), direction, relTypes)
        (Set(leftNode, rightNode), Set(r))

      case RelationshipChain(relChain: RelationshipChain, RelationshipPattern(Some(relId), _, relTypes, None, None, direction), NodePattern(Some(rightNodeId), Seq(), None, _)) =>
        val (idNames, rels) = destruct(relChain)
        val leftNode = IdName(rels.last.nodes._2.name)
        val rightNode = IdName(rightNodeId.name)
        val resultRels = rels + PatternRelationship(IdName(relId.name), (leftNode, rightNode), direction, relTypes)
        (idNames + rightNode, resultRels)

      case _ => throw new CantHandleQueryException
    }

    private def destruct(node: NodePattern): (Set[IdName], Set[PatternRelationship]) = (Set(IdName(node.identifier.get.name)), Set.empty)
  }

  override def produce(ast: Query): QueryGraph = {

    val (projections: Seq[(String, Expression)], selections: Selections, nodes: Set[IdName], rels: Set[PatternRelationship]) = ast match {
      // return 42
      case Query(None, SingleQuery(Seq(Return(false, ListedReturnItems(expressions), None, None, None)))) =>
        val projections: Seq[(String, Expression)] = expressions.map(e => e.name -> e.expression)
        val selections = Selections()
        val nodes = Set.empty
        (projections, selections, nodes, Set.empty)

      // match (n ...) return ...  ||   match (n ...)-[r ...]->(m ...) return ...
      case Query(None, SingleQuery(Seq(
      Match(false, pattern: Pattern, Seq(), optWhere),
      Return(false, ListedReturnItems(expressions), None, None, None)
      ))) =>
        val projections: Seq[(String, Expression)] = expressions.map(e => e.name -> e.expression)
        val (nodeIds: Set[IdName], rels: Set[PatternRelationship]) = PatternDestructuring.destruct(pattern)
        val selections = Selections(optWhere.map(SelectionPredicates.fromWhere).getOrElse(Seq.empty))
        (projections, selections, nodeIds, rels)

      case _ =>
        throw new CantHandleQueryException
    }

    if (projections.exists {
      case (_,e) => e.asCommandExpression.containsAggregate
    }) throw new CantHandleQueryException

    QueryGraph(projections.toMap, selections, nodes, rels)
  }
}
