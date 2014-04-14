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
import org.neo4j.cypher.internal.compiler.v2_1.ast.convert.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans._

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
      // (a)->[r]->(b)
      case RelationshipChain(NodePattern(Some(leftNodeId), Seq(), None, _), RelationshipPattern(Some(relId), _, relTypes, length, None, direction), NodePattern(Some(rightNodeId), Seq(), None, _)) =>
        val leftNode = IdName(leftNodeId.name)
        val rightNode = IdName(rightNodeId.name)
        val r = PatternRelationship(IdName(relId.name), (leftNode, rightNode), direction, relTypes, length)
        (Set(leftNode, rightNode), Set(r))

      // ...->[r]->(b)
      case RelationshipChain(relChain: RelationshipChain, RelationshipPattern(Some(relId), _, relTypes, length, None, direction), NodePattern(Some(rightNodeId), Seq(), None, _)) =>
        val (idNames, rels) = destruct(relChain)
        val leftNode = IdName(rels.last.nodes._2.name)
        val rightNode = IdName(rightNodeId.name)
        val resultRels = rels + PatternRelationship(IdName(relId.name), (leftNode, rightNode), direction, relTypes, length)
        (idNames + rightNode, resultRels)

      case _ => throw new CantHandleQueryException
    }

    private def destruct(node: NodePattern): (Set[IdName], Set[PatternRelationship]) = (Set(IdName(node.identifier.get.name)), Set.empty)
  }

  override def produce(ast: Query): MainQueryGraph = ast match {
    case Query(None, SingleQuery(clauses)) =>
      clauses.foldLeft(QueryGraph.empty)(
        (qg, clause) =>
          clause match {
            case Return(false, ListedReturnItems(expressions), None, None, None) =>
              val projections: Seq[(String, Expression)] = expressions.map(e => e.name -> e.expression)
              if (projections.exists {
                case (_,e) => e.asCommandExpression.containsAggregate
              }) throw new CantHandleQueryException

              qg.copy(projections = projections.toMap)

            case Match(optional@false, pattern: Pattern, Seq(), optWhere) =>
              if (qg.patternRelationships.nonEmpty || qg.patternNodes.nonEmpty)
                throw new CantHandleQueryException

              val (nodeIds: Set[IdName], rels: Set[PatternRelationship]) = PatternDestructuring.destruct(pattern)
              val selections = Selections(optWhere.map(SelectionPredicates.fromWhere).getOrElse(Seq.empty))
              qg.copy(selections = selections, patternNodes = nodeIds, patternRelationships = rels)

            case Match(optional@true, pattern: Pattern, Seq(), optWhere) =>
              val (nodeIds: Set[IdName], rels: Set[PatternRelationship]) = PatternDestructuring.destruct(pattern)
              val selections = Selections(optWhere.map(SelectionPredicates.fromWhere).getOrElse(Seq.empty))
              qg.withAddedOptionalMatch(selections, nodeIds, rels)

            case _ =>
              throw new CantHandleQueryException
          }
      )

    case _ =>
      throw new CantHandleQueryException
  }

  private implicit def asPatternLength(length: Option[Option[Range]]): PatternLength = length match {
    case Some(Some(Range(Some(left), Some(right)))) => VarPatternLength(left.value.toInt, Some(right.value.toInt))
    case Some(Some(Range(Some(left), None)))        => VarPatternLength(left.value.toInt, None)
    case Some(Some(Range(None, Some(right))))       => VarPatternLength(1, Some(right.value.toInt))
    case Some(Some(Range(None, None)))              => VarPatternLength.unlimited
    case Some(None)                                 => VarPatternLength.unlimited
    case None                                       => SimplePatternLength
  }
}
