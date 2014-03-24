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
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.IdName
import org.neo4j.cypher.internal.compiler.v2_1.ast.convert.ExpressionConverters._

class SimpleQueryGraphBuilder extends QueryGraphBuilder {

  object NodePatternSeq {
    def unapply(v: Any): Option[Seq[Identifier]] = v match {
      case patterns: Seq[_] =>
        val identifiers = patterns.map {
          case EveryPath(NodePattern(Some(identifier), Seq(), None, _)) => Some(identifier)
          case _                                                        => None
        }
        if (identifiers.exists(_.isEmpty)) None else Some(identifiers.map(_.get))
    }

  }
  override def produce(ast: Query): QueryGraph = {

    val (projections:  Seq[(String, Expression)], selections, nodes: Set[IdName]) = ast match {
      // return 42
      case Query(None, SingleQuery(Seq(Return(false, ListedReturnItems(expressions), None, None, None)))) =>
        val projections: Seq[(String, Expression)] = expressions.map(e => e.name -> e.expression)
        val selections = Selections()
        val nodes = Set.empty
        (projections, selections, nodes)

      // match (n ...) return ...
      case Query(None, SingleQuery(Seq(
        Match(false, Pattern(NodePatternSeq(nodeIdentifiers: Seq[Identifier])), Seq(), optWhere),
        Return(false, ListedReturnItems(expressions), None, None, None)
      ))) =>
        val projections: Seq[(String, Expression)] = expressions.map(e => e.name -> e.expression)
        val nodes = nodeIdentifiers.map(x => IdName(x.name)).toSet
        val selections = Selections(optWhere.map(SelectionPredicates.fromWhere).getOrElse(Seq.empty))
        (projections, selections, nodes)

      case _ =>
        throw new CantHandleQueryException
    }

    if (projections.exists {
      case (_,e) => e.asCommandExpression.containsAggregate
    }) throw new CantHandleQueryException

    QueryGraph(projections.toMap, selections, nodes)
  }
}
