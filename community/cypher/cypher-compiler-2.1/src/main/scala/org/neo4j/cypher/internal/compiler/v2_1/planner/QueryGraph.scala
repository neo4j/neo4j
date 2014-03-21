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

import org.neo4j.cypher.internal.compiler.v2_1.ast
import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.ast.Where
import org.neo4j.cypher.internal.compiler.v2_1.ast.LabelName
import org.neo4j.cypher.internal.compiler.v2_1.ast.Identifier
import org.neo4j.cypher.internal.compiler.v2_1.ast.HasLabels
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.plans.{PatternRelationship, IdName}

/*
An abstract representation of the query graph being solved at the current step
 */
case class QueryGraph(projections: Map[String, ast.Expression],
                      selections: Selections,
                      patternNodes: Set[IdName],
                      patternRelationships: Set[PatternRelationship])

object SelectionPredicates {
  def fromWhere(where: Where): Seq[(Set[IdName], Expression)] = extractPredicates(where.expression)

  private def idNames(predicate: Expression): Set[IdName] = predicate.treeFold(Set.empty[IdName]) {
    case id: Identifier =>
      (acc: Set[IdName], _) => acc + IdName(id.name)
  }

  private def extractPredicates(predicate: ast.Expression): Seq[(Set[IdName], Expression)] = {
    predicate.treeFold(Seq.empty[(Set[IdName], ast.Expression)]) {
      // n:Label
      case predicate@HasLabels(identifier@Identifier(name), labels) =>
        (acc, _) => acc ++ labels.map { label: LabelName =>
          Set(IdName(name)) -> predicate.copy(labels = Seq(label))(predicate.position)
        }
      // and
      case _: And =>
        (acc, children) => children(acc)
      case predicate: Expression =>
        (acc, _) => acc :+ (idNames(predicate) -> predicate)
    }
  }
}


