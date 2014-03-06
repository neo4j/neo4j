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
import org.neo4j.cypher.internal.compiler.v2_1.planner.logical.Id
import org.neo4j.cypher.internal.compiler.v2_1.ast.{LabelName, Identifier, HasLabels, Where}

/*
An abstract representation of the query graph being solved at the current step
 */
case class QueryGraph(projections: Map[String, ast.Expression],
                      selections: Selections,
                      identifiers: Set[Id])

object SelectionPredicates {
  def fromWhere(where: Where): Seq[(Set[Id], ast.Expression)] = where.expression match {
    case expr@HasLabels(identifier@Identifier(name), labels) =>
      labels.map( (label: LabelName) => Set(Id(name)) -> HasLabels(identifier, Seq(label))(expr.position) )
    case _ =>
      throw new CantHandleQueryException
  }
}

case class Selections(predicates: Seq[(Set[Id], ast.Expression)] = Seq.empty) {
  def apply(availableIds: Set[Id]): Seq[ast.Expression] =
    predicates.collect { case (k, v) if k.subsetOf(availableIds) => v }
}
