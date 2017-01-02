/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_2.ast.convert.commands

import org.neo4j.cypher.internal.compiler.v3_2._
import org.neo4j.cypher.internal.compiler.v3_2.ast.convert.commands.ExpressionConverters._
import org.neo4j.cypher.internal.compiler.v3_2.commands.{expressions => commandexpressions, values => commandvalues}
import org.neo4j.cypher.internal.frontend.v3_2.ast

object StatementConverters {

  implicit class StartItemConverter(val item: ast.StartItem) extends AnyVal {
    def asCommandStartItem = item match {
      case ast.NodeByIds(variable, ids) =>
        commands.NodeById(variable.name, commandexpressions.Literal(ids.map(_.value)))
      case ast.NodeByParameter(variable, parameter) =>
        commands.NodeById(variable.name, toCommandParameter(parameter))
      case ast.AllNodes(variable) =>
        commands.AllNodes(variable.name)
      case ast.NodeByIdentifiedIndex(variable, index, key, value) =>
        commands.NodeByIndex(variable.name, index, commandexpressions.Literal(key), toCommandExpression(value))
      case ast.NodeByIndexQuery(variable, index, query) =>
        commands.NodeByIndexQuery(variable.name, index, toCommandExpression(query))
      case ast.RelationshipByIds(variable, ids) =>
        commands.RelationshipById(variable.name, commandexpressions.Literal(ids.map(_.value)))
      case ast.RelationshipByParameter(variable, parameter) =>
        commands.RelationshipById(variable.name, toCommandParameter(parameter))
      case ast.AllRelationships(variable) =>
        commands.AllRelationships(variable.name)
      case ast.RelationshipByIdentifiedIndex(variable, index, key, value) =>
        commands.RelationshipByIndex(variable.name, index, commandexpressions.Literal(key), toCommandExpression(value))
      case ast.RelationshipByIndexQuery(variable, index, query) =>
        commands.RelationshipByIndexQuery(variable.name, index, toCommandExpression(query))
    }
  }
}
