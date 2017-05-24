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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert

import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.ExpressionConverters.{toCommandExpression, toCommandParameter}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.{expressions => commandexpressions, values => commandvalues}
import org.neo4j.cypher.internal.compiler.v3_3.planDescription.InternalPlanDescription.Arguments
import org.neo4j.cypher.internal.compiler.v3_3.planner.logical.plans.Argument
import org.neo4j.cypher.internal.frontend.v3_3.ast

object StatementConverters {

  implicit class StartItemConverter(val item: ast.StartItem) extends AnyVal {
    def asCommandStartItem = item match {
      case ast.NodeByIds(variable, ids) =>
        val args = ids.map(Arguments.Expression)
        commands.NodeById(variable.name, commandexpressions.Literal(ids.map(_.value)), args)
      case ast.NodeByParameter(variable, parameter) =>
        commands.NodeById(variable.name, toCommandParameter(parameter), Seq(Arguments.Expression(parameter)))
      case ast.AllNodes(variable) =>
        commands.AllNodes(variable.name)
      case ast.NodeByIdentifiedIndex(variable, index, key, value) =>
        val args = Seq(
          Arguments.Expression(value),
          Arguments.LegacyIndex(index),
          Arguments.KeyNames(Seq(key)))
        commands.NodeByIndex(variable.name, index, commandexpressions.Literal(key), toCommandExpression(value), args)
      case ast.NodeByIndexQuery(variable, index, query) =>
        val args = Seq(
          Arguments.Expression(query),
          Arguments.LegacyIndex(index))
        commands.NodeByIndexQuery(variable.name, index, toCommandExpression(query), args)
      case ast.RelationshipByIds(variable, ids) =>
        commands.RelationshipById(variable.name, commandexpressions.Literal(ids.map(_.value)), ids.map(Arguments.Expression))
      case ast.RelationshipByParameter(variable, parameter) =>
        commands.RelationshipById(variable.name, toCommandParameter(parameter), Seq(Arguments.Expression(parameter)))
      case ast.AllRelationships(variable) =>
        commands.AllRelationships(variable.name)
      case ast.RelationshipByIdentifiedIndex(variable, index, key, value) =>
        val args = Seq(
          Arguments.Expression(value),
          Arguments.LegacyIndex(index),
          Arguments.KeyNames(Seq(key)))
        commands.RelationshipByIndex(variable.name, index, commandexpressions.Literal(key), toCommandExpression(value), args)
      case ast.RelationshipByIndexQuery(variable, index, query) =>
        val args = Seq(
          Arguments.LegacyIndex(index),
          Arguments.Expression(query))
        commands.RelationshipByIndexQuery(variable.name, index, toCommandExpression(query), args)
    }
  }
}
