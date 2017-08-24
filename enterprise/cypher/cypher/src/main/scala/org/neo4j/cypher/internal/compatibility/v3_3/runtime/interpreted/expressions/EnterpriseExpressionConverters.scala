/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.expressions

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert.{ExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.{expressions => commands}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.interpreted.{expressions => runtimeExpression}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.{ast => runtimeAst}
import org.neo4j.cypher.internal.frontend.v3_3.ast

object EnterpriseExpressionConverters extends ExpressionConverter {
  override def toCommandExpression(expression: ast.Expression, self: ExpressionConverters): Option[commands.Expression] =
    expression match {
      case runtimeAst.NodeFromRegister(offset, _) =>
        Some(runtimeExpression.NodeFromRegister(offset))
      case runtimeAst.RelationshipFromRegister(offset, _) =>
        Some(runtimeExpression.RelationshipFromRegister(offset))
      case runtimeAst.ReferenceFromRegister(offset) =>
        Some(runtimeExpression.ReferenceFromRegister(offset))
      case runtimeAst.NodeProperty(offset, token, _) =>
        Some(runtimeExpression.NodeProperty(offset, token))
      case runtimeAst.RelationshipProperty(offset, token, _) =>
        Some(runtimeExpression.RelationshipProperty(offset, token))
      case runtimeAst.IdFromSlot(offset) =>
        Some(runtimeExpression.IdFromSlot(offset))
      case runtimeAst.NodePropertyLate(offset, propKey, _) =>
        Some(runtimeExpression.NodePropertyLate(offset, propKey))
      case runtimeAst.RelationshipPropertyLate(offset, propKey, _) =>
        Some(runtimeExpression.RelationshipPropertyLate(offset, propKey))
      case runtimeAst.PrimitiveEquals(a, b) =>
        val lhs = self.toCommandExpression(a)
        val rhs = self.toCommandExpression(b)
        Some(runtimeExpression.PrimitiveEquals(lhs, rhs))
      case runtimeAst.GetDegreePrimitive(offset, typ, direction) =>
        Some(runtimeExpression.GetDegreePrimitive(offset, typ, direction))
      case runtimeAst.NodePropertyExists(offset, token, _) =>
        Some(runtimeExpression.NodePropertyExists(offset, token))
      case runtimeAst.NodePropertyExistsLate(offset, token, _) =>
        Some(runtimeExpression.NodePropertyExistsLate(offset, token))
      case runtimeAst.RelationshipPropertyExists(offset, token, _) =>
        Some(runtimeExpression.RelationshipPropertyExists(offset, token))
      case runtimeAst.RelationshipPropertyExistsLate(offset, token, _) =>
        Some(runtimeExpression.RelationshipPropertyExistsLate(offset, token))

      case _ =>
        None
    }
}
