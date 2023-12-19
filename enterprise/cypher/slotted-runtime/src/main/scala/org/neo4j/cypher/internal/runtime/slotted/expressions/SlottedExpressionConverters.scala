/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cypher.internal.runtime.slotted.expressions

import org.neo4j.cypher.internal.compatibility.v3_4.runtime.{ast => runtimeAst}
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.{ExpressionConverter, ExpressionConverters}
import org.neo4j.cypher.internal.runtime.interpreted.commands.{expressions => commands}
import org.neo4j.cypher.internal.runtime.slotted.{expressions => runtimeExpression}
import org.neo4j.cypher.internal.runtime.slotted.expressions.SlottedProjectedPath._
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.{expressions => ast}

object SlottedExpressionConverters extends ExpressionConverter {
  override def toCommandExpression(expression: ast.Expression, self: ExpressionConverters): Option[commands.Expression] =
    expression match {
      case runtimeAst.NodeFromSlot(offset, _) =>
        Some(runtimeExpression.NodeFromSlot(offset))
      case runtimeAst.RelationshipFromSlot(offset, _) =>
        Some(runtimeExpression.RelationshipFromSlot(offset))
      case runtimeAst.ReferenceFromSlot(offset, _) =>
        Some(runtimeExpression.ReferenceFromSlot(offset))
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
      case runtimeAst.NullCheck(offset, inner) =>
        val a = self.toCommandExpression(inner)
        Some(runtimeExpression.NullCheck(offset, a))
      case runtimeAst.NullCheckVariable(offset, inner) =>
        val a = self.toCommandExpression(inner)
        Some(runtimeExpression.NullCheck(offset, a))
      case runtimeAst.NullCheckProperty(offset, inner) =>
        val a = self.toCommandExpression(inner)
        Some(runtimeExpression.NullCheck(offset, a))
      case e: ast.PathExpression =>
        Some(toCommandProjectedPath(e, self))
      case runtimeAst.IsPrimitiveNull(offset) =>
        Some(runtimeExpression.IsPrimitiveNull(offset))
      case _ =>
        None
    }

  def toCommandProjectedPath(e: ast.PathExpression, self: ExpressionConverters): SlottedProjectedPath = {
    def project(pathStep: PathStep): Projector = pathStep match {

      case NodePathStep(nodeExpression, next) =>
        singleNodeProjector(toCommandExpression(nodeExpression, self).get, project(next))

      case SingleRelationshipPathStep(relExpression, SemanticDirection.INCOMING, next) =>
        singleIncomingRelationshipProjector(toCommandExpression(relExpression, self).get, project(next))

      case SingleRelationshipPathStep(relExpression, SemanticDirection.OUTGOING, next) =>
        singleOutgoingRelationshipProjector(toCommandExpression(relExpression, self).get, project(next))

      case SingleRelationshipPathStep(relExpression, SemanticDirection.BOTH, next) =>
        singleUndirectedRelationshipProjector(toCommandExpression(relExpression, self).get, project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.INCOMING, next) =>
        multiIncomingRelationshipProjector(toCommandExpression(relExpression, self).get, project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.OUTGOING, next) =>
        multiOutgoingRelationshipProjector(toCommandExpression(relExpression, self).get, project(next))

      case MultiRelationshipPathStep(relExpression, SemanticDirection.BOTH, next) =>
        multiUndirectedRelationshipProjector(toCommandExpression(relExpression, self).get, project(next))

      case NilPathStep =>
        nilProjector
    }

    val projector = project(e.step)
    // Symbol table dependencies is only used for runtime expressions by PatternMatcher. It would be nice to
    // get rid of this from runtime expressions.
    val dependencies = e.step.dependencies.flatMap(_.dependencies).map(_.name)

    SlottedProjectedPath(dependencies, projector)
  }

}
