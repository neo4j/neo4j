package org.neo4j.cypher.internal.compatibility.v3_3

import org.neo4j.cypher.internal.util.v3_4.{ASTNode => ASTNodeV3_4}
import org.neo4j.cypher.internal.frontend.v3_3.ast.{ASTNode => ASTNodeV3_3}
import org.neo4j.cypher.internal.frontend.v3_3.{ast => astV3_3}
import org.neo4j.cypher.internal.v3_4.{expressions => expressionsV3_4}
import org.neo4j.cypher.internal.frontend.v3_3.ast.{Expression => ExpressionV3_3}
import org.neo4j.cypher.internal.v3_4.expressions.{Expression => ExpressionV3_4}

object ASTConverter {

  def convertExpression(expression: ExpressionV3_3): ExpressionV3_4 = expression match {
    case astV3_3.Add(lhs, rhs) => expressionsV3_4.Add(convertExpression(lhs), convertExpression(rhs))(helpers.as3_4(expression.position))
      // TODO slightly more expressions
  }
}
