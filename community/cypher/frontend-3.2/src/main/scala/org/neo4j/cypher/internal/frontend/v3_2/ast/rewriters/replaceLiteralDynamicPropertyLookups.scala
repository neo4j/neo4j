package org.neo4j.cypher.internal.frontend.v3_2.ast.rewriters

import org.neo4j.cypher.internal.frontend.v3_2.ast.{Property, PropertyKeyName}
import org.neo4j.cypher.internal.frontend.v3_2.{Rewriter, ast, bottomUp}

case object replaceLiteralDynamicPropertyLookups extends Rewriter {

  private val instance = bottomUp(Rewriter.lift {
    case index @ ast.ContainerIndex(expr, lit: ast.StringLiteral) =>
      Property(expr, PropertyKeyName(lit.value)(lit.position))(index.position)
  })

  override def apply(v: AnyRef): AnyRef = instance(v)
}
