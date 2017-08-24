package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.symbols.CTString
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, SemanticCheck, SemanticCheckable, SemanticChecking}

final case class GraphUrl(url: Either[Parameter, StringLiteral])(val position: InputPosition)
  extends ASTNode with ASTParticle with SemanticCheckable with SemanticChecking {

  override def semanticCheck: SemanticCheck = url match {
    case Left(parameter) =>
      parameter.semanticCheck(Expression.SemanticContext.Simple) chain parameter.expectType(CTString.covariant)

    case Right(literal) =>
      literal.semanticCheck(Expression.SemanticContext.Simple) chain literal.expectType(CTString.covariant)
  }
}
