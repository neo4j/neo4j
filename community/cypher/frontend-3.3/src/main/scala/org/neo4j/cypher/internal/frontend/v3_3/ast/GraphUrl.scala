package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.symbols.CTString
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, SemanticCheck, SemanticCheckable, SemanticChecking}

final case class GraphUrl(url: Expression)(val position: InputPosition)
  extends ASTNode with ASTParticle with SemanticCheckable with SemanticChecking {

  override def semanticCheck: SemanticCheck = {
    url.semanticCheck(Expression.SemanticContext.Simple) chain url.expectType(CTString.covariant)
  }
}
