package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, SemanticCheck, SemanticCheckResult, SemanticCheckable, SemanticChecking}

sealed trait GraphRef extends ASTNode with ASTParticle with SemanticCheckable with SemanticChecking

final case class NamedGraph(alias: Variable)(val position: InputPosition) extends GraphRef {
  override def semanticCheck: SemanticCheck = alias.semanticCheck(Expression.SemanticContext.Simple)
}

final case class SourceGraph()(val position: InputPosition) extends GraphRef {
  override def semanticCheck: SemanticCheck = SemanticCheckResult.success
}

final case class TargetGraph()(val position: InputPosition) extends GraphRef {
  override def semanticCheck: SemanticCheck = SemanticCheckResult.success
}

final case class DefaultGraph()(val position: InputPosition) extends GraphRef {
  override def semanticCheck: SemanticCheck = SemanticCheckResult.success
}
