package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, SemanticCheck, SemanticCheckResult, SemanticCheckable, SemanticChecking}

final case class GraphRefAlias(ref: GraphRef, as: Option[Variable])(val position: InputPosition)
  extends ASTNode with ASTParticle with SemanticCheckable with SemanticChecking {

  def withNewName(newName: Variable) = copy(as = Some(newName))(position)

  override def semanticCheck: SemanticCheck =
    ref.semanticCheck chain as.map(_.declareGraph: SemanticCheck).getOrElse(SemanticCheckResult.success)
}
