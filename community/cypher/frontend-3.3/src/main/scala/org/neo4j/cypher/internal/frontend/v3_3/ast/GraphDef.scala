package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.symbols.CTGraphRef
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, SemanticCheck, SemanticCheckResult, SemanticCheckable, SemanticChecking}

sealed trait GraphDef extends ASTNode with ASTParticle with SemanticCheckable with SemanticChecking {
  def name: Variable = alias.get
  def alias: Option[Variable]
  protected def inner: SemanticCheck

  override def semanticCheck: SemanticCheck = {
    inner chain alias.map(_.declare(CTGraphRef): SemanticCheck).getOrElse(SemanticCheckResult.success)
  }
}

final case class AliasGraph(ref: GraphRef, as: Option[Variable])(val position: InputPosition) extends GraphDef {
  override def alias = as
  override protected def inner = ref.semanticCheck
}

final case class NewGraph(alias: Option[Variable], url: Option[GraphUrl])(val position: InputPosition) extends GraphDef {
  override protected def inner = url.semanticCheck
}

final case class CopyGraph(ref: GraphRef, to: GraphUrl, as: Option[Variable])(val position: InputPosition)
  extends GraphDef {
  override def alias = as
  override protected def inner = ref.semanticCheck chain to.semanticCheck
}

final case class LoadGraph(alias: Option[Variable], url: GraphUrl)(val position: InputPosition) extends GraphDef {
  override protected def inner = url.semanticCheck
}
