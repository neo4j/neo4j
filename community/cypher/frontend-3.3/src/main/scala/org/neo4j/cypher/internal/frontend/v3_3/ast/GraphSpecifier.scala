package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.symbols.CTGraphRef
import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, SemanticCheck, SemanticCheckResult, SemanticCheckable, SemanticChecking}

sealed trait GraphSpecifier extends ASTNode with ASTParticle with SemanticCheckable with SemanticChecking {
  def graph: Option[GraphDef]
}

final case class NoGraph()(val position: InputPosition) extends GraphSpecifier {
  override def semanticCheck: SemanticCheck = SemanticCheckResult.success
  override def graph = None
}

sealed trait GraphDef extends GraphSpecifier {

  self =>

  def name: Variable = as.get
  def as: Option[Variable]

  def withNewName(newName: Variable): GraphDef

  override def semanticCheck: SemanticCheck = {
    inner chain as.map(_.declare(CTGraphRef): SemanticCheck).getOrElse(SemanticCheckResult.success)
  }

  final override def graph = Some(self)

  protected def inner: SemanticCheck
}

final case class AliasGraph(ref: GraphRef, as: Option[Variable])
                           (val position: InputPosition) extends GraphDef {
  override protected def inner = ref.semanticCheck

  override def withNewName(newName: Variable) = copy(as = Some(newName))(position)
}

final case class NewGraph(url: Option[GraphUrl], as: Option[Variable])
                         (val position: InputPosition) extends GraphDef {
  override protected def inner = url.semanticCheck

  override def withNewName(newName: Variable) = copy(as = Some(newName))(position)
}

final case class CopyGraph(ref: GraphRef, to: GraphUrl, as: Option[Variable])
                          (val position: InputPosition)
  extends GraphDef {

  override protected def inner = ref.semanticCheck chain to.semanticCheck

  override def withNewName(newName: Variable) = copy(as = Some(newName))(position)
}

final case class LoadGraph(url: GraphUrl, as: Option[Variable])
                          (val position: InputPosition) extends GraphDef {

  override protected def inner = url.semanticCheck
  override def withNewName(newName: Variable) = copy(as = Some(newName))(position)
}
