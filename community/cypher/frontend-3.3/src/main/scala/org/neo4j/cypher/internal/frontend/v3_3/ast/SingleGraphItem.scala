package org.neo4j.cypher.internal.frontend.v3_3.ast

import org.neo4j.cypher.internal.frontend.v3_3.{InputPosition, SemanticCheck, SemanticCheckResult, SemanticCheckable, SemanticChecking}

sealed trait SingleGraphItem extends ASTNode with ASTParticle with SemanticCheckable with SemanticChecking {

  def as: Option[Variable]

  def withNewName(newName: Variable): SingleGraphItem

  override def semanticCheck: SemanticCheck = {
    inner chain as.map(_.declareGraph: SemanticCheck).getOrElse(SemanticCheckResult.success)
  }

  protected def inner: SemanticCheck
}

final case class GraphOfItem(of: Pattern, as: Option[Variable])(val position: InputPosition) extends SingleGraphItem {
  override def withNewName(newName: Variable) = copy(as = Some(newName))(position)
  override protected def inner: SemanticCheck = of.semanticCheck(Pattern.SemanticContext.Create)
}

final case class GraphAtItem(at: GraphUrl, as: Option[Variable])(val position: InputPosition) extends SingleGraphItem {
  override def withNewName(newName: Variable) = copy(as = Some(newName))(position)
  override protected def inner: SemanticCheck = at.semanticCheck
}

final case class SourceGraphItem(as: Option[Variable])(val position: InputPosition) extends SingleGraphItem {
  override def withNewName(newName: Variable) = copy(as = Some(newName))(position)
  override protected def inner: SemanticCheck = SemanticCheckResult.success
}

final case class TargetGraphItem(as: Option[Variable])(val position: InputPosition) extends SingleGraphItem {
  override def withNewName(newName: Variable) = copy(as = Some(newName))(position)
  override protected def inner: SemanticCheck = SemanticCheckResult.success
}

final case class GraphRefAliasItem(alias: GraphRefAlias)(val position: InputPosition) extends SingleGraphItem {
  def ref = alias.ref
  override def as = alias.as

  override def withNewName(newName: Variable) = copy(alias = alias.withNewName(newName))(position)
  override def semanticCheck: SemanticCheck = alias.semanticCheck
  override protected def inner: SemanticCheck = SemanticCheckResult.success
}

