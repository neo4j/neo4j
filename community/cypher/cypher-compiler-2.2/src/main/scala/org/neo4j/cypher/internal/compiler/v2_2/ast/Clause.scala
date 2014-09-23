/**
 * Copyright (c) 2002-2014 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.compiler.v2_2.ast

import org.neo4j.cypher.internal.compiler.v2_2._
import org.neo4j.cypher.internal.compiler.v2_2.commands.expressions.StringHelper.RichString
import org.neo4j.cypher.internal.compiler.v2_2.symbols._

sealed trait Clause extends ASTNode with ASTPhrase with SemanticCheckable {
  def name: String
}

sealed trait UpdateClause extends Clause

case class LoadCSV(withHeaders: Boolean, urlString: Expression, identifier: Identifier, fieldTerminator: Option[StringLiteral])(val position: InputPosition) extends Clause with SemanticChecking {
  val name = "LOAD CSV"

  def semanticCheck: SemanticCheck =
    urlString.semanticCheck(Expression.SemanticContext.Simple) chain
    urlString.expectType(CTString.covariant) chain
    checkFieldTerminator chain
    typeCheck

  private def checkFieldTerminator: SemanticCheck = {
    fieldTerminator match {
      case Some(literal) if literal.value.length != 1 =>
        SemanticError("CSV field terminator can only be one character wide", literal.position)
      case _ => SemanticCheckResult.success
    }
  }

  private def typeCheck: SemanticCheck = {
    val typ = if (withHeaders)
      CTMap
    else
      CTCollection(CTString)

    identifier.declare(typ)
  }
}

case class Start(items: Seq[StartItem], where: Option[Where])(val position: InputPosition) extends Clause {
  val name = "START"

  def semanticCheck = items.semanticCheck chain where.semanticCheck
}

case class Match(optional: Boolean, pattern: Pattern, hints: Seq[RonjaHint], where: Option[Where])(val position: InputPosition) extends Clause with SemanticChecking {
  def name = "MATCH"

  def semanticCheck =
    pattern.semanticCheck(Pattern.SemanticContext.Match) chain
    hints.semanticCheck chain
    uniqueHints chain
    where.semanticCheck chain
    checkHints

  def uniqueHints: SemanticCheck = {
    val errors = hints.groupBy(_.identifier).collect {
      case pair@(ident, identHints) if identHints.size > 1 =>
        SemanticError("Multiple hints for same identifier are not supported", ident.position, identHints.map(_.position): _*)
    }.toVector

    (state: SemanticState) => SemanticCheckResult(state, errors)
  }

  def checkHints: SemanticCheck = {
    val error: Option[SemanticCheck] = hints.collectFirst {
      case hint@UsingIndexHint(Identifier(identifier), LabelName(labelName), Identifier(property))
        if !containsLabelPredicate(identifier, labelName)
          || !containsPropertyPredicate(identifier, property) =>
        SemanticError(
          """|Cannot use index hint in this context.
             | Index hints require using a simple equality comparison or IN condition in WHERE (either directly or as part of a
             | top-level AND).
             | Note that the label and property comparison must be specified on a
             | non-optional node""".stripLinesAndMargins, hint.position)
      case hint@UsingScanHint(Identifier(identifier), LabelName(labelName))
        if !containsLabelPredicate(identifier, labelName) =>
        SemanticError(
          """|Cannot use label scan hint in this context.
             | Label scan hints require using a simple label test in WHERE (either directly or as part of a
             | top-level AND). Note that the label must be specified on a non-optional node""".stripLinesAndMargins, hint.position)
    }
    error.getOrElse(SemanticCheckResult.success)
  }

  def containsPropertyPredicate(identifier: String, property: String): Boolean = {
    val properties: Seq[String] = (where match {
      case Some(where) => where.treeFold(Seq.empty[String]) {
        case Equals(Property(Identifier(id), PropertyKeyName(name)), _) if id == identifier =>
          (acc, _) => acc :+ name
        case Equals(_, Property(Identifier(id), PropertyKeyName(name))) if id == identifier =>
          (acc, _) => acc :+ name
        case In(Property(Identifier(id), PropertyKeyName(name)),_) if id == identifier =>
          (acc, _) => acc :+ name
        case _: Where | _: And | _: Ands | _: Set[_] =>
          (acc, children) => children(acc)
        case _ =>
          (acc, _) => acc
      }
      case None => Seq.empty
    }) ++ pattern.treeFold(Seq.empty[String]) {
      case NodePattern(Some(Identifier(id)), _, Some(MapExpression(properties)), _) if identifier == id => {
        case (acc, _) =>
          acc ++ properties.map(_._1.name)
      }
    }
    properties.contains(property)
  }

  def containsLabelPredicate(identifier: String, label: String): Boolean = {
    var labels = pattern.fold(Seq.empty[String]) {
      case NodePattern(Some(Identifier(id)), labels, _, _) if identifier == id =>
        list => list ++ labels.map(_.name)
    }
    labels = where match {
      case Some(where) => where.treeFold(labels) {
        case HasLabels(Identifier(id), labels) if id == identifier =>
          (acc, _) => acc ++ labels.map(_.name)
        case _: Where | _: And | _: Ands | _: Set[_] =>
          (acc, children) => children(acc)
        case _ =>
          (acc, _) => acc
      }
      case None => labels
    }
    labels.contains(label)
  }
}

case class Merge(pattern: Pattern, actions: Seq[MergeAction])(val position: InputPosition) extends UpdateClause {
  def name = "MERGE"

  def semanticCheck =
    pattern.semanticCheck(Pattern.SemanticContext.Merge) chain
    actions.semanticCheck
}

case class Create(pattern: Pattern)(val position: InputPosition) extends UpdateClause {
  def name = "CREATE"

  def semanticCheck = pattern.semanticCheck(Pattern.SemanticContext.Create)
}

case class CreateUnique(pattern: Pattern)(val position: InputPosition) extends UpdateClause {
  def name = "CREATE UNIQUE"

  def semanticCheck = pattern.semanticCheck(Pattern.SemanticContext.CreateUnique)
}

case class SetClause(items: Seq[SetItem])(val position: InputPosition) extends UpdateClause {
  def name = "SET"

  def semanticCheck = items.semanticCheck
}

case class Delete(expressions: Seq[Expression])(val position: InputPosition) extends UpdateClause {
  def name = "DELETE"

  def semanticCheck =
    expressions.semanticCheck(Expression.SemanticContext.Simple) chain
    warnAboutDeletingLabels chain
    expressions.expectType(CTNode.covariant | CTRelationship.covariant | CTPath.covariant)

  def warnAboutDeletingLabels =
    expressions.filter(_.isInstanceOf[HasLabels]) map {
      e => SemanticError("DELETE doesn't support removing labels from a node. Try REMOVE.", e.position)
    }
}

case class Remove(items: Seq[RemoveItem])(val position: InputPosition) extends UpdateClause {
  def name = "REMOVE"

  def semanticCheck = items.semanticCheck
}

case class Foreach(identifier: Identifier, expression: Expression, updates: Seq[Clause])(val position: InputPosition) extends UpdateClause with SemanticChecking {
  def name = "FOREACH"

  def semanticCheck =
    expression.semanticCheck(Expression.SemanticContext.Simple) chain
    expression.expectType(CTCollection(CTAny).covariant) chain
    updates.filter(!_.isInstanceOf[UpdateClause]).map(c => SemanticError(s"Invalid use of ${c.name} inside FOREACH", c.position)) ifOkChain
    withScopedState {
      val possibleInnerTypes: TypeGenerator = expression.types(_).unwrapCollections
      identifier.declare(possibleInnerTypes) chain updates.semanticCheck
    }
}

case class Unwind(expression: Expression, identifier: Identifier)(val position: InputPosition) extends Clause {
  def name = "UNWIND"

  override def semanticCheck =
    expression.semanticCheck(Expression.SemanticContext.Results) chain
      expression.expectType(CTCollection(CTAny).covariant) ifOkChain {
      val possibleInnerTypes: TypeGenerator = expression.types(_).unwrapCollections
      identifier.declare(possibleInnerTypes)
    }
}

sealed trait HorizonClause extends Clause with SemanticChecking {
  def semanticCheck = s =>
    SemanticCheckResult.success(s.noteCurrentScope(this))

  def semanticCheckContinuation(previousScope: Scope): SemanticCheck
}

sealed trait ProjectionClause extends HorizonClause with SemanticChecking {
  def distinct: Boolean
  def returnItems: ReturnItems
  def orderBy: Option[OrderBy]
  def skip: Option[Skip]
  def limit: Option[Limit]

  override def semanticCheck =
    super.semanticCheck chain
    returnItems.semanticCheck

  def semanticCheckContinuation(previousScope: Scope): SemanticCheck =
    returnItems.declareIdentifiers(previousScope) chain
      orderBy.semanticCheck chain
      checkSkip chain
      checkLimit

  // use an empty state when checking skip & limit, as these have entirely isolated context
  protected def checkSkip: SemanticState => Seq[SemanticError] =
    s => skip.semanticCheck(SemanticState.clean).errors
  protected def checkLimit: SemanticState => Seq[SemanticError] =
    s => limit.semanticCheck(SemanticState.clean).errors
}

case class With(
    distinct: Boolean,
    returnItems: ReturnItems,
    orderBy: Option[OrderBy],
    skip: Option[Skip],
    limit: Option[Limit],
    where: Option[Where])(val position: InputPosition) extends ProjectionClause {
  def name = "WITH"

  override def semanticCheck =
    super.semanticCheck chain
    checkAliasedReturnItems

  override def semanticCheckContinuation(previousScope: Scope) =
    super.semanticCheckContinuation(previousScope) chain
    where.semanticCheck

  private def checkAliasedReturnItems: SemanticState => Seq[SemanticError] = state => returnItems match {
    case li: ReturnItems => li.items.filter(!_.alias.isDefined).map(i => SemanticError("Expression in WITH must be aliased (use AS)", i.position))
    case _                     => Seq()
  }
}

case class Return(
    distinct: Boolean,
    returnItems: ReturnItems,
    orderBy: Option[OrderBy],
    skip: Option[Skip],
    limit: Option[Limit])(val position: InputPosition) extends ProjectionClause {

  def name = "RETURN"

  override def semanticCheck = super.semanticCheck chain checkIdentifiersInScope

  override def semanticCheckContinuation(previousScope: Scope): SemanticCheck =
    checkSkip chain checkLimit

  protected def checkIdentifiersInScope: SemanticState => Seq[SemanticError] = s =>
    if (returnItems.includeExisting && s.currentScope.isEmpty)
      Seq(SemanticError("RETURN * is not allowed when there are no identifiers in scope", position))
    else
      Seq()
}

case class PragmaWithout(excluded: Seq[Identifier])(val position: InputPosition) extends HorizonClause {
  def name = "_PRAGMA WITHOUT"
  val excludedNames = excluded.map(_.name).toSet

  def semanticCheckContinuation(previousScope: Scope): SemanticCheck = s =>
    SemanticCheckResult.success(s.importScope(previousScope, excludedNames))
}
