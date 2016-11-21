/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_0.ast

import org.neo4j.cypher.internal.frontend.v3_0.SemanticCheckResult._
import org.neo4j.cypher.internal.frontend.v3_0._
import org.neo4j.cypher.internal.frontend.v3_0.ast.Expression.SemanticContext
import org.neo4j.cypher.internal.frontend.v3_0.helpers.StringHelper.RichString
import org.neo4j.cypher.internal.frontend.v3_0.notification.CartesianProductNotification
import org.neo4j.cypher.internal.frontend.v3_0.symbols._

sealed trait Clause extends ASTNode with ASTPhrase with SemanticCheckable {
  def name: String

  def noteCurrentScope: SemanticCheck = s => SemanticCheckResult.success(s.noteCurrentScope(this))

  def returnColumns: List[String] =
    throw new InternalException("This clause is not allowed as a last clause and hence does not declare return columns")
}

sealed trait UpdateClause extends Clause {
  override def returnColumns = List.empty
}

case class LoadCSV(withHeaders: Boolean, urlString: Expression, variable: Variable, fieldTerminator: Option[StringLiteral])(val position: InputPosition) extends Clause with SemanticChecking {
  override def name = "LOAD CSV"

  override def semanticCheck: SemanticCheck =
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
      CTList(CTString)

    variable.declare(typ)
  }
}

case class Start(items: Seq[StartItem], where: Option[Where])(val position: InputPosition) extends Clause {
  override def name = "START"

  override def semanticCheck = items.semanticCheck chain where.semanticCheck
}

case class Match(optional: Boolean, pattern: Pattern, hints: Seq[UsingHint], where: Option[Where])(val position: InputPosition) extends Clause with SemanticChecking {
  override def name = "MATCH"

  override def semanticCheck =
    pattern.semanticCheck(Pattern.SemanticContext.Match) chain
      hints.semanticCheck chain
      uniqueHints chain
      where.semanticCheck chain
      checkHints chain
      checkForCartesianProducts chain
      noteCurrentScope

  private def uniqueHints: SemanticCheck = {
    val errors = hints.groupBy(_.variables.toSeq).collect {
      case pair@(variables, identHints) if identHints.size > 1 =>
        SemanticError("Multiple hints for same variable are not supported", variables.head.position, identHints.map(_.position): _*)
    }.toVector

    (state: SemanticState) => SemanticCheckResult(state, errors)
  }

  private def checkForCartesianProducts: SemanticCheck = (state: SemanticState) => {
    import connectedComponents._
    val cc = connectedComponents(pattern.patternParts)
    //if we have multiple connected components we will have
    //a cartesian product
    val newState = cc.drop(1).foldLeft(state) { (innerState, component) =>
      innerState.addNotification(CartesianProductNotification(position, component.variables.map(_.name)))
    }

    SemanticCheckResult(newState, Seq.empty)
  }

  private def checkHints: SemanticCheck = {
    val error: Option[SemanticCheck] = hints.collectFirst {
      case hint@UsingIndexHint(Variable(variable), LabelName(labelName), PropertyKeyName(property))
        if !containsLabelPredicate(variable, labelName)
          || !containsPropertyPredicate(variable, property) =>
        SemanticError(
          """|Cannot use index hint in this context.
            | Index hints are only supported for the following predicates in WHERE
            | (either directly or as part of a top-level AND):
            | equality comparison, inequality (range) comparison, STARTS WITH,
            | IN condition or checking property existence.
            | The comparison cannot be performed between two property values.
            | Note that the label and property comparison must be specified on a
            | non-optional node""".stripLinesAndMargins, hint.position)
      case hint@UsingScanHint(Variable(variable), LabelName(labelName))
        if !containsLabelPredicate(variable, labelName) =>
        SemanticError(
          """|Cannot use label scan hint in this context.
            | Label scan hints require using a simple label test in WHERE (either directly or as part of a
            | top-level AND). Note that the label must be specified on a non-optional node""".stripLinesAndMargins, hint.position)
      case hint@UsingJoinHint(_)
        if pattern.length == 0 =>
        SemanticError("Cannot use join hint for single node pattern.", hint.position)
    }
    error.getOrElse(SemanticCheckResult.success)
  }

  private def containsPropertyPredicate(variable: String, property: String): Boolean = {
    val properties: Seq[String] = (where match {
      case Some(w) => w.treeFold(Seq.empty[String]) {
        case Equals(Property(Variable(id), PropertyKeyName(name)), other) if id == variable && applicable(other) =>
          acc => (acc :+ name, None)
        case Equals(other, Property(Variable(id), PropertyKeyName(name))) if id == variable && applicable(other) =>
          acc => (acc :+ name, None)
        case In(Property(Variable(id), PropertyKeyName(name)),_) if id == variable =>
          acc => (acc :+ name, None)
        case predicate@FunctionInvocation(_, _, IndexedSeq(Property(Variable(id), PropertyKeyName(name))))
          if id == variable && predicate.function.contains(functions.Exists) =>
          acc => (acc :+ name, None)
        case IsNotNull(Property(Variable(id), PropertyKeyName(name))) if id == variable =>
          acc => (acc :+ name, None)
        case StartsWith(Property(Variable(id), PropertyKeyName(name)), _) if id == variable =>
          acc => (acc :+ name, None)
        case EndsWith(Property(Variable(id), PropertyKeyName(name)), _) if id == variable =>
          acc => (acc :+ name, None)
        case Contains(Property(Variable(id), PropertyKeyName(name)), _) if id == variable =>
          acc => (acc :+ name, None)
        case expr: InequalityExpression =>
          acc =>
            val newAcc: Seq[String] = Seq(expr.lhs, expr.rhs).foldLeft(acc) { (acc, expr) =>
              expr match {
                case Property(Variable(id), PropertyKeyName(name)) if id == variable => acc :+ name
                case _ => acc
              }
            }
            (newAcc, None)
        case _: Where | _: And | _: Ands | _: Set[_] =>
          acc => (acc, Some(identity))
        case _ =>
          acc => (acc, None)
      }
      case None => Seq.empty
    }) ++ pattern.treeFold(Seq.empty[String]) {
      case NodePattern(Some(Variable(id)), _, Some(MapExpression(prop))) if variable == id =>
        acc => (acc ++ prop.map(_._1.name), None)
    }
    properties.contains(property)
  }

  /*
   * Checks validity of the other side, X, of expressions such as
   *  USING INDEX ON n:Label(prop) WHERE n.prop = X (or X = n.prop)
   *
   * Returns true if X is a valid expression in this context, otherwise false.
   */
  private def applicable(other: Expression) = {
    other match {
      case _: Property => false
      case f: FunctionInvocation => !f.function.contains(functions.Id)
      case _ => true
    }
  }

  private def containsLabelPredicate(variable: String, label: String): Boolean = {
    var labels = pattern.fold(Seq.empty[String]) {
      case NodePattern(Some(Variable(id)), nodeLabels, _) if variable == id =>
        list => list ++ nodeLabels.map(_.name)
    }
    labels = where match {
      case Some(innerWhere) => innerWhere.treeFold(labels) {
        case HasLabels(Variable(id), predicateLabels) if id == variable =>
          acc => (acc ++ predicateLabels.map(_.name), None)
        case _: Where | _: And | _: Ands | _: Set[_] =>
          acc => (acc, Some(identity))
        case _ =>
          acc => (acc, None)
      }
      case None => labels
    }
    labels.contains(label)
  }
}

case class Merge(pattern: Pattern, actions: Seq[MergeAction])(val position: InputPosition) extends UpdateClause {
  override def name = "MERGE"

  override def semanticCheck =
    pattern.semanticCheck(Pattern.SemanticContext.Merge) chain
      actions.semanticCheck chain checkRelTypes

  // Copied code from CREATE below
  private def checkRelTypes: SemanticCheck  =
    pattern.patternParts.foldSemanticCheck {
      case EveryPath(RelationshipChain(_, rel, _)) if rel.types.size != 1 =>
        SemanticError("A single relationship type must be specified for MERGE", rel.position)
      case _ => SemanticCheckResult.success
    }
}

case class Create(pattern: Pattern)(val position: InputPosition) extends UpdateClause {
  override def name = "CREATE"

  override def semanticCheck = pattern.semanticCheck(Pattern.SemanticContext.Create) chain checkRelTypes

  //CREATE only support CREATE ()-[:T]->(), thus one-and-only-one type
  private def checkRelTypes: SemanticCheck  =
    pattern.patternParts.foldSemanticCheck {
      case EveryPath(RelationshipChain(_, rel, _)) if rel.types.size != 1 =>
        SemanticError("A single relationship type must be specified for CREATE", rel.position)
      case _ => SemanticCheckResult.success
    }
}

case class CreateUnique(pattern: Pattern)(val position: InputPosition) extends UpdateClause {
  override def name = "CREATE UNIQUE"

  override def semanticCheck = pattern.semanticCheck(Pattern.SemanticContext.CreateUnique)
}

case class SetClause(items: Seq[SetItem])(val position: InputPosition) extends UpdateClause {
  override def name = "SET"

  override def semanticCheck = items.semanticCheck
}

case class Delete(expressions: Seq[Expression], forced: Boolean)(val position: InputPosition) extends UpdateClause {
  override def name = "DELETE"

  override def semanticCheck =
    expressions.semanticCheck(Expression.SemanticContext.Simple) chain
      warnAboutDeletingLabels chain
      expressions.expectType(CTNode.covariant | CTRelationship.covariant | CTPath.covariant)

  private def warnAboutDeletingLabels =
    expressions.filter(_.isInstanceOf[HasLabels]) map {
      e => SemanticError("DELETE doesn't support removing labels from a node. Try REMOVE.", e.position)
    }
}

case class Remove(items: Seq[RemoveItem])(val position: InputPosition) extends UpdateClause {
  override def name = "REMOVE"

  override def semanticCheck = items.semanticCheck
}

case class Foreach(variable: Variable, expression: Expression, updates: Seq[Clause])(val position: InputPosition) extends UpdateClause with SemanticChecking {
  override def name = "FOREACH"

  override def semanticCheck =
    expression.semanticCheck(Expression.SemanticContext.Simple) chain
      expression.expectType(CTList(CTAny).covariant) chain
      updates.filter(!_.isInstanceOf[UpdateClause]).map(c => SemanticError(s"Invalid use of ${c.name} inside FOREACH", c.position)) ifOkChain
      withScopedState {
        val possibleInnerTypes: TypeGenerator = expression.types(_).unwrapLists
        variable.declare(possibleInnerTypes) chain updates.semanticCheck
      }
}

case class Unwind(expression: Expression, variable: Variable)(val position: InputPosition) extends Clause {
  override def name = "UNWIND"

  override def semanticCheck =
    expression.semanticCheck(Expression.SemanticContext.Results) chain
      expression.expectType(CTList(CTAny).covariant) ifOkChain {
      val possibleInnerTypes: TypeGenerator = expression.types(_).unwrapLists
      variable.declare(possibleInnerTypes)
    }
}

abstract class CallClause extends Clause {
  override def name = "CALL"
  def returnColumns: List[String]
  def containsNoUpdates: Boolean
}

case class UnresolvedCall(procedureNamespace: ProcedureNamespace,
                          procedureName: ProcedureName,
                          // None: No arguments given (i.e. no "YIELD" part)
                          declaredArguments: Option[Seq[Expression]] = None,
                          // None: No results declared
                          declaredResults: Option[Seq[ProcedureResultItem]] = None
                         )(val position: InputPosition) extends CallClause {

  override def returnColumns =
    declaredResults.map(_.map(_.variable.name).toList).getOrElse(List.empty)

  override def semanticCheck: SemanticCheck = {
    val argumentCheck = declaredArguments.map(_.semanticCheck(SemanticContext.Results)).getOrElse(success)
    val resultsCheck = declaredResults.map(_.foldSemanticCheck(_.semanticCheck)).getOrElse(success)
    val invalidExpressionsCheck = declaredArguments.map(_.map {
      case arg if arg.containsAggregate =>
        error(_: SemanticState,
              SemanticError(
                """Procedure call cannot take an aggregating function as argument, please add a 'WITH' to your statement.
                  |For example:
                  |    MATCH (n:Person) WITH collect(n.name) AS names CALL proc(names) YIELD value RETURN value""".stripMargin, position))
      case _ => success
    }.foldLeft(success)(_ chain _)).getOrElse(success)

    argumentCheck chain resultsCheck chain invalidExpressionsCheck
  }

  //At this stage we are not sure whether or not the procedure
  // contains updates, so let's err on the side of caution
  override def containsNoUpdates = false
}

sealed trait HorizonClause extends Clause with SemanticChecking {
  override def semanticCheck = noteCurrentScope
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

  override def semanticCheckContinuation(previousScope: Scope): SemanticCheck =  (s: SemanticState) => {
    val specialReturnItems = createSpecialReturnItems(previousScope, s)
    val specialStateForShuffle = specialReturnItems.declareVariables(previousScope)(s).state
    val shuffleResult = (orderBy.semanticCheck chain checkSkip chain checkLimit)(specialStateForShuffle)
    val shuffleErrors = shuffleResult.errors

    // We still need to declare the return items, and register the use of variables in the ORDER BY clause. But we
    // don't want to see errors from ORDER BY - we'll get them through shuffleErrors instead
    val orderByResult = (returnItems.declareVariables(previousScope) chain ignoreErrors(orderBy.semanticCheck))(s)
    val fixedOrderByResult =
      if (specialReturnItems.includeExisting) {
        val shuffleScope = shuffleResult.state.currentScope.scope
        val definedHere = specialReturnItems.items.map(_.name).toSet
        orderByResult.copy(orderByResult.state.mergeScope(shuffleScope, definedHere))
      } else
        orderByResult

    SemanticCheckResult(fixedOrderByResult.state, fixedOrderByResult.errors ++ shuffleErrors)
  }

  private def createSpecialReturnItems(previousScope: Scope, s: SemanticState): ReturnItems = {
    // ORDER BY lives in this special scope that has access to things in scope before the RETURN/WITH clause,
    // but also to the variables introduced by RETURN/WITH. This is most easily done by turning
    // RETURN a, b, c => RETURN *, a, b, c

    // Except when we are doing DISTINCT or aggregation, in which case we only see the scope introduced by the
    // projecting clause
    val includePreviousScope = !(returnItems.containsAggregate || distinct)
    val specialReturnItems = returnItems.copy(includeExisting = includePreviousScope)(returnItems.position)
    specialReturnItems
  }

  // use an empty state when checking skip & limit, as these have entirely isolated context
  private def checkSkip: SemanticState => Seq[SemanticError] =
    s => skip.semanticCheck(SemanticState.clean).errors

  private def checkLimit: SemanticState => Seq[SemanticError] =
    s => limit.semanticCheck(SemanticState.clean).errors

  private def ignoreErrors(inner: SemanticCheck): SemanticCheck =
    s => SemanticCheckResult.success(inner.apply(s).state)

  def verifyOrderByAggregationUse(fail: (String, InputPosition) => Nothing): Unit = {
    val aggregationInProjection = returnItems.items.map(_.expression).exists(containsAggregate)
    val aggregationInOrderBy = orderBy.exists(_.sortItems.map(_.expression).exists(containsAggregate))
    if (!aggregationInProjection && aggregationInOrderBy)
      fail(s"Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding $name", position)
  }
}

case class With(
                 distinct: Boolean,
                 returnItems: ReturnItems,
                 orderBy: Option[OrderBy],
                 skip: Option[Skip],
                 limit: Option[Limit],
                 where: Option[Where])(val position: InputPosition) extends ProjectionClause {

  override def name = "WITH"

  override def semanticCheck =
    super.semanticCheck chain
      checkAliasedReturnItems

  override def semanticCheckContinuation(previousScope: Scope) =
    super.semanticCheckContinuation(previousScope) chain
      where.semanticCheck

  private def checkAliasedReturnItems: SemanticState => Seq[SemanticError] = state => returnItems match {
    case li: ReturnItems => li.items.filter(_.alias.isEmpty).map(i => SemanticError("Expression in WITH must be aliased (use AS)", i.position))
    case _                     => Seq()
  }
}

case class Return(distinct: Boolean,
                  returnItems: ReturnItems,
                  orderBy: Option[OrderBy],
                  skip: Option[Skip],
                  limit: Option[Limit],
                  excludedNames: Set[String] = Set.empty)(val position: InputPosition) extends ProjectionClause {

  override def name = "RETURN"

  override def returnColumns = returnItems.items.map(_.name).toList

  override def semanticCheck = super.semanticCheck chain checkVariableScope

  private def checkVariableScope: SemanticState => Seq[SemanticError] = s =>
    if (returnItems.includeExisting && s.currentScope.isEmpty)
      Seq(SemanticError("RETURN * is not allowed when there are no variables in scope", position))
    else
      Seq()
}

case class PragmaWithout(excluded: Seq[Variable])(val position: InputPosition) extends HorizonClause {
  override def name = "_PRAGMA WITHOUT"
  val excludedNames = excluded.map(_.name).toSet

  override def semanticCheckContinuation(previousScope: Scope): SemanticCheck = s =>
    SemanticCheckResult.success(s.importScope(previousScope, excludedNames))
}
