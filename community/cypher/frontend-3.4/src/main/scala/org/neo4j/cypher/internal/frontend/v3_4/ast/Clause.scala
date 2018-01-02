/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.v3_4.ast

import org.neo4j.cypher.internal.util.v3_4.Foldable._
import org.neo4j.cypher.internal.util.v3_4.symbols._
import org.neo4j.cypher.internal.util.v3_4.{ASTNode, InputPosition, InternalException}
import org.neo4j.cypher.internal.frontend.v3_4._
import org.neo4j.cypher.internal.frontend.v3_4.helpers.StringHelper.RichString
import org.neo4j.cypher.internal.frontend.v3_4.notification.{CartesianProductNotification, DeprecatedStartNotification}
import org.neo4j.cypher.internal.frontend.v3_4.semantics.SemanticCheckResult._
import org.neo4j.cypher.internal.frontend.v3_4.semantics._
import org.neo4j.cypher.internal.v3_4.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.v3_4.expressions._
import org.neo4j.cypher.internal.v3_4.functions

sealed trait Clause extends ASTNode with SemanticCheckable {
  def name: String

  def returnColumns: List[String] =
    throw new InternalException("This clause is not allowed as a last clause and hence does not declare return columns")
}

sealed trait UpdateClause extends Clause with SemanticAnalysisTooling {
  override def returnColumns: List[String] = List.empty
}

case class LoadCSV(
                    withHeaders: Boolean,
                    urlString: Expression,
                    variable: Variable,
                    fieldTerminator: Option[StringLiteral]
                  )(val position: InputPosition) extends Clause with SemanticAnalysisTooling {
  override def name: String = "LOAD CSV"

  override def semanticCheck: SemanticCheck =
    SemanticExpressionCheck.simple(urlString) chain
      expectType(CTString.covariant, urlString) chain
      checkFieldTerminator chain
      typeCheck

  private def checkFieldTerminator: SemanticCheck = {
    fieldTerminator match {
      case Some(literal) if literal.value.length != 1 =>
        error("CSV field terminator can only be one character wide", literal.position)
      case _ => SemanticCheckResult.success
    }
  }

  private def typeCheck: SemanticCheck = {
    val typ = if (withHeaders)
      CTMap
    else
      CTList(CTString)

    declareVariable(variable, typ)
  }
}

sealed trait MultipleGraphClause extends Clause with SemanticAnalysisTooling {

  override def semanticCheck: SemanticCheck =
    requireMultigraphSupport(s"The `$name` clause", position)
}

sealed trait CreateGraphClause extends MultipleGraphClause with UpdateClause {
  def snapshot: Boolean
  def graph: Variable
  def at: GraphUrl
  def of: Option[Pattern]

  override def name = "CREATE GRAPH"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
    declareGraph(graph) chain
    of.fold(SemanticCheckResult.success)(SemanticPatternCheck.check(Pattern.SemanticContext.Create, _))
}

final case class CreateRegularGraph(snapshot: Boolean, graph: Variable, of: Option[Pattern], at: GraphUrl)(val position: InputPosition)
  extends CreateGraphClause {

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
    SemanticState.recordCurrentScope(this)
}

final case class CreateNewSourceGraph(snapshot: Boolean, graph: Variable, of: Option[Pattern], at: GraphUrl)(val position: InputPosition)
  extends CreateGraphClause {

  override def semanticCheck: SemanticCheck = error("Clause not rewritten as expected (see PreparatoryRewriting)", position)

}

final case class CreateNewTargetGraph(snapshot: Boolean, graph: Variable, of: Option[Pattern], at: GraphUrl)(val position: InputPosition)
  extends CreateGraphClause {

  override def semanticCheck: SemanticCheck = error("Clause not rewritten as expected (see PreparatoryRewriting)", position)
}

final case class DeleteGraphs(graphs: Seq[Variable])(val position: InputPosition)
  extends MultipleGraphClause with UpdateClause{

  override def name = "DELETE GRAPHS"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
    SemanticExpressionCheck.semanticCheckFold(graphs)(SemanticExpressionCheck.ensureGraphDefined) chain
    SemanticState.recordCurrentScope(this)
}

final case class Persist(graph: BoundGraphAs, to: GraphUrl)(val position: InputPosition)
  extends MultipleGraphClause with UpdateClause {

  override def name = "PERSIST"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      graph.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class Snapshot(graph: BoundGraphAs, to: GraphUrl)(val position: InputPosition)
  extends MultipleGraphClause with UpdateClause {

  override def name = "SNAPSHOT"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      graph.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

final case class Relocate(graph: BoundGraphAs, to: GraphUrl)(val position: InputPosition)
  extends MultipleGraphClause with UpdateClause {

  override def name = "RELOCATE"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      graph.semanticCheck chain
      SemanticState.recordCurrentScope(this)
}

case class Start(items: Seq[StartItem], where: Option[Where])(val position: InputPosition) extends Clause {
  override def name = "START"

  override def semanticCheck: SemanticCheck = (state: SemanticState) => {

    val query = rewrittenQuery
    val newState = state.addNotification(DeprecatedStartNotification(position, query))
    SemanticCheckResult(newState, Seq(SemanticError(
      s"""START is deprecated, use: `$query` instead.
       """.stripMargin, position)))
  }

  private def rewrittenQuery: String = {
    val rewritten = items.map {
      case NodeByIdentifiedIndex(variable, index, key, expression) =>
        s"CALL db.index.explicit.seekNodes('$index', '$key', '${expression.asCanonicalStringVal}') YIELD node AS ${variable.asCanonicalStringVal}"
      case NodeByIndexQuery(variable, index, expression) =>
        s"CALL db.index.explicit.searchNodes('$index', '${expression.asCanonicalStringVal}') YIELD node AS ${variable.asCanonicalStringVal}"
      case RelationshipByIdentifiedIndex(variable, index, key, expression) =>
        s"CALL db.index.explicit.seekRelationships('$index', '$key', '${expression.asCanonicalStringVal}') YIELD relationship AS ${variable.asCanonicalStringVal}"
      case RelationshipByIndexQuery(variable, index, expression) =>
        s"CALL db.index.explicit.searchRelationships('$index', '${expression.asCanonicalStringVal}') YIELD relationship AS ${variable.asCanonicalStringVal}"
      case AllNodes(variable) => s"MATCH (${variable.asCanonicalStringVal})"
      case AllRelationships(variable) =>  s"MATCH ()-[${variable.asCanonicalStringVal}]->()"
      case NodeByIds(variable, ids) =>
        if (ids.size == 1) s"MATCH (${variable.asCanonicalStringVal}) WHERE id(${variable.asCanonicalStringVal}) = ${ids.head.asCanonicalStringVal}"
        else s"MATCH (${variable.asCanonicalStringVal}) WHERE id(${variable.asCanonicalStringVal}) IN ${ids.map(_.asCanonicalStringVal).mkString("[", ", ", "]")}"
      case RelationshipByIds(variable, ids) =>
        if (ids.size == 1) s"MATCH ()-[${variable.asCanonicalStringVal}]->() WHERE id(${variable.asCanonicalStringVal}) = ${ids.head.asCanonicalStringVal}"
        else s"MATCH ()-[${variable.asCanonicalStringVal}]->() WHERE id(${variable.asCanonicalStringVal}) IN ${ids.map(_.asCanonicalStringVal).mkString("[", ", ", "]")}"
      case NodeByParameter(variable, parameter) => s"MATCH (${variable.asCanonicalStringVal}) WHERE id(${variable.asCanonicalStringVal}) IN ${parameter.asCanonicalStringVal}"
      case RelationshipByParameter(variable, parameter) => s"MATCH ()-[${variable.asCanonicalStringVal}]->() WHERE id(${variable.asCanonicalStringVal}) IN ${parameter.asCanonicalStringVal}"
    }

    rewritten.mkString(" ")
  }

}

case class Match(
                  optional: Boolean,
                  pattern: Pattern,
                  hints: Seq[UsingHint],
                  where: Option[Where]
                )(val position: InputPosition) extends Clause with SemanticAnalysisTooling {
  override def name = "MATCH"

  override def semanticCheck: SemanticCheck =
    SemanticPatternCheck.check(Pattern.SemanticContext.Match, pattern) chain
      hints.semanticCheck chain
      uniqueHints chain
      where.semanticCheck chain
      checkHints chain
      checkForCartesianProducts chain
      SemanticState.recordCurrentScope(this)

  private def uniqueHints: SemanticCheck = {
    val errors = hints.groupBy(_.variables.toIndexedSeq).collect {
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
      case hint@UsingIndexHint(Variable(variable), LabelName(labelName), properties)
        if !containsLabelPredicate(variable, labelName) =>
        SemanticError(
          """|Cannot use index hint in this context.
            | Must use label on node that hint is referring to.""".stripLinesAndMargins, hint.position)
      case hint@UsingIndexHint(Variable(variable), LabelName(labelName), properties)
        if !containsPropertyPredicates(variable, properties) =>
        SemanticError(
          """|Cannot use index hint in this context.
            | Index hints are only supported for the following predicates in WHERE
            | (either directly or as part of a top-level AND or OR):
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

  private def containsPropertyPredicates(variable: String, propertiesInHint: Seq[PropertyKeyName]): Boolean = {
    val propertiesInPredicates: Seq[String] = (where match {
      case Some(w) => w.treeFold(Seq.empty[String]) {
        case Equals(Property(Variable(id), PropertyKeyName(name)), other) if id == variable && applicable(other) =>
          acc => (acc :+ name, None)
        case Equals(other, Property(Variable(id), PropertyKeyName(name))) if id == variable && applicable(other) =>
          acc => (acc :+ name, None)
        case In(Property(Variable(id), PropertyKeyName(name)),_) if id == variable =>
          acc => (acc :+ name, None)
        case predicate@FunctionInvocation(_, _, _, IndexedSeq(Property(Variable(id), PropertyKeyName(name))))
          if id == variable && predicate.function == functions.Exists =>
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
        case _: Where | _: And | _: Ands | _: Set[_] | _: Or | _: Ors =>
          acc => (acc, Some(identity))
        case _ =>
          acc => (acc, None)
      }
      case None => Seq.empty
    }) ++ pattern.treeFold(Seq.empty[String]) {
      case NodePattern(Some(Variable(id)), _, Some(MapExpression(prop))) if variable == id =>
        acc => (acc ++ prop.map(_._1.name), None)
    }

    propertiesInHint.forall(p => propertiesInPredicates.contains(p.name))
  }

  /*
   * Checks validity of the other side, X, of expressions such as
   *  USING INDEX ON n:Label(prop) WHERE n.prop = X (or X = n.prop)
   *
   * Returns true if X is a valid expression in this context, otherwise false.
   */
  private def applicable(other: Expression) = {
    other match {
      case f: FunctionInvocation => f.function != functions.Id
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

  override def semanticCheck: SemanticCheck =
    SemanticPatternCheck.check(Pattern.SemanticContext.Merge, pattern) chain
      actions.semanticCheck chain
      checkRelTypes

  // Copied code from CREATE below
  private def checkRelTypes: SemanticCheck  =
    pattern.patternParts.foldSemanticCheck {
      case EveryPath(RelationshipChain(_, rel, _)) if rel.types.size != 1 =>
      if (rel.types.size > 1) {
        SemanticError("A single relationship type must be specified for MERGE", rel.position)
      } else {
        SemanticError("Exactly one relationship type must be specified for MERGE. Did you forget to prefix your relationship type with a ':'?", rel.position)
      }
      case _ => SemanticCheckResult.success
    }
}

case class Create(pattern: Pattern)(val position: InputPosition) extends UpdateClause {
  override def name = "CREATE"

  override def semanticCheck: SemanticCheck =
    SemanticPatternCheck.check(Pattern.SemanticContext.Create, pattern) chain
    checkRelTypes

  //CREATE only support CREATE ()-[:T]->(), thus one-and-only-one type
  private def checkRelTypes: SemanticCheck  =
    pattern.patternParts.foldSemanticCheck {
      case EveryPath(RelationshipChain(_, rel, _)) if rel.types.size != 1 =>
      if (rel.types.size > 1) {
        SemanticError("A single relationship type must be specified for CREATE", rel.position)
      } else {
        SemanticError("Exactly one relationship type must be specified for CREATE. Did you forget to prefix your relationship type with a ':'?", rel.position)
      }
      case _ => SemanticCheckResult.success
    }
}

case class CreateUnique(pattern: Pattern)(val position: InputPosition) extends UpdateClause {
  override def name = "CREATE UNIQUE"

  override def semanticCheck =
    SemanticError("CREATE UNIQUE is no longer supported. You can achieve the same result using MERGE", position)

}

case class SetClause(items: Seq[SetItem])(val position: InputPosition) extends UpdateClause {
  override def name = "SET"

  override def semanticCheck: SemanticCheck = items.semanticCheck
}

case class Delete(expressions: Seq[Expression], forced: Boolean)(val position: InputPosition) extends UpdateClause {
  override def name = "DELETE"

  override def semanticCheck: SemanticCheck =
    SemanticExpressionCheck.simple(expressions) chain
      warnAboutDeletingLabels chain
      expectType(CTNode.covariant | CTRelationship.covariant | CTPath.covariant, expressions)

  private def warnAboutDeletingLabels =
    expressions.filter(_.isInstanceOf[HasLabels]) map {
      e => SemanticError("DELETE doesn't support removing labels from a node. Try REMOVE.", e.position)
    }
}

case class Remove(items: Seq[RemoveItem])(val position: InputPosition) extends UpdateClause {
  override def name = "REMOVE"

  override def semanticCheck: SemanticCheck = items.semanticCheck
}

case class Foreach(
                    variable: Variable,
                    expression: Expression,
                    updates: Seq[Clause]
                  )(val position: InputPosition) extends UpdateClause {
  override def name = "FOREACH"

  override def semanticCheck: SemanticCheck =
    SemanticExpressionCheck.simple(expression) chain
      expectType(CTList(CTAny).covariant, expression) chain
      updates.filter(!_.isInstanceOf[UpdateClause]).map(c => SemanticError(s"Invalid use of ${c.name} inside FOREACH", c.position)) ifOkChain
      withScopedState {
        val possibleInnerTypes: TypeGenerator = types(expression)(_).unwrapLists
        declareVariable(variable, possibleInnerTypes) chain updates.semanticCheck
      }
}

case class Unwind(
                   expression: Expression,
                   variable: Variable
                 )(val position: InputPosition) extends Clause with SemanticAnalysisTooling {
  override def name = "UNWIND"

  override def semanticCheck: SemanticCheck =
    SemanticExpressionCheck.check(SemanticContext.Results, expression) chain
      expectType(CTList(CTAny).covariant, expression) ifOkChain {
      val possibleInnerTypes: TypeGenerator = types(expression)(_).unwrapLists
      declareVariable(variable, possibleInnerTypes)
    }
}

abstract class CallClause extends Clause {
  override def name = "CALL"
  def returnColumns: List[String]
  def containsNoUpdates: Boolean
}

case class UnresolvedCall(procedureNamespace: Namespace,
                          procedureName: ProcedureName,
                          // None: No arguments given
                          declaredArguments: Option[Seq[Expression]] = None,
                          // None: No results declared  (i.e. no "YIELD" part)
                          declaredResult: Option[ProcedureResult] = None
                         )(val position: InputPosition) extends CallClause {

  override def returnColumns: List[String] =
    declaredResult.map(_.items.map(_.variable.name).toList).getOrElse(List.empty)

  override def semanticCheck: SemanticCheck = {
    val argumentCheck = declaredArguments.map(
      SemanticExpressionCheck.check(SemanticContext.Results, _)).getOrElse(success)
    val resultsCheck = declaredResult.map(_.semanticCheck).getOrElse(success)
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

sealed trait HorizonClause extends Clause with SemanticAnalysisTooling {
  override def semanticCheck: SemanticCheck = SemanticState.recordCurrentScope(this)
  def semanticCheckContinuation(previousScope: Scope): SemanticCheck
}

sealed trait ProjectionClause extends HorizonClause {
  def distinct: Boolean
  def returnItems: ReturnItemsDef
  def graphReturnItems: Option[GraphReturnItems]
  def orderBy: Option[OrderBy]
  def skip: Option[Skip]
  def limit: Option[Limit]

  final def isWith: Boolean = !isReturn
  def isReturn: Boolean = false

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
    returnItems.semanticCheck chain
    graphReturnItems.semanticCheck

  override def semanticCheckContinuation(previousScope: Scope): SemanticCheck = {
    val declareAllTheThings = (s: SemanticState) => {
      val specialReturnItems = createSpecialReturnItems(previousScope, s)
      val specialStateForShuffle = specialReturnItems.declareVariables(previousScope)(s).state
      val shuffleResult = (orderBy.semanticCheck chain checkSkip chain checkLimit) (specialStateForShuffle)
      val shuffleErrors = shuffleResult.errors

      // We still need to declare the return items, and register the use of variables in the ORDER BY clause. But we
      // don't want to see errors from ORDER BY - we'll get them through shuffleErrors instead
      val orderByResult = (returnItems.declareVariables(previousScope) chain ignoreErrors(orderBy.semanticCheck)) (s)
      val fixedOrderByResult = specialReturnItems match {
        case ReturnItems(star, items) if star =>
          val shuffleScope = shuffleResult.state.currentScope.scope
          val definedHere = items.map(_.name).toSet
          orderByResult.copy(orderByResult.state.mergeSymbolPositionsFromScope(shuffleScope, definedHere))
        case _ =>
          orderByResult
      }
      val tabularState = fixedOrderByResult.state
      val graphResult = graphReturnItems.foldSemanticCheck(_.declareGraphs(previousScope, isReturn))(tabularState)
      graphResult.copy(errors = fixedOrderByResult.errors ++ shuffleErrors ++ graphResult.errors)
    }
    val variableStar = returnItems.isStarOnly
    val graphsStar = graphReturnItems.forall(_.isGraphsStarOnly)
    declareAllTheThings
  }

  private def createSpecialReturnItems(previousScope: Scope, s: SemanticState): ReturnItemsDef = {
    // ORDER BY lives in this special scope that has access to things in scope before the RETURN/WITH clause,
    // but also to the variables introduced by RETURN/WITH. This is most easily done by turning
    // RETURN a, b, c => RETURN *, a, b, c

    // Except when we are doing DISTINCT or aggregation, in which case we only see the scope introduced by the
    // projecting clause
    val includePreviousScope = !(returnItems.containsAggregate || distinct)
    val specialReturnItems = returnItems.withExisting(includePreviousScope)
    specialReturnItems
  }

  // use an empty state when checking skip & limit, as these have entirely isolated context
  private def checkSkip: SemanticState => Seq[SemanticErrorDef] =
    s => skip.semanticCheck(SemanticState.clean).errors

  private def checkLimit: SemanticState => Seq[SemanticErrorDef] =
    s => limit.semanticCheck(SemanticState.clean).errors

  private def ignoreErrors(inner: SemanticCheck): SemanticCheck =
    s => SemanticCheckResult.success(inner.apply(s).state)

  def verifyOrderByAggregationUse(fail: (String, InputPosition) => Nothing): Unit = {
    val aggregationInProjection = returnItems.containsAggregate
    val aggregationInOrderBy = orderBy.exists(_.sortItems.map(_.expression).exists(containsAggregate))
    if (!aggregationInProjection && aggregationInOrderBy)
      fail(s"Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding $name", position)
  }
}

object From {
  def apply(graph: SingleGraphAs)(position: InputPosition): With = {
    With(ReturnItems(includeExisting = true, Seq.empty)(position), GraphReturnItems(includeExisting = true, Seq(NewContextGraphs(graph)(position)))(position))(position)
  }
}

object Into {
  def apply(graph: SingleGraphAs)(position: InputPosition): With = {
    With(ReturnItems(includeExisting = true, Seq.empty)(position), GraphReturnItems(includeExisting = true, Seq(NewTargetGraph(graph)(position)))(position))(position)
  }
}

object With {
  def apply(graphReturnItems: GraphReturnItems)(pos: InputPosition): With =
    With(distinct = false, DiscardCardinality()(pos), graphReturnItems, None, None, None, None)(pos)

  def apply(returnItems: ReturnItemsDef, graphReturnItems: GraphReturnItems)(pos: InputPosition): With =
    With(distinct = false, returnItems, graphReturnItems, None, None, None, None)(pos)
}

case class With(distinct: Boolean,
                returnItems: ReturnItemsDef,
                mandatoryGraphReturnItems: GraphReturnItems,
                orderBy: Option[OrderBy],
                skip: Option[Skip],
                limit: Option[Limit],
                where: Option[Where]
)(val position: InputPosition) extends ProjectionClause {

  override def name = "WITH"

  override def graphReturnItems = Some(mandatoryGraphReturnItems)

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      checkAliasedReturnItems

  override def semanticCheckContinuation(previousScope: Scope): SemanticCheck =
    super.semanticCheckContinuation(previousScope) chain
      where.semanticCheck

  private def checkAliasedReturnItems: SemanticState => Seq[SemanticError] = state => returnItems match {
    case li: ReturnItems => li.items.filter(_.alias.isEmpty).map(i => SemanticError("Expression in WITH must be aliased (use AS)", i.position))
    case _ => Seq()
  }
}

object Return {
  def apply(graphReturnItems: GraphReturnItems)(pos: InputPosition): Return =
    Return(distinct = false, DiscardCardinality()(pos), Some(graphReturnItems), None, None, None)(pos)

  def apply(returnItems: ReturnItemsDef, graphReturnItems: Option[GraphReturnItems])(pos: InputPosition): Return =
    Return(distinct = false, returnItems, graphReturnItems, None, None, None)(pos)
}

case class Return(distinct: Boolean,
                  returnItems: ReturnItemsDef,
                  graphReturnItems: Option[GraphReturnItems],
                  orderBy: Option[OrderBy],
                  skip: Option[Skip],
                  limit: Option[Limit],
                  excludedNames: Set[String] = Set.empty)(val position: InputPosition) extends ProjectionClause {

  override def name = "RETURN"

  override def isReturn: Boolean = true

  override def returnColumns: List[String] = returnItems.items.map(_.name).toList

  override def semanticCheck: SemanticCheck = super.semanticCheck chain checkVariableScope

  private def checkVariableScope: SemanticState => Seq[SemanticError] = s =>
    returnItems match {
      case ReturnItems(star, _) if star && s.currentScope.isEmpty =>
        Seq(SemanticError("RETURN * is not allowed when there are no variables in scope", position))
      case _ =>
        Seq.empty
    }
}

case class PragmaWithout(excluded: Seq[LogicalVariable])(val position: InputPosition) extends HorizonClause {
  override def name = "_PRAGMA WITHOUT"
  val excludedNames: Set[String] = excluded.map(_.name).toSet

  override def semanticCheckContinuation(previousScope: Scope): SemanticCheck = s =>
    SemanticCheckResult.success(s.importValuesFromScope(previousScope, excludedNames))
}
