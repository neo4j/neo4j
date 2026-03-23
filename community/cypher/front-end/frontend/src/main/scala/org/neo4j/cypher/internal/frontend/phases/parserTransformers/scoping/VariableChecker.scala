/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
package org.neo4j.cypher.internal.frontend.phases.parserTransformers.scoping

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.CommandClause
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.CreateOrInsert
import org.neo4j.cypher.internal.ast.Foreach
import org.neo4j.cypher.internal.ast.FullSubqueryExpression
import org.neo4j.cypher.internal.ast.LocalCallableDefinition
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.ast.StrictlyAdditiveProjection
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature.ScopeQueries
import org.neo4j.cypher.internal.ast.semantics.scoping.CommonContext
import org.neo4j.cypher.internal.ast.semantics.scoping.Declarations
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionScope
import org.neo4j.cypher.internal.ast.semantics.scoping.PatternScope
import org.neo4j.cypher.internal.ast.semantics.scoping.ProjectionExpressionContext
import org.neo4j.cypher.internal.ast.semantics.scoping.StatementScope
import org.neo4j.cypher.internal.ast.semantics.scoping.TableResult
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IterableExpression
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.notification.DeprecatedPropertyReferenceInCreate
import org.neo4j.cypher.internal.notification.InternalNotificationLogger
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.Foldable
import org.neo4j.cypher.internal.util.Foldable.FoldingBehavior
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildrenNewAccForSiblings
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.gqlstatus.ErrorGqlStatusObjectImplementation
import org.neo4j.gqlstatus.GqlParams.StringParam

case class VariableChecker(
  version: CypherVersion,
  checkAggregations: Boolean = false,
  logger: InternalNotificationLogger
) extends VariableCheckerUtil {

  private val redeclarationOfVariable: VariableCheck = {
    case (acc, Scope.Clause.Declaring(astNode, incoming, Declarations(constants, variables, _), children))
      if !(constants.isEmpty && variables.isEmpty) =>
      // redeclaration of constants
      val redeclarationOfConstants = astNode match {
        case _: Foreach => Seq.empty // historically, the FOREACH iteration variable is allowed to shadow
        case _ =>
          incoming.checkIfVariablesAreAlreadyDeclaredAsConstant((constants ++ variables).toSet)
      }
      // redeclaration of variables
      val redeclarationOfVariables = astNode match {
        case _: CommandClause =>
          incoming.checkIfVariablesAreAlreadyDeclaredAsVariable(variables)
        case pc: ProjectionClause if pc.returnItems.projectionType == StrictlyAdditiveProjection =>
          incoming.checkIfVariablesAreAlreadyDeclaredAsVariable(variables)
        case _: UnresolvedCall => incoming.checkIfVariablesHaveMultipleDeclarations(variables)
        case _: Unwind         => incoming.checkIfVariablesAreAlreadyDeclaredAsVariable(variables)
        case _: Search         => incoming.checkIfVariablesAreAlreadyDeclaredAsVariable(variables)
        case _: SubqueryCall =>
          incoming.checkIfVariablesAreAlreadyDeclaredAsVariable(
            variables,
            (n, p) => {
              if (
                children.head.result.getColumns.exists(_.name == n) &&
                !children.head.astNode.isInstanceOf[ConditionalQueryWhen]
              )
                SemanticError.variableAlreadyDeclaredInOuterScope(n, p) // Inner query throws 42N07
              else SemanticError.variableAlreadyDeclared(n, p) // IN TRANSACTIONS throw 42N59
            }
          )
        case _ => Seq.empty
      }
      acc(redeclarationOfConstants ++ redeclarationOfVariables)
    case (acc, Scope.Clause.Command(incoming, children)) =>
      val innerResult = children.last.result match {
        case TableResult(columns) => columns
        case _                    => Seq.empty
      }
      acc(innerResult.filter(x => incoming.allSymbols.exists(_.name == x.name)).map(v =>
        SemanticError.variableAlreadyDeclared(v.name, v.position)
      ))
  }

  private val multipleReturnColumns: VariableCheck = {
    case (acc, StatementScope(w: With, _, _, Declarations(_, variables, _), _, _, _, _)) =>
      acc(findMultipleDeclarationsIn(variables, w))
    case (acc, StatementScope(y: Yield, _, _, _, _, TableResult(columns), _, _)) =>
      acc(findMultipleDeclarationsIn(columns, y))
    case (acc, StatementScope(r: Return, _, _, _, _, TableResult(columns), _, _)) =>
      acc(findMultipleDeclarationsIn(columns, r))
  }

  private val incompatibleReturnColumns: VariableCheck = {
    case (acc, StatementScope(u: Union, _, _, _, _, result, children, _)) =>
      acc(getIncompatibleReturnColumnsForUnion(u.position, result, children))
    case (acc, StatementScope(_: ConditionalQueryWhen, _, _, _, _, result, children, _)) =>
      acc(getIncompatibleReturnColumnsForConditionalQuery(result, children))
  }

  private val invalidUseOfReturnStar: VariableCheck = {
    case (Acc.SubqueryExpr(acc), Scope.Clause.ReturnStar(incoming, position))
      if incoming.constantsAndVariables.isEmpty => acc(SemanticError.invalidUseOfReturnStar(position))
    case (Acc.Opinionated(acc, constants), Scope.Clause.ReturnItems(items, position)) =>
      acc(getAliasesShadowingConstants(items, constants, position))
    case (acc, Scope.Clause.ReturnStar(incoming, position)) if incoming.isVariablesEmpty =>
      acc(SemanticError.invalidUseOfReturnStar(position))
  }

  private val variableNotDefinedInScopeClause: VariableCheck = {
    case (acc, Scope.Clause.SubqueryCall(imports, incoming)) if imports.nonEmpty =>
      acc(imports.filter(v => !incoming.allSymbols.exists(_.name == v.name))
        .flatMap(v => Seq(SemanticError.variableNotDefined(v.name, v.position))))
  }

  private val invalidEntityReferenceInUpdatingClause: VariableCheck = {
    case ( // ≥ Cypher 25
        Acc.CreatePattern(acc, topo, _, create, true),
        StatementScope(_: Match, _, _, declared, _, _, _, _)
      )
      if version != CypherVersion.Cypher5 && (declared.withoutAnonymousDeclaration.allSymbols intersect topo).nonEmpty =>
      acc(declared.withoutAnonymousDeclaration.allSymbols.filter(topo).map(v =>
        SemanticError.invalidEntityReference(v.name, create.name, v.position)
      ).toSeq)
    case ( // Cypher 5
        Acc.CreatePattern(acc, topo, patternVars, create, true),
        StatementScope(_: Match, _, _, declared, _, _, _, _)
      )
      if version == CypherVersion.Cypher5 && (declared.withoutAnonymousDeclaration.allSymbols intersect topo).nonEmpty =>
      acc(declared.withoutAnonymousDeclaration.allSymbols.flatMap(v =>
        if (patternVars contains v)
          Seq(SemanticError.invalidEntityReference(v.name, create.name, v.position))
        else {
          logger.log(DeprecatedPropertyReferenceInCreate(v.position, v.name))
          Seq()
        }
      ).toSeq)
  }

  private val localCallableAlreadyDefined: VariableCheck = {
    case (
        acc,
        Scope.Definition.LocalCallable(name, _)
      ) =>
      val isError = acc.definedLocalCallableNames.exists(_.fullNameEqual(name))
      val accum = acc.withDefinedLocalCallableName(name)
      if (isError) {
        accum(SemanticError.localCallableAlreadyDefined(name.fullName, name.position))
      } else {
        accum
      }
  }

  private val duplicateLocalCallableParameter: VariableCheck = {
    case (
        acc,
        Scope.Definition.LocalCallable(_, inputSignature)
      ) =>
      val duplicateParameterErrors = inputSignature.groupBy(_.name).collect {
        case (name, parameters) if parameters.size > 1 =>
          val lastDuplicatePosition = parameters.map(_.position).maxBy(_.offset)
          SemanticError.duplicateParameter(name, lastDuplicatePosition)
      }
      if (duplicateParameterErrors.nonEmpty) {
        acc(duplicateParameterErrors)
      } else {
        acc
      }
  }

  private val statementChecks: Seq[VariableCheck] =
    Seq(
      redeclarationOfVariable,
      multipleReturnColumns,
      incompatibleReturnColumns,
      invalidUseOfReturnStar,
      variableNotDefinedInScopeClause,
      invalidEntityReferenceInUpdatingClause,
      localCallableAlreadyDefined,
      duplicateLocalCallableParameter
    )

  private val unboundVariablesInPatternExpression: VariableCheck = {
    case (acc, ExpressionScope(_: PatternExpression, _, _, declarations, _)) if declarations.variables.nonEmpty =>
      acc(declarations.variables.map(v => SemanticError.unboundVariablesInPatternExpression(v.name, v.position)))
  }

  private val variableNotDefined: VariableCheck = {
    case (
        Acc.CreatePattern(acc, topo, patternVariables, create, inScalarSubquery),
        Scope.Expr.Variable(variable, incoming)
      ) =>
      val isIncoming = incoming.constants contains variable
      val declaredInSameGraphPattern = topo contains variable
      val declaredInSamePathPattern = patternVariables contains variable
      (isIncoming, declaredInSameGraphPattern, inScalarSubquery, declaredInSamePathPattern, version) match {
        case (true, false, _, _, _)                         => acc
        case (_, true, false, false, CypherVersion.Cypher5) => acc
        case (_, false, _, _, _) => acc(SemanticError.variableNotDefined(variable.name, variable.position))
        case _ => acc(SemanticError.invalidEntityReference(variable.name, create.name, variable.position))
      }
    case (
        Acc.MergePattern(acc, topo, merge),
        Scope.Expr.Variable(variable, incoming)
      ) if !(incoming.constants contains variable) =>
      if (topo contains variable) {
        if (version == CypherVersion.Cypher5) acc
        else
          acc(SemanticError.invalidEntityReference(variable.name, merge.name, variable.position))
      } else {
        acc(SemanticError.variableNotDefined(variable.name, variable.position))
      }
    case (Acc.Aggregation(acc, clause, incomingToClause), Scope.Expr.VariableAggregation(v, constants, recognizable)) =>
      if (!(constants contains v) && !recognizable(v)) getVariableNotDefined(acc, clause, incomingToClause, v) else acc
    case (Acc.Aggregation(acc, clause, incomingToClause), Scope.Expr.Variable(variable, in))
      if !(in.constants contains variable) => getVariableNotDefined(acc, clause, incomingToClause, variable)
    case (
        Acc.Aggregation(acc, clause, incomingToClause),
        ExpressionScope(_: FullSubqueryExpression, ctx: ProjectionExpressionContext, refs, _, _)
      ) if !refs.forall(r => ctx.projectionItems.containsSubclauseRef(r) || ctx.constants(r)) =>
      refs.foldLeft(acc) { case (acc, lv) => getVariableNotDefined(acc, clause, incomingToClause, lv) }
    case (acc, Scope.Expr.Variable(variable, incoming)) if !(incoming.constants contains variable) =>
      acc(SemanticError.variableNotDefined(variable.name, variable.position))
  }

  private val expressionChecks: Seq[VariableCheck] =
    Seq(
      unboundVariablesInPatternExpression,
      variableNotDefined
    )

  private val expressionAggregationChecks: Seq[VariableCheck] =
    Seq(
      variableNotDefined
    )

  private val redeclarationOfVariablesInPatterns: VariableCheck = {
    case (acc, Scope.Pattern.NamedPath(path, topo, Declarations(_, variables, _))) =>
      acc((topo.filter(_.name == path.name) ++ variables.filter(x =>
        x.name == path.name && x.position != path.position
      ))
        .map(_ => SemanticError.variableAlreadyDeclared(path.name, path.position)).toSeq)
    case (acc, Scope.Pattern.Quantified(groupings, referenced))
      if referenced.intersect(groupings.map(_.group)).nonEmpty =>
      acc(referenced.intersect(groupings.map(_.group)).map(v =>
        SemanticError.variableAlreadyDeclared(v.name, v.position)
      ).toSeq)
    case Scope.Pattern.VariableInPatternAlreadyDeclared(acc, name, position) =>
      acc(SemanticError.variableAlreadyDeclared(name, position))
    case (acc, Scope.Pattern.Element(variable, group, referenced))
      if referenced.intersect(group).exists(_.name == variable.name) =>
      acc(SemanticError.variableAlreadyDeclared(variable.name, variable.position))
  }

  private val relationshipVariableAlreadyBound: VariableCheck = {
    case (acc, Scope.Pattern.ShortestPath(name, element, topologicalConstants)) => element match {
        case RelationshipChain(_, rel, _) if rel.variable.exists(topologicalConstants.contains) =>
          acc(SemanticError.relationshipVariableAlreadyBound(name, rel.position))
        case _ => acc
      }
  }

  private val patternChecks: Seq[VariableCheck] =
    Seq(
      redeclarationOfVariablesInPatterns,
      relationshipVariableAlreadyBound
    )

  private def checkFold(s: WorkingScope, acc: Acc, checks: Seq[VariableCheck]): Acc =
    checks.foldLeft(acc) { case (acc, check) =>
      check.applyOrElse((acc, s), (_: (Acc, WorkingScope)) => acc)
    }

  private def checkWorkingScope(acc: Acc, workingScope: WorkingScope): Acc =
    if (checkAggregations)
      workingScope match {
        case es: ExpressionScope => checkFold(es, acc, expressionAggregationChecks)
        case _                   => acc
      }
    else
      workingScope match {
        case ss: StatementScope  => checkFold(ss, acc, statementChecks)
        case es: ExpressionScope => checkFold(es, acc, expressionChecks)
        case ps: PatternScope    => checkFold(ps, acc, patternChecks)
        case _                   => acc
      }

  private def updateAccAndTraverse(
    acc: Acc,
    scope: WorkingScope
  )(foldingBehavior: Acc => FoldingBehavior[Acc]): FoldingBehavior[Acc] = {
    val checkSelf = checkWorkingScope(acc, scope)
    foldingBehavior(checkSelf)
  }

  private def folderWorkingScopes(acc: Acc, target: Foldable): Acc =
    target.folder.treeFold(acc) {
      case ws: WorkingScope => acc => TraverseChildren(checkWorkingScope(acc, ws))
    }

  private def folderSubclauseScopes(acc: Acc, target: Foldable, aggregatingExpressions: Set[Expression]): Acc =
    target.folder.treeFold(acc) {
      case ExpressionScope(expr: Expression, _, _, _, _) if aggregatingExpressions(expr) => acc => SkipChildren(acc)
      case ws: WorkingScope => acc => TraverseChildren(checkWorkingScope(acc, ws))
    }

  private def collectSemanticErrors(workingScope: WorkingScope) = workingScope.folder.treeFold(Acc.init) {
    case s @ ExpressionScope(_: IterableExpression, _, _, d, _) => {
      case acc @ Acc(_, dCtx: DeclaringContext, _, _, _, _) if dCtx.declared.nonEmpty =>
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            _acc.inVariableContext(dCtx.updateDeclared(d.constants.toSet)),
            acc => acc.inVariableContext(_acc.variableContext)
          )
        )
      case acc => updateAccAndTraverse(acc, s)(_acc => TraverseChildren(_acc))
    }
    case s @ ExpressionScope(_: FullSubqueryExpression, in, _, _, _) => acc =>
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            _acc.inReturnContext(SubqueryExpression(in.allSymbols)),
            acc => acc.inReturnContext(_acc.scopeContext)
          )
        )
    case s @ StatementScope(_: NextStatement, in, _, _, _, _, children, _) => acc =>
        updateAccAndTraverse(acc, s)(_acc => {
          val trunkAcc = folderWorkingScopes(_acc.inReturnContext(NextStatement(in.constants)), children.dropRight(1))
          val tailAcc =
            folderWorkingScopes(_acc.withDefinedLocalCallableNames(trunkAcc.definedLocalCallableNames), children.tail)
          SkipChildren(Acc(
            tailAcc.scopeContext,
            tailAcc.variableContext,
            tailAcc.projectionContext,
            tailAcc.foreachContext,
            tailAcc.definedLocalCallableNames,
            trunkAcc.errors ++ tailAcc.errors
          ))
        })
    case s @ StatementScope(_: LocalCallableDefinition, _, _, _, _, _, _, _) => acc =>
        updateAccAndTraverse(acc, s)(_acc => {
          TraverseChildrenNewAccForSiblings(
            _acc.inReturnContext(Unopinionated),
            acc => acc.inReturnContext(_acc.scopeContext)
          )
        })

    case s @ StatementScope(c: CreateOrInsert, _, _, declared, _, _, _, _) => acc =>
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            _acc.inVariableContext(UpdatingPattern(declared.variables.toSet, Set.empty, c)),
            acc => acc.inVariableContext(_acc.variableContext)
          )
        )
    case s @ StatementScope(m: Merge, _, _, declared, _, _, _, _) => acc =>
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            _acc.inVariableContext(UpdatingPattern(declared.variables.toSet, Set.empty, m)),
            acc => acc.inVariableContext(_acc.variableContext)
          )
        )
    case s @ StatementScope(_: Match, _, _, _, _, _, _, _) => acc =>
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            _acc.inMatchingPattern,
            acc => acc.inVariableContext(_acc.variableContext)
          )
        )
    case s @ StatementScope(f: Foreach, incoming, _, _, _, _, _, _) => acc =>
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            _acc.withForeachClause(incoming.allSymbols filterNot (_.name == f.variable.name)),
            acc => acc.inForeachClause(_acc.foreachContext)
          )
        )
    case s @ StatementScope(p: ProjectionClause, incoming, _, _, _, _, children, _) => acc =>
        updateAccAndTraverse(acc, s)(_acc =>
          if (p.isAggregating) {
            val context = Aggregating(p.name, incoming.variables)

            // Partition children into groupingItems, aggregatingItems and subclauseItems
            val (groupingItems, other) = children.partition(_.incoming.isInstanceOf[CommonContext])
            val (subclauses, aggregatingItems) =
              other.partition(_.incoming.asInstanceOf[ProjectionExpressionContext].inSubclause)

            val aggregatingExpressions = aggregatingItems.map(_.astNode.asInstanceOf[Expression]).toSet

            val groupingItemAcc = folderWorkingScopes(_acc, groupingItems)
            val aggregatingItemAcc = folderWorkingScopes(_acc.inProjectionContext(context), aggregatingItems)
            val subclauseAcc =
              folderSubclauseScopes(_acc.inProjectionContext(context), subclauses, aggregatingExpressions)

            val subclauseErrors = subclauses
              .filter {
                case ExpressionScope(expr: Expression, _, _, _, _) =>
                  !(aggregatingExpressions(expr) ||
                    expr.subExpressions.exists(subExpr => aggregatingExpressions(subExpr)))
                case _ => true
              }
              .flatMap {
                case ExpressionScope(_, ProjectionExpressionContext(_, variables, _, keys, true), refs, _, _) =>
                  refs.filter(r => !keys.containsSubclauseRef(r) && variables(r)).map(r =>
                    SemanticError.inaccessibleVariable(r.name, p.name, r.position)
                  ).toSeq
                case _ => Seq.empty
              }

            val (inaccessibleVariable, otherErrors) =
              aggregatingItemAcc.errors.partition(_.gqlStatusObject.cause().get().gqlStatus() == "42N44")

            val aggErrors = if (checkAggregations) {
              if (inaccessibleVariable.nonEmpty) {
                val variableNames: Set[String] =
                  inaccessibleVariable.map(
                    _.gqlStatusObject.cause().get()
                      .asInstanceOf[ErrorGqlStatusObjectImplementation]
                      .getParamValue(StringParam.variable)
                      .asInstanceOf[String]
                  )
                Seq(SemanticError.invalidReferenceToGroupingExpression(
                  variableNames.toSeq,
                  inaccessibleVariable.head.position
                ))
              } else Seq.empty
            } else otherErrors

            SkipChildren(_acc(groupingItemAcc.errors ++ aggErrors ++ subclauseAcc.errors ++ subclauseErrors))
          } else {
            TraverseChildrenNewAccForSiblings(
              _acc.inProjectionContext(NonAggregating),
              acc => {
                acc.inProjectionContext(_acc.projectionContext)
              }
            )
          }
        )

    case s @ PatternScope(_: RelationshipChain, _, _, Declarations(_, variables, _), _, _) => acc =>
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            if (_acc.hasPatternVariables) _acc else _acc.withPatternVariables(variables.toSet, inRelationship = true),
            acc => acc.inVariableContext(_acc.variableContext)
          )
        )
    case s @ PatternScope(_: NodePattern, _, _, Declarations(_, variables, _), _, _) => acc =>
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            if (_acc.hasPatternVariables) _acc else _acc.withPatternVariables(variables.toSet),
            acc => acc.inVariableContext(_acc.variableContext)
          )
        )
    case ws: WorkingScope => acc => TraverseChildren(checkWorkingScope(acc, ws))
  }

  // this collects all errors it can find
  private def collectAll(workingScope: WorkingScope): Iterable[SemanticError] = {
    collectSemanticErrors(workingScope).errors
  }

  // this collects the first errors it can find
  private def collectFirst(workingScope: WorkingScope): Iterable[SemanticError] = {
    val errors = collectSemanticErrors(workingScope).errors
    Option.when(errors.nonEmpty)(errors.head)
  }
}

case object VariableChecker extends Phase[BaseContext, BaseState, BaseState] with StepSequencer.Step {

  override def process(from: BaseState, context: BaseContext): BaseState = {
    if (context.semanticFeatures contains ScopeQueries) {
      val semanticsErrors = if (1 == 1) {
        from.maybeScopeState.map(s =>
          VariableChecker(context.cypherVersion, logger = context.notificationLogger).collectAll(s.workingScope) ++
            VariableChecker.checkForAmbiguousAggregation(from, context)
        )
      } else {
        from.maybeScopeState.map(s =>
          VariableChecker(context.cypherVersion, logger = context.notificationLogger).collectFirst(s.workingScope) ++
            VariableChecker.checkForAmbiguousAggregation(from, context)
        )
      }
      semanticsErrors.foreach(errors => context.errorHandler(errors.toSeq))
    }
    from
  }

  def gatherAllErrors(from: BaseState, context: BaseContext): Seq[SemanticError] = {
    val errors = from.maybeScopeState.map(s =>
      VariableChecker(context.cypherVersion, logger = context.notificationLogger).collectAll(s.workingScope).toSeq
    ).getOrElse(Seq.empty)
    errors.distinct
  }

  def checkForAmbiguousAggregation(from: BaseState, context: BaseContext): Seq[SemanticError] = {
    val errors = from.maybeScopeState.map(s =>
      VariableChecker(context.cypherVersion, checkAggregations = true, logger = context.notificationLogger)
        .collectAll(s.workingScope).toSeq
    ).getOrElse(Seq.empty)
    errors.filter(_.gqlStatusObject.cause().get().gqlStatus() == "42I18")
      .distinct
  }

  def checkForAmbiguousAggregationFromClause(
    from: BaseState,
    context: BaseContext,
    clause: ASTNode
  ): Seq[SemanticError] =
    from.scopeState().recordedScopes.get(clause).fold(Seq.empty[SemanticError]) { c =>
      VariableChecker(context.cypherVersion, checkAggregations = true, logger = context.notificationLogger)
        .collectAll(c).toSeq
        .filter(_.gqlStatusObject.cause().get().gqlStatus() == "42I18")
        .distinct
    }

  override val phase = CompilationPhase.VARIABLE_CHECK

  override def preConditions: Set[StepSequencer.Condition] = Set(BaseContains[WorkingScope]())

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set.empty

  override def postConditions: Set[StepSequencer.Condition] = Set.empty

  def isNotImplementedCode(error: SemanticErrorDef): Boolean = {
    if (error.gqlStatusObject != null && error.gqlStatusObject.cause().isPresent)
      !implementedErrorCodes.contains(error.gqlStatusObject.cause().get().gqlStatus())
    else
      true
  }

  val implementedErrorCodes: Set[String] = Set(
    "42I18", // 42I18 - invalid reference to implicitly grouped expressions
    "42I37", // 42I37 - invalidUseOfReturnStar
    "42N07", // 42N07 - variableAlreadyDeclaredInOuterScope
    "42N38", // 42N38 - multipleReturnColumnsWithSameName
    "42N39", // 42N39 - incompatibleReturnColumns
    "42N3A", // 42N3A - incompatibleSubqueryType
    "42N3B", // 42N3B - incompatibleNumberOfReturnColumns
    "42N44", // 42N44 - inaccessible variable
    "42N59", // 42N59 - variableAlreadyDeclared
    "42N62", // 42N62 - variableNotDefined
    "42N66", // 42N66 - relationshipVariableAlreadyBound
    "42N29" // 42N29 - pattern expression not allowed to introduce
  )

  def getErrorOrder(e: SemanticErrorDef): Int = {
    if (
      e.gqlStatusObject != null &&
      e.gqlStatusObject.cause() != null &&
      e.gqlStatusObject.cause().isPresent &&
      e.gqlStatusObject.cause().get().gqlStatus() != null
    ) {
      explicitOrderingOfErrorCodes.indexOf(e.gqlStatusObject.cause().get().gqlStatus())
    } else -1
  }

  val explicitOrderingOfErrorCodes: Seq[String] = Seq(
    "42N14",
    "42I28",
    "42I21",
    "42N21",
    "42N44",
    "42I43",
    "42N39",
    "42I32",
    "42I56",
    "42N07", // Variable declared in outer scope
    "42N59", // Variable already declared
    "42N62", // Variable not defined
    "42I37", // Invalid use of RETURN *
    "42I38",
    "42I69",
    "42N34",
    "42N71",
    "22N27",
    "42I41",
    "42I14",
    "42I32",
    "42I58",
    "42N77",
    "42I25",
    "42N24"
  )
}
