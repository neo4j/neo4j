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
import org.neo4j.cypher.internal.ast.CallClause
import org.neo4j.cypher.internal.ast.CommandClause
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.CreateOrInsert
import org.neo4j.cypher.internal.ast.Foreach
import org.neo4j.cypher.internal.ast.FullSubqueryExpression
import org.neo4j.cypher.internal.ast.ImportingWithSubqueryCall
import org.neo4j.cypher.internal.ast.LocalCallableDefinition
import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.RewrittenOptional
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.Search
import org.neo4j.cypher.internal.ast.StrictlyAdditiveProjection
import org.neo4j.cypher.internal.ast.SubqueryCall
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.Unwind
import org.neo4j.cypher.internal.ast.With
import org.neo4j.cypher.internal.ast.Yield
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.scoping.Declarations
import org.neo4j.cypher.internal.ast.semantics.scoping.ExpressionScope
import org.neo4j.cypher.internal.ast.semantics.scoping.PatternScope
import org.neo4j.cypher.internal.ast.semantics.scoping.StatementScope
import org.neo4j.cypher.internal.ast.semantics.scoping.TableResult
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.expressions.AllReducePredicate.AllReduceScope
import org.neo4j.cypher.internal.expressions.AllReducePredicate.ReductionStepVariableScope
import org.neo4j.cypher.internal.expressions.ExtractScope
import org.neo4j.cypher.internal.expressions.FilterScope
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternExpression
import org.neo4j.cypher.internal.expressions.ReduceScope
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.frontend.phases.BaseContains
import org.neo4j.cypher.internal.frontend.phases.BaseContext
import org.neo4j.cypher.internal.frontend.phases.BaseState
import org.neo4j.cypher.internal.frontend.phases.CompilationPhaseTracer.CompilationPhase
import org.neo4j.cypher.internal.frontend.phases.Phase
import org.neo4j.cypher.internal.notification.DeprecatedPropertyReferenceInCreate
import org.neo4j.cypher.internal.notification.InternalNotificationLogger
import org.neo4j.cypher.internal.util.Foldable.FoldingBehavior
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildrenNewAccForSiblings
import org.neo4j.cypher.internal.util.StepSequencer

case class VariableChecker(
  version: CypherVersion,
  logger: InternalNotificationLogger,
  debug: Option[VariableCheckerDebugLogger] = None
) extends VariableCheckerUtil {
  private val isDebugEnabled = debug.isDefined

  private def redeclarationOfVariable(acc: Acc, ss: StatementScope): Acc = ss match {
    case Scope.Clause.Declaring(astNode, incoming, Declarations(constants, variables, _), children)
      if !(constants.isEmpty && variables.isEmpty) =>
      // redeclaration of constants
      val redeclarationOfConstants = astNode match {
        case _: Foreach => Seq.empty // historically, the FOREACH iteration variable is allowed to shadow
        case _: Union   => Seq.empty // ignore, union variable declarations
        case _: CreateOrInsert | _: Merge if acc.foreachContext.allowedToShadow.nonEmpty =>
          val outerNames = acc.foreachContext.allowedToShadow.map(_.name)
          val bodyLocalDeclared = (constants ++ variables).filterNot(v => outerNames.contains(v.name))
          incoming.checkIfVariablesAreAlreadyDeclaredAsConstant(bodyLocalDeclared.toSet)
        // An optional procedure call is wrapped in a subquery by rewriting,
        // so should throw 42N59 instead of 42N07. The latter is muted below using a flag.
        case c: CallClause if c.optionalState == RewrittenOptional =>
          incoming.checkIfVariablesAreAlreadyDeclaredAsConstant(
            (constants ++ variables).toSet,
            SemanticError.variableAlreadyDeclared
          )
        case _ =>
          incoming.checkIfVariablesAreAlreadyDeclaredAsConstant((constants ++ variables).toSet)
      }
      // redeclaration of variables
      val redeclarationOfVariables = astNode match {
        case _: CommandClause =>
          incoming.checkIfVariablesAreAlreadyDeclaredAsVariable(variables)
        case pc: ProjectionClause if pc.returnItems.projectionType == StrictlyAdditiveProjection =>
          incoming.checkIfVariablesAreAlreadyDeclaredAsVariable(variables)
        case _: CallClause => incoming.checkIfVariablesHaveMultipleDeclarations(variables)
        case _: Unwind     => incoming.checkIfVariablesAreAlreadyDeclaredAsVariable(variables)
        case _: Search     => incoming.checkIfVariablesAreAlreadyDeclaredAsVariable(variables)
        // Mutes the error caused by rewriting optional call procedures
        case call: ScopeClauseSubqueryCall if call.addedInRewriteOptionalCall => Seq.empty
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
    case Scope.Clause.Command(incoming, children) =>
      val innerResult = children.last.result match {
        case TableResult(columns) => columns
        case _                    => Seq.empty
      }
      acc(innerResult.filter(x => incoming.allSymbols.exists(_.name == x.name)).map(v =>
        SemanticError.variableAlreadyDeclared(v.name, v.position)
      ))
    case _ => acc
  }

  private def multipleReturnColumns(acc: Acc, ss: StatementScope): Acc = ss match {
    case StatementScope(w: With, _, _, _, _, _, _, _)   => acc(findMultipleDeclarationsIn(w))
    case StatementScope(y: Yield, _, _, _, _, _, _, _)  => acc(findMultipleDeclarationsIn(y))
    case StatementScope(r: Return, _, _, _, _, _, _, _) => acc(findMultipleDeclarationsIn(r))
    case _                                              => acc
  }

  private def incompatibleReturnColumns(acc: Acc, ss: StatementScope): Acc = ss match {
    case StatementScope(u: Union, _, _, _, _, result, children, _) =>
      acc(getIncompatibleReturnColumnsForUnion(u.position, result, children))
    case StatementScope(_: ConditionalQueryWhen, _, _, _, _, result, children, _) =>
      acc(getIncompatibleReturnColumnsForConditionalQuery(result, children))
    case _ => acc
  }

  private def invalidUseOfReturnStar(acc: Acc, ss: StatementScope): Acc = (acc, ss) match {
    case (Acc.SubqueryExpr(a), Scope.Clause.ReturnStar(incoming, position))
      if incoming.constantsAndVariables.isEmpty => a(SemanticError.invalidUseOfReturnStar(position))
    case (Acc.Opinionated(a, constants), Scope.Clause.ReturnItems(items, position)) =>
      a(getAliasesShadowingConstants(items, constants, position))
    case (_, Scope.Clause.ReturnStar(incoming, position)) if incoming.isVariablesEmpty =>
      acc(SemanticError.invalidUseOfReturnStar(position))
    case _ => acc
  }

  private def variableNotDefinedInScopeClause(acc: Acc, ss: StatementScope): Acc = (acc, ss) match {
    case (Acc.Aggregation(a, incomingToClause), Scope.Clause.SubqueryCall(imports, incoming))
      if imports.nonEmpty =>
      imports.filter(v => !incoming.allSymbols.exists(_.name == v.name))
        .foldLeft(a) { case (innerAcc, v) => getVariableNotDefined(innerAcc, incomingToClause, v) }
    case (_, Scope.Clause.SubqueryCall(imports, incoming)) if imports.nonEmpty =>
      acc(imports.filter(v => !incoming.allSymbols.exists(_.name == v.name))
        .flatMap(v => Seq(SemanticError.variableNotDefined(v.name, v.position))))
    case _ => acc
  }

  private def variableNotDefinedInGroupBy(acc: Acc, ss: StatementScope): Acc = (acc, ss) match {
    case (_, Scope.Clause.GroupBy(references)) if references.hasSelfReference =>
      acc(references.getSelfReferences.map(lv => SemanticError.variableNotDefined(lv.name, lv.position)))
    case _ => acc
  }

  private def invalidEntityReferenceInUpdatingClause(acc: Acc, ss: StatementScope): Acc = (acc, ss) match {
    case ( // ≥ Cypher 25
        Acc.CreatePattern(a, topo, _, create, true),
        StatementScope(_: Match, _, _, declared, _, _, _, _)
      )
      if version != CypherVersion.Cypher5 && (declared.withoutAnonymousDeclaration.allSymbols.toSet intersect topo).nonEmpty =>
      a(declared.withoutAnonymousDeclaration.allSymbols.filter(topo).map(v =>
        SemanticError.invalidEntityReference(v.name, create.name, v.position)
      ))
    case ( // Cypher 5
        Acc.CreatePattern(a, topo, patternVars, create, true),
        StatementScope(_: Match, _, _, declared, _, _, _, _)
      )
      if version == CypherVersion.Cypher5 && (declared.withoutAnonymousDeclaration.allSymbols.toSet intersect topo).nonEmpty =>
      a(declared.withoutAnonymousDeclaration.allSymbols.flatMap(v =>
        if (patternVars contains v)
          Seq(SemanticError.invalidEntityReference(v.name, create.name, v.position))
        else {
          logger.log(DeprecatedPropertyReferenceInCreate(v.position, v.name))
          Seq()
        }
      ))
    case _ => acc
  }

  private def localCallableAlreadyDefined(acc: Acc, ss: StatementScope): Acc = ss match {
    case Scope.Definition.LocalCallable(name, _) =>
      val isError = acc.definedLocalCallableNames.exists(_.fullNameEqual(name))
      val accum = acc.withDefinedLocalCallableName(name)
      if (isError) accum(SemanticError.localCallableAlreadyDefined(name.fullName, name.position))
      else accum
    case _ => acc
  }

  private def duplicateLocalCallableParameter(acc: Acc, ss: StatementScope): Acc = ss match {
    case Scope.Definition.LocalCallable(_, inputSignature) =>
      val duplicateParameterErrors = inputSignature.groupBy(_.name).collect {
        case (name, parameters) if parameters.size > 1 =>
          val lastDuplicatePosition = parameters.map(_.position).maxBy(_.offset)
          SemanticError.duplicateParameter(name, lastDuplicatePosition)
      }
      if (duplicateParameterErrors.nonEmpty) acc(duplicateParameterErrors) else acc
    case _ => acc
  }

  private def unboundVariablesInPatternExpression(acc: Acc, es: ExpressionScope): Acc = es match {
    case ExpressionScope(_: PatternExpression, _, _, declarations, _) if declarations.variables.nonEmpty =>
      acc(declarations.variables.map(v => SemanticError.unboundVariablesInPatternExpression(v.name, v.position)))
    case _ => acc
  }

  private def variableNotDefined(acc: Acc, es: ExpressionScope): Acc = (acc, es) match {
    case (
        Acc.CreatePattern(a, topo, patternVariables, create, inScalarSubquery),
        Scope.Expr.Variable(variable, isConstant)
      ) =>
      val isIncoming = isConstant(variable)
      val declaredInSameGraphPattern = topo contains variable
      val declaredInSamePathPattern = patternVariables contains variable
      (isIncoming, declaredInSameGraphPattern, inScalarSubquery, declaredInSamePathPattern, version) match {
        case (true, false, _, _, _)                         => a
        case (_, true, false, false, CypherVersion.Cypher5) => a
        case (_, false, _, _, _) => a(SemanticError.variableNotDefined(variable.name, variable.position))
        case _ => a(SemanticError.invalidEntityReference(variable.name, create.name, variable.position))
      }
    case (
        Acc.MergePattern(a, topo, merge),
        Scope.Expr.Variable(variable, isConstant)
      ) if !isConstant(variable) =>
      if (topo contains variable) {
        if (version == CypherVersion.Cypher5) a
        else
          a(SemanticError.invalidEntityReference(variable.name, merge.name, variable.position))
      } else {
        a(SemanticError.variableNotDefined(variable.name, variable.position))
      }
    case (Acc.Aggregation(_, incomingToClause), Scope.Expr.Variable(variable, isConstant)) =>
      if (!incomingToClause(variable) && (!isConstant(variable) || es.referenced.hasSelfReference))
        acc(SemanticError.variableNotDefined(variable.name, variable.position))
      else acc
    case (_, Scope.Expr.Variable(variable, isConstant)) if !isConstant(variable) =>
      acc(SemanticError.variableNotDefined(variable.name, variable.position))
    case _ => acc
  }

  private def redeclarationOfVariablesInPatterns(acc: Acc, ps: PatternScope): Acc = (acc, ps) match {
    case (_, Scope.Pattern.NamedPath(path, topo, Declarations(_, variables, _))) =>
      acc((topo.filter(_.name == path.name) ++ variables.filter(x =>
        x.name == path.name && x.position != path.position
      ))
        .map(_ => SemanticError.variableAlreadyDeclared(path.name, path.position)).toSeq)
    case (acc, Scope.Pattern.Quantified(topo, declared, pathVars)) =>
      val pathNames = pathVars.map(_.name)
      val conflicts = declared.intersect(topo).filterNot(v => pathNames(v.name))
      acc(conflicts.map(v =>
        SemanticError.variableAlreadyDeclared(v.name, v.position)
      ).toSeq)
    case Scope.Pattern.VariableInUpdatingPatternAlreadyDeclared(a, name, position) =>
      a(SemanticError.variableAlreadyDeclared(name, position))
    case (_, Scope.Pattern.Element(variable, group, referenced))
      if referenced.intersect(group).exists(_.name == variable.name) =>
      acc(SemanticError.variableAlreadyDeclared(variable.name, variable.position))
    case _ => acc
  }

  private def relationshipVariableAlreadyBound(acc: Acc, ps: PatternScope): Acc = ps match {
    case Scope.Pattern.ShortestPath(name, element, topologicalConstants) => element match {
        case RelationshipChain(_, rel, _) if rel.variable.exists(topologicalConstants.contains) =>
          acc(SemanticError.relationshipVariableAlreadyBound(name, rel.position))
        case _ => acc
      }
    case _ => acc
  }

  private val statementChecks: Seq[(Acc, StatementScope) => Acc] = Seq(
    redeclarationOfVariable,
    multipleReturnColumns,
    incompatibleReturnColumns,
    invalidUseOfReturnStar,
    variableNotDefinedInScopeClause,
    variableNotDefinedInGroupBy,
    invalidEntityReferenceInUpdatingClause,
    localCallableAlreadyDefined,
    duplicateLocalCallableParameter
  )

  private val expressionChecks: Seq[(Acc, ExpressionScope) => Acc] = Seq(
    unboundVariablesInPatternExpression,
    variableNotDefined
  )

  private val patternChecks: Seq[(Acc, PatternScope) => Acc] = Seq(
    redeclarationOfVariablesInPatterns,
    relationshipVariableAlreadyBound
  )

  private def runChecks[S <: WorkingScope](acc: Acc, scope: S, checks: Seq[(Acc, S) => Acc]): Acc =
    checks.foldLeft(acc)((a, check) => check(a, scope))

  private def checkWorkingScope(acc: Acc, workingScope: WorkingScope): Acc = {
    if (isDebugEnabled) debug.foreach(_.logBeforeCheck(workingScope, acc))
    val checkResult = workingScope match {
      case ss: StatementScope  => runChecks(acc, ss, statementChecks)
      case es: ExpressionScope => runChecks(acc, es, expressionChecks)
      case ps: PatternScope    => runChecks(acc, ps, patternChecks)
      case _                   => acc
    }
    if (isDebugEnabled) debug.foreach(_.logAfterCheck(workingScope, checkResult))
    checkResult
  }

  private def updateAccAndTraverse(
    acc: Acc,
    scope: WorkingScope
  )(foldingBehavior: Acc => FoldingBehavior[Acc]): FoldingBehavior[Acc] = {
    val checkSelf = checkWorkingScope(acc, scope)
    foldingBehavior(checkSelf)
  }

  private def walk(acc: Acc, ws: WorkingScope): Acc = visitWorkingScope(ws, acc) match {
    case TraverseChildren(childAcc) =>
      ws.children.foldLeft(childAcc)(walk)
    case TraverseChildrenNewAccForSiblings(childAcc, mergeBack) =>
      val afterChildren = ws.children.foldLeft(childAcc)(walk)
      mergeBack(afterChildren)
    case SkipChildren(finalAcc) =>
      finalAcc
  }

  private def walkChecksOnly(acc: Acc, ws: WorkingScope): Acc = {
    val acc1 = checkWorkingScope(acc, ws)
    ws.children.foldLeft(acc1)(walkChecksOnly)
  }

  private def folderWorkingScopes(acc: Acc, scopes: Seq[WorkingScope]): Acc =
    scopes.foldLeft(acc)(walkChecksOnly)

  private def collectSemanticErrors(workingScope: WorkingScope): Acc = walk(Acc.init, workingScope)

  private def visitWorkingScope(ws: WorkingScope, acc: Acc): FoldingBehavior[Acc] = {
    if (isDebugEnabled) debug.foreach(_.logVisit(ws, acc))
    ws match {
      case s @ ExpressionScope(
          _: ExtractScope | _: FilterScope | _: ReduceScope | _: AllReduceScope | _: ReductionStepVariableScope,
          _,
          _,
          d,
          _
        ) =>
        acc match {
          case Acc(_, dCtx: DeclaringContext, _, _, _, _) if dCtx.declared.nonEmpty =>
            updateAccAndTraverse(acc, s)(_acc =>
              TraverseChildrenNewAccForSiblings(
                _acc.inVariableContext(dCtx.updateDeclared(d.constants.toSet)),
                acc => acc.inVariableContext(_acc.variableContext)
              )
            )
          case _ => updateAccAndTraverse(acc, s)(_acc => TraverseChildren(_acc))
        }

      case s @ ExpressionScope(_: FullSubqueryExpression, in, _, _, _) =>
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            _acc.inReturnContext(SubqueryExpression(in.allSymbols)),
            acc => acc.inReturnContext(_acc.scopeContext)
          )
        )

      case s @ StatementScope(_: NextStatement, in, _, _, _, _, children, _) =>
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

      case s @ StatementScope(_: LocalCallableDefinition, _, _, _, _, _, _, _) =>
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            _acc.inReturnContext(Unopinionated),
            acc => acc.inReturnContext(_acc.scopeContext)
          )
        )

      case s @ StatementScope(c: CreateOrInsert, _, _, declared, _, _, _, _) =>
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            _acc.inVariableContext(UpdatingPattern(declared.variables.toSet, Set.empty, c)),
            acc => acc.inVariableContext(_acc.variableContext)
          )
        )

      case s @ StatementScope(m: Merge, _, _, declared, _, _, _, _) =>
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            _acc.inVariableContext(UpdatingPattern(declared.variables.toSet, Set.empty, m)),
            acc => acc.inVariableContext(_acc.variableContext)
          )
        )

      case s @ StatementScope(_: Match, _, _, _, _, _, _, _) =>
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            _acc.inMatchingPattern,
            acc => acc.inVariableContext(_acc.variableContext)
          )
        )

      case s @ StatementScope(f: Foreach, incoming, _, _, _, _, _, _) =>
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            _acc.withForeachClause(incoming.allSymbols filterNot (_.name == f.variable.name)),
            acc => acc.inForeachClause(_acc.foreachContext)
          )
        )

      case s @ StatementScope(p: ProjectionClause, incoming, _, _, _, _, _, _) if p.isAggregating =>
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            _acc.inProjectionContext(Aggregating(incoming.variables)),
            acc => acc.inProjectionContext(_acc.projectionContext)
          )
        )

      case s @ StatementScope(call: SubqueryCall, outerIncoming, _, _, _, _, _, _) =>
        val importedSymbols: Set[LogicalVariable] = call match {
          case ImportingWithSubqueryCall(query, _, _) =>
            if (query.isCorrelated && query.importColumns.isEmpty) {
              outerIncoming.allSymbols
            } else {
              outerIncoming.allSymbols.filter(s => query.importColumns.exists(_.name == s.name))
            }
          case ScopeClauseSubqueryCall(_, isImportingAll, importedVars, _, _, _) =>
            if (isImportingAll) {
              outerIncoming.allSymbols
            } else {
              outerIncoming.allSymbols.filter(s => importedVars.exists(_.name == s.name))
            }
        }
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            _acc.dropIncomingVariablesToClause(importedSymbols),
            acc => acc.inProjectionContext(_acc.projectionContext)
          )
        )

      case s @ PatternScope(_: RelationshipChain, _, _, Declarations(_, variables, _), _, _) =>
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            if (_acc.hasPatternVariables) _acc else _acc.withPatternVariables(variables.toSet, inRelationship = true),
            acc => acc.inVariableContext(_acc.variableContext)
          )
        )

      case s @ PatternScope(_: NodePattern, _, _, Declarations(_, variables, _), _, _) =>
        updateAccAndTraverse(acc, s)(_acc =>
          TraverseChildrenNewAccForSiblings(
            if (_acc.hasPatternVariables) _acc else _acc.withPatternVariables(variables.toSet),
            acc => acc.inVariableContext(_acc.variableContext)
          )
        )

      case _ => TraverseChildren(checkWorkingScope(acc, ws))
    }
  }

  private def collectAll(workingScope: WorkingScope): Iterable[SemanticError] = {
    collectSemanticErrors(workingScope).errors
  }
}

case object VariableChecker extends Phase[BaseContext, BaseState, BaseState] with StepSequencer.Step {

  override def process(from: BaseState, context: BaseContext): BaseState = {
    val isDebug = context.isDebugSession
    val maybeDebugLogger = if (isDebug) from.maybeDebugInfo.flatMap(_.maybeVariableCheckerDebugLogger) else None
    val semanticsErrors = from.maybeScopeState.map(s =>
      VariableChecker(
        context.cypherVersion,
        logger = context.notificationLogger,
        debug = maybeDebugLogger
      ).collectAll(s.workingScope)
    )
    semanticsErrors.foreach(errors => context.errorHandler(errors.toSeq))
    from
  }

  def gatherAllErrors(
    from: BaseState,
    context: BaseContext
  ): Seq[SemanticError] = {
    val errors = from.maybeScopeState.map(s =>
      VariableChecker(
        context.cypherVersion,
        logger = context.notificationLogger
      ).collectAll(s.workingScope).toSeq
    ).getOrElse(Seq.empty)
    errors.distinct
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
    "42N34",
    "42N07", // Variable declared in outer scope
    "42N59", // Variable already declared
    "42N62", // Variable not defined
    "42I37", // Invalid use of RETURN *
    "42I38",
    "42I69",
    "42N25",
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
