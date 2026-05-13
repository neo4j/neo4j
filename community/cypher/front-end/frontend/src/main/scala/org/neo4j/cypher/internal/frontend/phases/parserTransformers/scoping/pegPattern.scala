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

import org.neo4j.cypher.internal.ast.semantics.scoping.Declarations
import org.neo4j.cypher.internal.ast.semantics.scoping.PatternIncomingContext
import org.neo4j.cypher.internal.ast.semantics.scoping.PatternScope
import org.neo4j.cypher.internal.ast.semantics.scoping.References
import org.neo4j.cypher.internal.ast.semantics.scoping.RegularContext
import org.neo4j.cypher.internal.ast.semantics.scoping.TableResult
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope.unitVariables
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.ParenthesizedPath
import org.neo4j.cypher.internal.expressions.PathConcatenation
import org.neo4j.cypher.internal.expressions.PathPatternPart
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.Pattern.ForMatch
import org.neo4j.cypher.internal.expressions.Pattern.ForUpdate
import org.neo4j.cypher.internal.expressions.PatternAtom
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.PatternPart.AllPaths
import org.neo4j.cypher.internal.expressions.PrefixedPatternPart
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ASTNode

import scala.annotation.tailrec

object pegPattern {

  def apply(pattern: Pattern, incoming: RegularContext, foreachIterVar: Option[LogicalVariable])(implicit
    c: PegContext): WorkingScope = {
    c.getRecordScopeOrElse[Pattern](
      pattern,
      incoming,
      inImportingWith = false,
      foreachIterVar,
      applyUncached(_, _, foreachIterVar)
    )
  }

  private def applyUncached(
    pattern: Pattern,
    incoming: RegularContext,
    foreachIterVar: Option[LogicalVariable]
  )(implicit c: PegContext): WorkingScope = {
    implicit val astNode: ASTNode = pattern
    val ctx: PegContext = c
    val patternIncomingContext = PatternIncomingContext(
      topologicalConstants = incoming.constants,
      predicateConstants = incoming.constants union (pattern match {
        case _: ForMatch  => collectVisibleVariables(pattern) diff incoming.constants
        case _: ForUpdate => Set.empty
      }),
      pathConstants = pattern match {
        case _: ForMatch  => collectPathVariables(pattern)
        case _: ForUpdate => Set.empty
      },
      groupConstants = Set.empty,
      localCallables = incoming.localCallables
    )
    val partForeachIter: Option[LogicalVariable] = pattern match {
      case _: ForUpdate => foreachIterVar
      case _: ForMatch  => None
    }
    val children =
      pattern.patternParts.scanLeft(WorkingScope.aprioriPattern(patternIncomingContext, RegularContext.unit)) {
        case (precedingPartsScope, currentPart) =>
          val newIncoming =
            precedingPartsScope.patternIncoming.amendedWithConstantAccordingToVersion(
              precedingPartsScope.outgoing.variables,
              ctx.language
            )
          scopePatternPart(currentPart, newIncoming, partForeachIter)
      }.tail
    val result =
      if (children.size == 1) {
        children.head
      } else {
        val declared = collectDeclaredFromChildren(children)
        val columns = collectColumnsFromChildren(children)
        patternIncomingContext.resultScope(TableResult(columns), children, declared = declared)
      }
    result.tagForCache(inImportingWith = false, foreachIterVar)
  }

  def apply(patternPart: PatternPart, incoming: RegularContext, foreachIterVar: Option[LogicalVariable])(implicit
    c: PegContext): WorkingScope = {
    c.getRecordScopeOrElse[PatternPart](
      patternPart,
      incoming,
      inImportingWith = false,
      foreachIterVar,
      applyUncached(_, _, foreachIterVar)
    )
  }

  private def applyUncached(
    patternPart: PatternPart,
    incoming: RegularContext,
    foreachIterVar: Option[LogicalVariable]
  )(
    implicit c: PegContext
  ): WorkingScope = {
    val patternIncomingContext = PatternIncomingContext(
      topologicalConstants = incoming.constants,
      predicateConstants = incoming.constants,
      pathConstants = Set.empty,
      groupConstants = Set.empty,
      localCallables = incoming.localCallables
    )
    scopePatternPart(patternPart, patternIncomingContext, foreachIterVar)
      .tagForCache(inImportingWith = false, foreachIterVar)
  }

  def apply(
    patternElement: PatternElement,
    incoming: RegularContext,
    foreachIterVar: Option[LogicalVariable]
  )(implicit c: PegContext): WorkingScope = {
    c.getRecordScopeOrElse[PatternElement](
      patternElement,
      incoming,
      inImportingWith = false,
      foreachIterVar,
      applyUncached(_, _, foreachIterVar)
    )
  }

  private def applyUncached(
    patternElement: PatternElement,
    incoming: RegularContext,
    foreachIterVar: Option[LogicalVariable]
  )(implicit c: PegContext): WorkingScope = {
    val patternIncomingContext = PatternIncomingContext(
      topologicalConstants = incoming.constants,
      predicateConstants =
        incoming.constants union (collectVisibleVariablesOfPatternElement(patternElement) diff incoming.constants),
      pathConstants = Set.empty,
      groupConstants = Set.empty,
      localCallables = incoming.localCallables
    )
    scopePatternElement(patternElement, patternIncomingContext, foreachIterVar)
      .tagForCache(inImportingWith = false, foreachIterVar)
  }

  private def scopePatternPart(
    patternPart: PatternPart,
    incoming: PatternIncomingContext,
    foreachIterVar: Option[LogicalVariable]
  )(implicit c: PegContext): PatternScope = {
    implicit val astNode: ASTNode = patternPart
    patternPart match {
      case PrefixedPatternPart(selector, _, patternPart) =>
        selector match {
          case AllPaths() => scopePatternPart(patternPart, incoming, foreachIterVar)
          case _ =>
            val newPathConstants = collectPathVariablesOfPatternPart(patternPart)
            val patternPartIncoming = incoming.removePathConstants().addPathConstants(newPathConstants)
            val patternPartScope = scopePatternPart(patternPart, patternPartIncoming, foreachIterVar)
            val children = Seq(patternPartScope)
            incoming.resultScope(
              patternPartScope.result,
              children,
              patternPartScope.declared
            )
        }
      case NamedPatternPart(variable, patternPart) =>
        val child = scopePatternPart(patternPart, incoming, foreachIterVar)
        val declared = Declarations(Seq.empty, variable +: child.declared.variables)
        val columns = variable +: child.result.columns
        incoming.resultScope(TableResult(columns), Seq(child), declared)
      case PathPatternPart(np: NodePattern) =>
        scopePatternAtom(np, incoming, forceFreshExceptIter = foreachIterVar)
      case PathPatternPart(element) => scopePatternElement(element, incoming, foreachIterVar)
      case sppp @ ShortestPathsPatternPart(element, _) =>
        scopePatternElement(element, incoming, foreachIterVar).withAstNode(sppp)
    }
  }

  private def scopePatternElement(
    patternElement: PatternElement,
    incoming: PatternIncomingContext,
    foreachIterVar: Option[LogicalVariable]
  )(implicit c: PegContext): PatternScope = {
    implicit val astNode: ASTNode = patternElement
    patternElement match {
      case PathConcatenation(pathFactors) =>
        val children = pathFactors.scanLeft(WorkingScope.aprioriPattern(incoming, RegularContext.unit)) {
          case (precedingFactorsScope, currentFactor) =>
            val newIncoming =
              precedingFactorsScope.patternIncoming.amendedWithTopologicalConstants(
                precedingFactorsScope.outgoing.variables
              )
            scopePatternElement(currentFactor, newIncoming, foreachIterVar)
        }.tail
        val declared = collectDeclaredFromChildren(children)
        val columns = collectColumnsFromChildren(children)
        val groups = collectGroupsFromChildren(children)
        incoming.addGroupConstants(groups).resultScope(TableResult(columns), children, declared = declared)
      case quantifiedPath: QuantifiedPath =>
        scopeQuantifiedPath(quantifiedPath, incoming)
      case parenthesizedPath: ParenthesizedPath =>
        scopeParenthesizedPath(parenthesizedPath, incoming)
      case nodePattern: NodePattern =>
        scopePatternAtom(nodePattern, incoming, forceFreshExceptIter = None)
      case relationshipChain: RelationshipChain =>
        val patternAtoms = collectPatternAtoms(relationshipChain)
        val children =
          patternAtoms.scanLeft(WorkingScope.aprioriPattern(incoming, RegularContext.unit)) {
            case (precedingAtomsScope, currentAtom: NodePattern) =>
              val newIncoming = precedingAtomsScope.patternIncoming.amendedWithTopologicalConstants(
                precedingAtomsScope.outgoing.variables
              )
              scopePatternAtom(currentAtom, newIncoming, forceFreshExceptIter = None)
            case (precedingAtomsScope, currentAtom: RelationshipPattern) =>
              val newIncoming = precedingAtomsScope.patternIncoming.amendedWithTopologicalConstants(
                precedingAtomsScope.outgoing.variables
              )
              scopePatternAtom(currentAtom, newIncoming, forceFreshExceptIter = foreachIterVar)
            case (precedingAtomsScope, currentAtom) =>
              val newIncoming = precedingAtomsScope.patternIncoming.amendedWithTopologicalConstants(
                precedingAtomsScope.outgoing.variables
              )
              scopePatternAtom(currentAtom, newIncoming, forceFreshExceptIter = None)
          }.tail
        val declared = collectDeclaredFromChildren(children)
        val columns = collectColumnsFromChildren(children)
        incoming.resultScope(TableResult(columns), children, declared)
    }
  }

  private def scopePatternAtom(
    patternAtom: PatternAtom,
    incoming: PatternIncomingContext,
    forceFreshExceptIter: Option[LogicalVariable]
  )(implicit c: PegContext): PatternScope = {
    implicit val astNode: ASTNode = patternAtom
    patternAtom match {
      case NodePattern(variableOpt, labelExpressionOpt, propertiesOpt, predicateOpt) =>
        val predicateIncoming =
          variableOpt.map(v => incoming.amendedWithTopologicalConstant(v)).getOrElse(incoming)
        val labelExpressionScopeOpt =
          labelExpressionOpt.map(labelExpression => scopePredicate(labelExpression, predicateIncoming))
        val propertiesScopeOpt =
          propertiesOpt.map(expression => scopePredicate(expression, predicateIncoming))
        val predicateScopeOpt = predicateOpt.map(predicate => scopePredicate(predicate, predicateIncoming))
        val matchesIter = variableOpt.exists(v => forceFreshExceptIter.exists(_.name == v.name))
        val effectivelyForceFresh = forceFreshExceptIter.isDefined && !matchesIter
        val (boundVariables, newVariables) =
          if (effectivelyForceFresh) (None, variableOpt)
          else variableOpt.partition(v => incoming.topologicalConstants contains v)
        val newVariablesWithAnon =
          if (variableOpt.isEmpty) Seq(Variable(c.anonVarGen.nextName)(patternAtom.position, isIsolated = false))
          else newVariables.toSeq
        val columns = variableOpt.toSeq
        val children = Seq(labelExpressionScopeOpt, propertiesScopeOpt, predicateScopeOpt).flatten
        val declared = Declarations(Seq.empty, newVariablesWithAnon)
        incoming.resultScope(
          TableResult(columns),
          children,
          declared,
          References.connect(boundVariables.toSeq, incoming.allSymbols)
        )

      case RelationshipPattern(variableOpt, labelExpressionOpt, _, propertiesOpt, predicateOpt, _) =>
        val predicateIncoming =
          variableOpt.map(v => incoming.amendedWithTopologicalConstant(v)).getOrElse(incoming)
        val labelExpressionScopeOpt =
          labelExpressionOpt.map(labelExpression => scopePredicate(labelExpression, predicateIncoming))
        val propertiesScopeOpt = propertiesOpt.map(expression => scopePredicate(expression, predicateIncoming))
        val predicateScopeOpt = predicateOpt.map(predicate => scopePredicate(predicate, predicateIncoming))
        val matchesIter = variableOpt.exists(v => forceFreshExceptIter.exists(_.name == v.name))
        val effectivelyForceFresh = forceFreshExceptIter.isDefined && !matchesIter
        val (boundVariables, newVariables) =
          if (effectivelyForceFresh) (None, variableOpt)
          else variableOpt.partition(v => incoming.topologicalConstants contains v)
        val newVariablesWithAnon =
          if (variableOpt.isEmpty) Seq(Variable(c.anonVarGen.nextName)(patternAtom.position, isIsolated = false))
          else newVariables.toSeq
        val columns = variableOpt.toSeq
        val children = Seq(labelExpressionScopeOpt, propertiesScopeOpt, predicateScopeOpt).flatten
        val declared = Declarations(Seq.empty, newVariablesWithAnon)
        incoming.resultScope(
          TableResult(columns),
          children,
          declared,
          References.connect(boundVariables.toSeq, incoming.allSymbols)
        )

      case parenthesizedPath: ParenthesizedPath => scopeParenthesizedPath(parenthesizedPath, incoming)
      case quantifiedPath: QuantifiedPath       => scopeQuantifiedPath(quantifiedPath, incoming)
    }
  }

  private def scopeParenthesizedPath(
    parenthesizedPath: ParenthesizedPath,
    incoming: PatternIncomingContext
  )(implicit c: PegContext): PatternScope = {
    implicit val astNode: ASTNode = parenthesizedPath
    val ParenthesizedPath(patternPart, whereExpressionOpt) = parenthesizedPath
    val newPathConstants = collectPathVariablesOfPatternPart(patternPart)
    val newIncoming = incoming.removePathConstants().addPathConstants(newPathConstants)
    val patternPartScope = scopePatternPart(patternPart, newIncoming, foreachIterVar = None)
    val whereExpressionScopes = whereExpressionOpt.map(whereExpression =>
      Seq(scopePredicate(whereExpression, newIncoming))
    ).getOrElse(Seq.empty[WorkingScope])
    val children = patternPartScope +: whereExpressionScopes
    incoming.resultScope(
      patternPartScope.result,
      children,
      patternPartScope.declared
    )
  }

  private def scopeQuantifiedPath(
    quantifiedPath: QuantifiedPath,
    incoming: PatternIncomingContext
  )(implicit c: PegContext): PatternScope = {
    implicit val astNode: ASTNode = quantifiedPath
    val QuantifiedPath(patternPart, _, whereExpressionOpt, variableGroupings) = quantifiedPath

    val singletons = variableGroupings.map(_.singleton)
    val groupings = variableGroupings.map(_.group)

    val newPathConstants = collectPathVariablesOfPatternPart(patternPart)
    val newIncoming =
      incoming.removePathConstants().addPathConstants(newPathConstants).replaceOccurrences(variableGroupings)
    val patternPartScope = scopePatternPart(patternPart, newIncoming, foreachIterVar = None)
    val whereExpressionScopes = whereExpressionOpt.map(whereExpression =>
      Seq(scopePredicate(whereExpression, newIncoming))
    ).getOrElse(Seq.empty[WorkingScope])
    val children = patternPartScope +: whereExpressionScopes

    // Inner sees singleton — each VariableGrouping.singleton references the same-named
    // column declared inside the QPP's pattern-part scope. Outer sees group.
    val singletonRefs = References.resolveByName(singletons, patternPartScope.result.getColumns)

    val outwardFacingResult = TableResult(patternPartScope.result.getColumns.filterNot(singletons) ++ groupings.toSeq)
    val outwardFacingDeclared =
      Declarations(
        constants = Seq.empty,
        variables = patternPartScope.declared.variables.filterNot(singletons) ++ groupings.toSeq
      )

    val (result, declared) = (outwardFacingResult, outwardFacingDeclared)

    incoming
      .addGroupConstants(groupings)
      .resultScope(result, children, declared = declared)
      .addReferences(singletonRefs)
  }

  @inline private def scopePredicate(
    astNode: ASTNode,
    incoming: PatternIncomingContext
  )(implicit c: PegContext): WorkingScope = {
    ScopeSurveyor.scope(
      astNode,
      RegularContext(
        constants = incoming.predicateConstants union incoming.pathConstants,
        variables = unitVariables,
        localCallables = incoming.localCallables
      ),
      c
    )
  }

  private def collectPathVariablesOfPatternPart(patternPart: PatternPart): Set[LogicalVariable] = {
    patternPart match {
      case NamedPatternPart(variable, _) => Set(variable)
      case PrefixedPatternPart(_, _, patternPart) =>
        collectPathVariablesOfPatternPart(patternPart)
      case _ => Set.empty
    }
  }.toSet

  private def collectPathVariables(pattern: Pattern): Set[LogicalVariable] = {
    pattern.patternParts.flatMap(pp => collectPathVariablesOfPatternPart(pp)).toSet
  }

  private def collectVisibleVariables(pattern: Pattern): Set[LogicalVariable] = {
    pattern.patternParts.flatMap(pp => collectVisibleVariablesOfPatternPart(pp)).toSet
  }

  @tailrec
  private def collectVisibleVariablesOfPatternPart(patternPart: PatternPart): Set[LogicalVariable] = {
    patternPart match {
      case PrefixedPatternPart(_, _, patternPart) =>
        collectVisibleVariablesOfPatternPart(patternPart)
      case NamedPatternPart(_, patternPart) =>
        // the path variables are only visible after the match
        collectVisibleVariablesOfPatternPart(patternPart)
      case PathPatternPart(element) =>
        collectVisibleVariablesOfPatternElement(element)
      case ShortestPathsPatternPart(element, _) =>
        collectVisibleVariablesOfPatternElement(element)
    }
  }

  private def collectVisibleVariablesOfPatternElement(patternElement: PatternElement): Set[LogicalVariable] = {
    patternElement match {
      case PathConcatenation(factors) =>
        factors.flatMap(pe => collectVisibleVariablesOfPatternElement(pe)).toSet
      case quantifiedPath: QuantifiedPath =>
        collectVisibleVariablesOfQuantifiedPath(quantifiedPath)
      case ParenthesizedPath(patternPart, _) =>
        collectVisibleVariablesOfPatternPart(patternPart)
      case nodePattern: NodePattern =>
        collectVisibleVariablesOfPatternAtom(nodePattern)
      case relationshipChain: RelationshipChain =>
        collectPatternAtoms(relationshipChain).flatMap(pa => collectVisibleVariablesOfPatternAtom(pa)).toSet
    }
  }

  private def collectVisibleVariablesOfPatternAtom(patternAtom: PatternAtom): Set[LogicalVariable] = {
    patternAtom match {
      case NodePattern(Some(variable), _, _, _)               => Set(variable)
      case RelationshipPattern(Some(variable), _, _, _, _, _) => Set(variable)
      case quantifiedPath: QuantifiedPath                     => collectVisibleVariablesOfQuantifiedPath(quantifiedPath)
      case _                                                  => Set.empty
    }
  }

  @inline
  private def collectVisibleVariablesOfQuantifiedPath(quantifiedPath: QuantifiedPath): Set[LogicalVariable] = {
    quantifiedPath.variableGroupings.map(_.group)
  }

  private def collectPatternAtoms(relationshipChain: RelationshipChain): Seq[PatternAtom] = {
    val RelationshipChain(leftElement, relationship, rightNodePattern) = relationshipChain
    leftElement match {
      case leftNodePattern: NodePattern => Seq(leftNodePattern, relationship, rightNodePattern)
      case leftRelationshipChain: RelationshipChain =>
        collectPatternAtoms(leftRelationshipChain) ++ Seq(relationship, rightNodePattern)
    }
  }

  private def collectColumnsFromChildren(children: Seq[PatternScope]): Seq[LogicalVariable] = {
    // keep them in order, i.e. first appearance wins
    children.foldLeft(Seq.empty[LogicalVariable]) {
      case (acc, child) => acc ++ child.result.columns.filterNot(v => acc contains v)
    }
  }

  private def collectDeclaredFromChildren(children: Seq[PatternScope]): Declarations = {
    // no duplicates in declared
    Declarations(Seq.empty, children.flatMap(_.declared.variables))
  }

  private def collectGroupsFromChildren(children: Seq[PatternScope]): Set[LogicalVariable] = {
    // no duplicates in declared
    children.foldLeft(Set.empty[LogicalVariable]) {
      case (acc, child) => acc ++ child.patternIncoming.groupConstants
    }
  }
}
