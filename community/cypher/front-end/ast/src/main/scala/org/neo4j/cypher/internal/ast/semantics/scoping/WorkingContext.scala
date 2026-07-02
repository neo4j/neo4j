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
package org.neo4j.cypher.internal.ast.semantics.scoping

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope.noLocalCallables
import org.neo4j.cypher.internal.ast.semantics.scoping.WorkingScope.unitVariables
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Ref
import org.neo4j.cypher.internal.util.helpers.LazyVal

sealed trait WorkingContext {
  def allSymbols: Set[LogicalVariable]
  def allSymbolsAndKeys: Set[LogicalVariable]
  val localCallables: Set[LocalCallableScopeSignature]
}

sealed trait RegularContext extends WorkingContext {
  val constants: Set[LogicalVariable]
  def constantSymbols: Set[LogicalVariable] = constants
  val variables: Set[LogicalVariable]
  override val localCallables: Set[LocalCallableScopeSignature]
  def projectionPart: ProjectionPart
  def getProjectionSpecification: Option[ProjectionSpecification]

  private val constantsAndVariablesLazy: LazyVal[Set[LogicalVariable]] = LazyVal(constants union variables)
  def constantsAndVariables: Set[LogicalVariable] = constantsAndVariablesLazy.value
  override def allSymbols: Set[LogicalVariable] = constantsAndVariables
  override def allSymbolsAndKeys: Set[LogicalVariable] = constantSymbols ++ variables

  @inline def isEmpty: Boolean = isVariablesEmpty && isConstantsEmpty
  @inline def isConstantsEmpty: Boolean = constants.isEmpty
  @inline def isVariablesEmpty: Boolean = variables.isEmpty

  def isConstantForPart(expr: Expression, part: ProjectionPart): Boolean
  def isSubclauseAggregation: RegularContext

  def recognizeExpression(expr: Expression, isSubExpression: Boolean): Option[ProjectionItem]

  @inline def amendedWithLocalCallable(amendment: LocalCallableScopeSignature): RegularContext =
    RegularContext(constants, variables, localCallables + amendment)

  @inline def amendedWithLocalCallables(amendment: Set[LocalCallableScopeSignature]): RegularContext =
    RegularContext(constants, variables, localCallables union amendment)

  @inline def amendedWithConstant(amendment: LogicalVariable): RegularContext =
    RegularContext(constants + amendment, variables, localCallables)

  @inline def amendedWithConstant(amendment: Set[LogicalVariable]): RegularContext =
    RegularContext(constants union amendment, variables, localCallables)

  @inline def amendedWith(amendment: LogicalVariable): RegularContext =
    RegularContext(constants, variables + amendment, localCallables)

  @inline def amendedWith(amendment: Set[LogicalVariable]): RegularContext =
    RegularContext(constants, variables union amendment, localCallables)

  @inline def shadowVariable(amendment: LogicalVariable): RegularContext =
    RegularContext(constants, (variables diff Set(amendment)) + amendment, localCallables)

  /** Add `amendment` as constants, dropping any same-name entries from `constants`/`variables` first. */
  def amendedWithShadowingConstant(amendment: Set[LogicalVariable]): RegularContext

  /** Add `amendment` as variables, dropping any same-name entries from `constants`/`variables` first. */
  def amendedWithShadowingVariables(amendment: Set[LogicalVariable]): RegularContext

  @inline def amendedWithProjectionSpecification(
    projectionSpecification: ProjectionSpecification,
    projectionPart: ProjectionPart
  ): ProjectionExpressionContext =
    ProjectionExpressionContext(constants, variables, localCallables, projectionSpecification, projectionPart)

  @inline def keepOnlyLocalCallable(): RegularContext = RegularContext(unitVariables, unitVariables, localCallables)

  @inline def constantChildContext(): RegularContext =
    RegularContext(constants union variables, unitVariables, localCallables)

  @inline def aggregatingConstantChildContext: RegularContext

  /**
   * Like [[aggregatingConstantChildContext]] in the constants/variables it produces, but for a
   * [[ProjectionExpressionContext]] this preserves the [[projectionSpecification]] and
   * [[projectionPart]]. This lets non-aggregating subclause expressions (WHERE, ORDER BY, …)
   * keep their `NonAggregatingSubclausePart` tag so the [[AggregationChecker]] can route them
   * to the inaccessible-variable check. Inside subquery expressions the boundary in
   * [[pegExpression]] still calls `constantChildContext()` and produces a plain `CommonContext`,
   * so behavior inside the subquery body is unchanged.
   */
  @inline def subclauseChildContext: RegularContext = aggregatingConstantChildContext

  @inline def replaceWith(replacement: Set[LogicalVariable]): RegularContext =
    RegularContext(constants, replacement, localCallables)

  @inline def checkIfVariablesAreAlreadyDeclaredAsConstant(
    newVariables: Iterable[LogicalVariable],
    errorFunc: (String, InputPosition) => SemanticError = SemanticError.variableShadowingOuterScope
  ): Seq[SemanticError] = {
    checkIfVariablesAreAlreadyDeclaredIn(constants, newVariables, errorFunc)
  }

  @inline def checkIfVariablesAreAlreadyDeclaredAsVariable(
    newVariables: Iterable[LogicalVariable],
    errorFunc: (String, InputPosition) => SemanticError = (n, p) => SemanticError.variableAlreadyDeclared(n, p)
  )
    : Seq[SemanticError] = {
    checkIfVariablesAreAlreadyDeclaredIn(variables, newVariables, errorFunc)
  }

  @inline def checkIfVariablesHaveMultipleDeclarations(
    newVariables: Iterable[LogicalVariable],
    errorFunc: (String, InputPosition) => SemanticError = (n, p) => SemanticError.variableAlreadyDeclared(n, p)
  )
    : Seq[SemanticError] = {
    checkIfVariablesAreAlreadyDeclaredIn(variables, newVariables, errorFunc) ++
      checkIfNameOccursTwiceInDeclared(newVariables.toSeq, errorFunc)

  }

  @inline private def checkIfVariablesAreAlreadyDeclaredIn(
    existingVariables: Set[LogicalVariable],
    newVariables: Iterable[LogicalVariable],
    errorFunc: (String, InputPosition) => SemanticError
  ): Seq[SemanticError] = {
    if (existingVariables.nonEmpty) {
      newVariables.collect {
        case variable if existingVariables contains variable =>
          errorFunc(variable.name, variable.position)
      }.toSeq
    } else {
      Seq.empty
    }
  }

  @inline private def checkIfNameOccursTwiceInDeclared(
    newVariables: Seq[LogicalVariable],
    errorFunc: (String, InputPosition) => SemanticError
  ): Seq[SemanticError] = {
    if (newVariables.nonEmpty) {
      newVariables.collect {
        case variable if newVariables.exists(v => v.name == variable.name && v.position != variable.position) =>
          errorFunc(variable.name, variable.position)
      }
    } else {
      Seq.empty
    }
  }

  @inline def resultScope(
    outgoing: RegularContext,
    result: Result,
    children: Seq[WorkingScope],
    referencedOpt: Option[References] = None,
    declared: Declarations = Declarations.noDeclarations
  )(
    implicit astNode: ASTNode
  ): StatementScope = {
    val referenced = referencedOpt.getOrElse(
      WorkingScope.referencedInChildren(children)
    )
    StatementScope(astNode, this, referenced, declared, outgoing, result, children)
  }

  @inline def forwardWithOmittedResult(children: Seq[WorkingScope] = WorkingScope.noChildren)(
    implicit astNode: ASTNode
  ): StatementScope = {
    StatementScope(
      astNode,
      incoming = this,
      referenced = WorkingScope.referencedInChildren(children),
      declared = Declarations.noDeclarations,
      outgoing = this,
      OmittedResult,
      children
    )
  }

  @inline def noResultScope(
    outgoing: RegularContext,
    children: Seq[WorkingScope],
    referencedOpt: Option[References] = None,
    declared: Declarations = Declarations.noDeclarations
  )(
    implicit astNode: ASTNode
  ): StatementScope = {
    val referenced = referencedOpt.getOrElse(
      WorkingScope.referencedInChildren(children)
    )
    StatementScope(astNode, this, referenced, declared, outgoing, NoResult, children)
  }

  @inline def omittedResultScope(
    outgoing: RegularContext,
    children: Seq[WorkingScope],
    referencedOpt: Option[References] = None,
    declared: Declarations = Declarations.noDeclarations
  )(
    implicit astNode: ASTNode
  ): StatementScope = {
    val referenced = referencedOpt.getOrElse(
      WorkingScope.referencedInChildren(children)
    )
    StatementScope(astNode, this, referenced, declared, outgoing, OmittedResult, children)
  }

  @inline def expressionResultScope(
    astNode: ASTNode,
    children: Seq[WorkingScope],
    referencedOpt: Option[References] = None,
    declared: Declarations = Declarations.noDeclarations
  ): ExpressionScope = {
    val referenced = referencedOpt.getOrElse(
      WorkingScope.referencedInChildren(children)
    )
    ExpressionScope(astNode, this, referenced, declared, children)
  }

  /**
   * Scope for an expression that is recognized as a projection item (e.g. a grouping key).
   * Its [[WorkingScope.referenced]] is set to the recognized item's alias (or, for a bare
   * variable, the variable itself) so parents see it as that column; the variables actually
   * written in the expression are preserved in [[WorkingScope.hiddenReferences]].
   */
  def recognizedLeafScope(
    expression: Expression,
    recognizedItem: ProjectionItem
  ): ExpressionScope = {
    val aliasRefs = expression match {
      case lv: LogicalVariable =>
        References.connect(lv, allSymbolsAndKeys)
      case _ =>
        recognizedItem.alias match {
          case Some(alias) => References.connect(Seq(alias), allSymbolsAndKeys)
          case None        => References.empty
        }
    }
    val scope = expressionResultScope(expression, WorkingScope.noChildren, Some(aliasRefs))
    expression match {
      case _: LogicalVariable => scope
      case other =>
        val incoming = allSymbolsAndKeys
        val innerRefs = other.folder.findAllByClass[LogicalVariable].iterator
          .flatMap(lv => incoming.find(lv.equals).map(matched => Ref(lv) -> Ref(matched)))
          .toSeq
        if (innerRefs.isEmpty) scope else scope.addHiddenReferences(innerRefs)
    }
  }
}

object RegularContext {

  def apply(
    constants: Set[LogicalVariable],
    variables: Set[LogicalVariable],
    localCallables: Set[LocalCallableScopeSignature]
  ): CommonContext =
    CommonContext(constants, variables, localCallables)

  def unit: RegularContext = RegularContext(unitVariables, unitVariables, noLocalCallables)
}

case class CommonContext(
  constants: Set[LogicalVariable],
  variables: Set[LogicalVariable],
  localCallables: Set[LocalCallableScopeSignature]
) extends RegularContext {

  override def isConstantForPart(expr: Expression, part: ProjectionPart): Boolean = expr match {
    case lv: LogicalVariable => constants(lv)
    case _                   => false
  }

  def isSubclauseAggregation: RegularContext = this

  def recognizeExpression(expr: Expression, isSubExpression: Boolean): Option[ProjectionItem] = None

  def projectionPart: ProjectionPart = NonAggregatingPart

  def getProjectionSpecification: Option[ProjectionSpecification] = None

  override def aggregatingConstantChildContext: RegularContext = constantChildContext()

  override def amendedWithShadowingConstant(amendment: Set[LogicalVariable]): RegularContext = {
    val shadowedNames = amendment.iterator.map(_.name).toSet
    copy(
      constants = constants.filterNot(c => shadowedNames.contains(c.name)) union amendment,
      variables = variables.filterNot(v => shadowedNames.contains(v.name))
    )
  }

  override def amendedWithShadowingVariables(amendment: Set[LogicalVariable]): RegularContext = {
    if (amendment.isEmpty) this
    else {
      val shadowedNames = amendment.iterator.map(_.name).toSet
      copy(
        constants = constants,
        variables = variables.filterNot(v => shadowedNames.contains(v.name)) union amendment
      )
    }
  }
}

sealed trait ProjectionPart {

  def isSubclause: Boolean = this match {
    case _: SubclausePart => true
    case _                => false
  }
}
trait ItemPart extends ProjectionPart
object AggregatingPart extends ItemPart
object NonAggregatingPart extends ItemPart
object GroupByPart extends ProjectionPart
trait SubclausePart extends ProjectionPart
object NonAggregatingSubclausePart extends SubclausePart
object AggregatingSubclausePart extends SubclausePart

case class ProjectionExpressionContext(
  constants: Set[LogicalVariable],
  variables: Set[LogicalVariable],
  localCallables: Set[LocalCallableScopeSignature],
  projectionSpecification: ProjectionSpecification,
  projectionPart: ProjectionPart
) extends RegularContext {

  override def isConstantForPart(expr: Expression, part: ProjectionPart): Boolean = {
    val partCheck: Expression => Boolean = part match {
      case _: SubclausePart   => x => projectionSpecification.isSubclauseRecognizable(x)
      case AggregatingPart    => x => projectionSpecification.isAggregationRecognizable(x)
      case NonAggregatingPart => x => projectionSpecification.isNonAggregatingRecognizable(x)
      case _                  => _ => false
    }

    expr match {
      case lv: LogicalVariable => constants(lv) || partCheck(lv)
      case expr                => partCheck(expr)
    }
  }

  def isSubclauseAggregation: RegularContext =
    copy(variables = variables union projectionSpecification.aliases, projectionPart = AggregatingSubclausePart)

  def getProjectionSpecification: Option[ProjectionSpecification] = Some(projectionSpecification)

  def recognizeExpression(that: Expression, isSubExpression: Boolean): Option[ProjectionItem] = {
    projectionPart match {
      case _ if !projectionSpecification.isAggregating => None
      case AggregatingPart =>
        projectionSpecification.recognizeInAggregation(that)
      case _: SubclausePart =>
        projectionSpecification.recognizeInSubclause(that, isSubExpression)
      case NonAggregatingPart =>
        projectionSpecification.recognizeInNonAggregatingItem(that, isSubExpression)
      case _ => None
    }

  }

  override def amendedWithShadowingConstant(amendment: Set[LogicalVariable]): RegularContext = {
    val shadowedNames = amendment.iterator.map(_.name).toSet
    copy(
      constants = constants.filterNot(c => shadowedNames.contains(c.name)) union amendment,
      variables = variables.filterNot(v => shadowedNames.contains(v.name)),
      projectionSpecification = projectionSpecification.shadowGroupingKeys(shadowedNames)
    )
  }

  override def amendedWithShadowingVariables(amendment: Set[LogicalVariable]): RegularContext = {
    if (amendment.isEmpty) this
    else {
      val shadowedNames = amendment.iterator.map(_.name).toSet
      copy(
        constants = constants,
        variables = variables.filterNot(v => shadowedNames.contains(v.name)) union amendment,
        projectionSpecification = projectionSpecification.shadowGroupingKeys(shadowedNames)
      )
    }
  }

  def groupByContext(): ProjectionExpressionContext = {
    val visibleSymbols = constants ++ variables ++ projectionSpecification.aliases
    ProjectionExpressionContext(visibleSymbols, Set.empty, localCallables, projectionSpecification, projectionPart)
  }

  def nonAggregatingChildContext(): ProjectionExpressionContext = {
    val hasExplicitGroupingKeys = projectionSpecification.hasExplicitKeys
    ProjectionExpressionContext(
      if (hasExplicitGroupingKeys) constants else constants union variables,
      unitVariables,
      localCallables,
      projectionSpecification,
      NonAggregatingPart
    )
  }

  def projectionChildContext(): ProjectionExpressionContext = {
    val (childConstants, childVariables) =
      (projectionSpecification.isAggregating, projectionPart.isSubclause) match {
        case (false, false) =>
          (constants union variables, unitVariables)
        case (false, true) =>
          (projectionSpecification.shadowSubclauseSymbols(constants union variables), unitVariables)
        case (true, false) =>
          (constants, variables)
        case (true, true) =>
          (projectionSpecification.shadowSubclauseSymbols(constants), unitVariables)
      }

    ProjectionExpressionContext(
      childConstants,
      childVariables,
      localCallables,
      projectionSpecification,
      projectionPart
    )
  }

  override def amendedWithConstant(amendment: LogicalVariable): RegularContext =
    ProjectionExpressionContext(
      constants + amendment,
      variables,
      localCallables,
      projectionSpecification,
      projectionPart
    )

  override def amendedWithConstant(amendment: Set[LogicalVariable]): RegularContext =
    ProjectionExpressionContext(
      constants union amendment,
      variables,
      localCallables,
      projectionSpecification,
      projectionPart
    )

  override def amendedWith(amendment: LogicalVariable): RegularContext =
    ProjectionExpressionContext(
      constants,
      variables + amendment,
      localCallables,
      projectionSpecification,
      projectionPart
    )

  override def amendedWith(amendment: Set[LogicalVariable]): RegularContext =
    ProjectionExpressionContext(
      constants,
      variables union amendment,
      localCallables,
      projectionSpecification,
      projectionPart
    )

  /**
   * Inside an aggregating function's arguments we operate using the incoming constants and variables.
   * No recognition of grouping keys available here thus we return to the CommonContext.
   */
  override def aggregatingConstantChildContext: RegularContext = {
    val newConstants = projectionPart match {
      case NonAggregatingSubclausePart =>
        constants union projectionSpecification.aliases
      case AggregatingSubclausePart =>
        constants union variables union projectionSpecification.aliases
      case _ =>
        constants union variables
    }

    CommonContext(newConstants, unitVariables, localCallables)
  }

  /**
   * Same fold as [[aggregatingConstantChildContext]] but preserves the [[projectionSpecification]]
   * and [[projectionPart]] tags. Used for non-aggregating subclause expressions on Cypher 25 so the
   * [[AggregationChecker]] can recognise the resulting scope as a subclause.
   */
  override def subclauseChildContext: ProjectionExpressionContext = {
    val newConstants = projectionPart match {
      case NonAggregatingSubclausePart =>
        constants union projectionSpecification.aliases
      case AggregatingSubclausePart =>
        constants union variables union projectionSpecification.aliases
      case _ =>
        constants union variables
    }

    copy(constants = newConstants, variables = unitVariables)
  }

}

case class PatternIncomingContext(
  topologicalConstants: Set[LogicalVariable],
  predicateConstants: Set[LogicalVariable],
  pathConstants: Set[LogicalVariable],
  groupConstants: Set[LogicalVariable],
  override val localCallables: Set[LocalCallableScopeSignature]
) extends WorkingContext {

  override def allSymbols: Set[LogicalVariable] =
    topologicalConstants union predicateConstants union pathConstants union groupConstants

  override def allSymbolsAndKeys: Set[LogicalVariable] = allSymbols

  private def replaceWith(origin: Set[LogicalVariable], substitutes: Set[VariableGrouping]): Set[LogicalVariable] = {
    val map = substitutes.map(vg => vg.group -> vg.singleton).toMap
    origin.map(lv => map.getOrElse(lv, lv))
  }

  def replaceOccurrences(variableGroupings: Set[VariableGrouping]): PatternIncomingContext =
    copy(replaceWith(topologicalConstants, variableGroupings), replaceWith(predicateConstants, variableGroupings))

  @inline def amendedWithTopologicalConstant(amendment: LogicalVariable): PatternIncomingContext =
    PatternIncomingContext(
      topologicalConstants + amendment,
      predicateConstants,
      pathConstants,
      groupConstants,
      localCallables
    )

  @inline def amendedWithTopologicalConstants(amendment: Set[LogicalVariable]): PatternIncomingContext =
    PatternIncomingContext(
      topologicalConstants union amendment,
      predicateConstants,
      pathConstants,
      groupConstants,
      localCallables
    )

  @inline def amendedWithConstantAccordingToVersion(
    amendment: Set[LogicalVariable],
    version: CypherVersion
  ): PatternIncomingContext =
    PatternIncomingContext(
      topologicalConstants union amendment,
      // CREATE/MERGE pattern in Cypher5 where predicates of pattern part can refer to variables introduce in earlier pattern parts
      if (version == CypherVersion.Cypher5) predicateConstants union amendment else predicateConstants,
      pathConstants,
      groupConstants,
      localCallables
    )

  @inline def removePathConstants(): PatternIncomingContext =
    PatternIncomingContext(topologicalConstants, predicateConstants, unitVariables, groupConstants, localCallables)

  @inline def addPathConstants(amendment: Set[LogicalVariable]): PatternIncomingContext =
    PatternIncomingContext(
      topologicalConstants,
      predicateConstants,
      pathConstants ++ amendment,
      groupConstants,
      localCallables
    )

  @inline def addGroupConstants(amendment: Set[LogicalVariable]): PatternIncomingContext =
    PatternIncomingContext(
      topologicalConstants,
      predicateConstants,
      pathConstants,
      groupConstants ++ amendment,
      localCallables
    )

  @inline def resultScope(
    result: TableResult,
    children: Seq[WorkingScope],
    declared: Declarations,
    referencedInTopology: References = References.empty
  )(
    implicit astNode: ASTNode
  ): PatternScope = {
    val referenced =
      (WorkingScope.referencedInChildren(children) diff declared.variables.toSet) union referencedInTopology
    PatternScope(astNode, this, referenced, declared, result, children)
  }

  def toRegularContext: RegularContext =
    RegularContext(
      constants = topologicalConstants union predicateConstants union pathConstants,
      variables = unitVariables,
      localCallables = localCallables
    )
}

object PatternIncomingContext {

  def unit: PatternIncomingContext =
    PatternIncomingContext(unitVariables, unitVariables, unitVariables, unitVariables, noLocalCallables)

}
