/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.ast.connectedComponents.RichConnectedComponent
import org.neo4j.cypher.internal.ast.semantics.Scope
import org.neo4j.cypher.internal.ast.semantics.SemanticAnalysisTooling
import org.neo4j.cypher.internal.ast.semantics.SemanticCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult.error
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckResult.success
import org.neo4j.cypher.internal.ast.semantics.SemanticCheckable
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.ast.semantics.SemanticErrorDef
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticExpressionCheck.FilteringExpressions
import org.neo4j.cypher.internal.ast.semantics.SemanticFeature
import org.neo4j.cypher.internal.ast.semantics.SemanticPatternCheck
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.ast.semantics.TypeGenerator
import org.neo4j.cypher.internal.ast.semantics.traversableOnceSemanticChecking
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Ands
import org.neo4j.cypher.internal.expressions.Contains
import org.neo4j.cypher.internal.expressions.EndsWith
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.EveryPath
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.Expression.SemanticContext
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.HasLabels
import org.neo4j.cypher.internal.expressions.HasLabelsOrTypes
import org.neo4j.cypher.internal.expressions.HasTypes
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.InequalityExpression
import org.neo4j.cypher.internal.expressions.IsNotNull
import org.neo4j.cypher.internal.expressions.LabelOrRelTypeName
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.Or
import org.neo4j.cypher.internal.expressions.Ors
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.ProcedureName
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.StartsWith
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.containsAggregate
import org.neo4j.cypher.internal.expressions.functions
import org.neo4j.cypher.internal.expressions.functions.Distance
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.CartesianProductNotification
import org.neo4j.cypher.internal.util.DeprecatedStartNotification
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.helpers.StringHelper.RichString
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTBoolean
import org.neo4j.cypher.internal.util.symbols.CTFloat
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.CTList
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTNode
import org.neo4j.cypher.internal.util.symbols.CTPath
import org.neo4j.cypher.internal.util.symbols.CTRelationship
import org.neo4j.cypher.internal.util.symbols.CTString

sealed trait Clause extends ASTNode with SemanticCheckable {
  def name: String

  def returnColumns: List[LogicalVariable] = List.empty
}

sealed trait UpdateClause extends Clause with SemanticAnalysisTooling {
  override def returnColumns: List[LogicalVariable] = List.empty
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
      case _ => success
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

case class InputDataStream(variables: Seq[Variable])(val position: InputPosition) extends Clause with SemanticAnalysisTooling {

  override def name: String = "INPUT DATA STREAM"

  override def semanticCheck: SemanticCheck =
    variables.foldSemanticCheck(v => declareVariable(v, types(v)))
}

sealed trait GraphSelection extends Clause with SemanticAnalysisTooling {

  def expression: Expression

  override def semanticCheck: SemanticCheck =
    checkGraphReference chain
      whenState(_.features(SemanticFeature.ExpressionsInViewInvocations))(
        thenBranch = checkGraphReferenceExpressions,
        elseBranch = checkGraphReferenceRecursive
      )

  private def checkGraphReference: SemanticCheck =
    GraphReference.checkNotEmpty(graphReference, expression.position)

  private def checkGraphReferenceRecursive: SemanticCheck =
    graphReference.foldSemanticCheck(_.semanticCheck)

  private def checkGraphReferenceExpressions: SemanticCheck =
    graphReference.foldSemanticCheck {
      case ViewRef(_, arguments)        => checkExpressions(arguments)
      case GraphRefParameter(parameter) => checkExpressions(Seq(parameter))
      case _                            => success
    }

  private def checkExpressions(expressions: Seq[Expression]): SemanticCheck =
    whenState(_.features(SemanticFeature.ExpressionsInViewInvocations))(
      expressions.foldSemanticCheck(expr =>
        SemanticExpressionCheck.check(Expression.SemanticContext.Results, expr)
      )
    )

  def graphReference: Option[GraphReference] =
    GraphReference.from(expression)
}

final case class UseGraph(expression: Expression)(val position: InputPosition) extends GraphSelection {
  override def name = "USE GRAPH"

  override def semanticCheck: SemanticCheck =
    requireFeatureSupport(s"The `$name` clause", SemanticFeature.UseGraphSelector, position) chain
      super.semanticCheck
}

object GraphReference extends SemanticAnalysisTooling {
  def from(expression: Expression): Option[GraphReference] = {

    def fqn(expr: Expression): Option[List[String]] = expr match {
      case p: Property           => fqn(p.map).map(_ :+ p.propertyKey.name)
      case v: Variable           => Some(List(v.name))
      case f: FunctionInvocation => Some(f.namespace.parts :+ f.functionName.name)
      case _                     => None
    }

    (expression, fqn(expression)) match {
      case (f: FunctionInvocation, Some(name)) => Some(ViewRef(CatalogName(name), f.args)(f.position))
      case (p: Parameter, _)                   => Some(GraphRefParameter(p)(p.position))
      case (e, Some(name))                     => Some(GraphRef(CatalogName(name))(e.position))
      case _                                   => None
    }
  }

  def checkNotEmpty(gr: Option[GraphReference], pos: InputPosition): SemanticCheck =
    when(gr.isEmpty)(error("Invalid graph reference", pos))
}

sealed trait GraphReference extends ASTNode with SemanticCheckable {
  override def semanticCheck: SemanticCheck = success
}

final case class GraphRef(name: CatalogName)(val position: InputPosition) extends GraphReference

final case class ViewRef(name: CatalogName, arguments: Seq[Expression])(val position: InputPosition) extends GraphReference with SemanticAnalysisTooling {
  override def semanticCheck: SemanticCheck =
    arguments.zip(argumentsAsGraphReferences).foldSemanticCheck { case (arg, gr) =>
      GraphReference.checkNotEmpty(gr, arg.position)
    }

  def argumentsAsGraphReferences: Seq[Option[GraphReference]] =
    arguments.map(GraphReference.from)
}

final case class GraphRefParameter(parameter: Parameter)(val position: InputPosition) extends GraphReference

trait SingleRelTypeCheck {
  self: Clause =>

  protected def checkRelTypes(pattern: Pattern): SemanticCheck =
    pattern.patternParts.foldSemanticCheck {
      case EveryPath(element) => checkRelTypes(element)
      case _ => success
    }

  private def checkRelTypes(patternElement: PatternElement): SemanticCheck = {
    patternElement match {
      case RelationshipChain(element, rel, _) =>
        checkRelTypes(rel) chain checkRelTypes(element)
      case _ => success
    }
  }

  protected def checkRelTypes(rel: RelationshipPattern): SemanticCheck = {
    if (rel.types.size != 1) {
      if (rel.types.size > 1) {
        SemanticError(s"A single relationship type must be specified for ${self.name}", rel.position)
      } else {
        SemanticError(s"Exactly one relationship type must be specified for ${self.name}. Did you forget to prefix your relationship type with a ':'?", rel.position)
      }
    } else success
  }
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
      case AllNodes(variable) => s"MATCH (${variable.asCanonicalStringVal})"
      case AllRelationships(variable) => s"MATCH ()-[${variable.asCanonicalStringVal}]->()"
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
      checkForCartesianProducts

  private def uniqueHints: SemanticCheck = {
    val errors = hints.groupBy {
      case h: UsingIndexHint => ("index", h.variables.toIndexedSeq)
      case h: UsingScanHint => ("index", h.variables.toIndexedSeq)
      case h: UsingJoinHint => ("join", h.variables.toIndexedSeq)
    }.collect {
      case ((category, variables), identHints) if identHints.size > 1 =>
        SemanticError(s"Multiple $category hints for same variable are not supported", variables.head.position)
    }.toVector

    state: SemanticState => semantics.SemanticCheckResult(state, errors)
  }

  private def checkForCartesianProducts: SemanticCheck = (state: SemanticState) => {
    val cc = connectedComponents(pattern.patternParts)
    //if we have multiple connected components we will have
    //a cartesian product
    val newState = cc.drop(1).foldLeft(state) { (innerState, component) =>
      innerState.addNotification(CartesianProductNotification(position, component.variables.map(_.name)))
    }

    semantics.SemanticCheckResult(newState, Seq.empty)
  }

  private def checkHints: SemanticCheck = {
    val error: Option[SemanticCheck] = hints.collectFirst {
      case hint@UsingIndexHint(Variable(variable), LabelOrRelTypeName(labelOrRelTypeName), _, _)
        if !containsLabelOrRelTypePredicate(variable, labelOrRelTypeName) =>
        SemanticError(
          s"""|Cannot use index hint in this context.
              | Must use label/relationship type '$labelOrRelTypeName' on node/relationship '$variable'
              | that this hint is referring to.
              | Did you choose the wrong variable or the wrong label/relationship type?""".stripLinesAndMargins, hint.position)
      case hint@UsingIndexHint(Variable(variable), LabelOrRelTypeName(_), properties, _)
        if !containsPropertyPredicates(variable, properties) =>
        SemanticError(
          """|Cannot use index hint in this context.
             | Index hints are only supported for the following predicates in WHERE
             | (either directly or as part of a top-level AND or OR):
             | equality comparison, inequality (range) comparison, STARTS WITH,
             | IN condition or checking property existence.
             | The comparison cannot be performed between two property values.
             | Note that the label/relationship type and property comparison must be specified on a
             | non-optional node/relationship.""".stripLinesAndMargins, hint.position)
      case hint@UsingScanHint(Variable(variable), LabelOrRelTypeName(labelName))
        if !containsLabelOrRelTypePredicate(variable, labelName) =>
        SemanticError(
          """|Cannot use label/relationship type scan hint in this context.
             | Label/relationship type scan hints require using a simple label/relationship type test in WHERE (either directly or as part of a
             | top-level AND). Note that the label/relationship type must be specified on a non-optional node/relationship.""".stripLinesAndMargins, hint.position)
      case hint@UsingJoinHint(_)
        if pattern.length == 0 =>
        SemanticError("Cannot use join hint for single node pattern.", hint.position)
    }
    error.getOrElse(success)
  }

  private[ast] def containsPropertyPredicates(variable: String, propertiesInHint: Seq[PropertyKeyName]): Boolean = {
    val propertiesInPredicates: Seq[String] = (where match {
      case Some(w) => w.treeFold(Seq.empty[String]) {
        case Equals(Property(Variable(id), PropertyKeyName(name)), other) if id == variable && applicable(other) =>
          acc => SkipChildren(acc :+ name)
        case Equals(other, Property(Variable(id), PropertyKeyName(name))) if id == variable && applicable(other) =>
          acc => SkipChildren(acc :+ name)
        case In(Property(Variable(id), PropertyKeyName(name)), _) if id == variable =>
          acc => SkipChildren(acc :+ name)
        case predicate@FunctionInvocation(_, _, _, IndexedSeq(Property(Variable(id), PropertyKeyName(name))))
          if id == variable && predicate.function == Exists =>
          acc => SkipChildren(acc :+ name)
        case IsNotNull(Property(Variable(id), PropertyKeyName(name))) if id == variable =>
          acc => SkipChildren(acc :+ name)
        case StartsWith(Property(Variable(id), PropertyKeyName(name)), _) if id == variable =>
          acc => SkipChildren(acc :+ name)
        case EndsWith(Property(Variable(id), PropertyKeyName(name)), _) if id == variable =>
          acc => SkipChildren(acc :+ name)
        case Contains(Property(Variable(id), PropertyKeyName(name)), _) if id == variable =>
          acc => SkipChildren(acc :+ name)
        case expr: InequalityExpression =>
          acc =>
            val newAcc: Seq[String] = Seq(expr.lhs, expr.rhs).foldLeft(acc) { (acc, expr) =>
              expr match {
                case Property(Variable(id), PropertyKeyName(name)) if id == variable =>
                  acc :+ name
                case FunctionInvocation(Namespace(List()), FunctionName(Distance.name), _, Seq(Property(Variable(id), PropertyKeyName(name)), _)) if id == variable =>
                  acc :+ name
                case _ =>
                  acc
              }
            }
            SkipChildren(newAcc)
        case _: Where | _: And | _: Ands | _: Set[_] | _: Seq[_] | _: Or | _: Ors =>
          acc => TraverseChildren(acc)
        case _ =>
          acc => SkipChildren(acc)
      }
      case None => Seq.empty
    }) ++ pattern.treeFold(Seq.empty[String]) {
      case NodePattern(Some(Variable(id)), _, Some(MapExpression(prop))) if variable == id =>
        acc => SkipChildren(acc ++ prop.map(_._1.name))
      case RelationshipPattern(Some(Variable(id)), _, _, Some(MapExpression(prop)), _, _) if variable == id =>
        acc => SkipChildren(acc ++ prop.map(_._1.name))
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

  private[ast] def containsLabelOrRelTypePredicate(variable: String, labelOrRelType: String): Boolean = {
    val inlinedLabels = pattern.fold(Seq.empty[String]) {
      case NodePattern(Some(Variable(id)), nodeLabels, _) if variable == id =>
        list => list ++ nodeLabels.map(_.name)
    }
    val inlinedRelTypes = pattern.fold(Seq.empty[String]) {
      case RelationshipPattern(Some(Variable(id)), types, _, _, _, _) if variable == id =>
        list => list ++ types.map(_.name)
    }
    val (predicateLabels, predicateRelTypes) = where match {
      case Some(innerWhere) => innerWhere.treeFold((Seq.empty[String], Seq.empty[String])) {
        case HasLabels(Variable(id), predicateLabels) if id == variable => {
          case (ls, rs) => SkipChildren((ls ++ predicateLabels.map(_.name), rs))
        }
        case HasLabelsOrTypes(Variable(id), predicateLabelsOrRelTypes) if id == variable => {
          case (ls, rs) => SkipChildren((ls ++ predicateLabelsOrRelTypes.map(_.name), rs  ++ predicateLabelsOrRelTypes.map(_.name)))
        }
        case HasTypes(Variable(id), predicateRelTypes) if id == variable => {
          case (ls, rs) => SkipChildren((ls, rs ++ predicateRelTypes.map(_.name)))
        }
        case _: Where | _: And | _: Ands | _: Set[_] | _: Seq[_] =>
          acc => TraverseChildren(acc)
        case _ =>
          acc => SkipChildren(acc)
      }
      case None => (inlinedLabels, inlinedRelTypes)
    }
    val allLabels = inlinedLabels ++ predicateLabels
    val allRelTypes = inlinedRelTypes ++ predicateRelTypes
    allLabels.contains(labelOrRelType) || allRelTypes.contains(labelOrRelType)
  }

  def allExportedVariables: Set[LogicalVariable] = pattern.patternParts.findByAllClass[LogicalVariable].toSet
}

sealed trait CommandClause extends Clause with SemanticAnalysisTooling {
  def unfilteredColumns: DefaultOrAllShowColumns
  override def semanticCheck: SemanticCheck = semanticCheckFold(unfilteredColumns.columns)(sc => declareVariable(sc.variable, sc.cypherType))

  def where: Option[Where]

  def moveWhereToYield: CommandClause
}

object CommandClause {
  def unapply(cc: CommandClause): Option[(Set[ShowColumn], Option[Where])] = Some((cc.unfilteredColumns.columns, cc.where))
}

case class DefaultOrAllShowColumns(useAllColumns: Boolean, brief: Set[ShowColumn], verbose: Set[ShowColumn]) {
  def columns: Set[ShowColumn] = if (useAllColumns) brief ++ verbose else brief
}

// For a query to be allowed to run on system it needs to consist of:
// - only ClauseAllowedOnSystem clauses
// - at least one CommandClauseAllowedOnSystem clause
sealed trait ClauseAllowedOnSystem
sealed trait CommandClauseAllowedOnSystem extends ClauseAllowedOnSystem

case class ShowIndexesClause(unfilteredColumns: DefaultOrAllShowColumns, indexType: ShowIndexType, brief: Boolean, verbose: Boolean, where: Option[Where], hasYield: Boolean)
                            (val position: InputPosition) extends CommandClause {
  override def name: String = "SHOW INDEXES"

  override def moveWhereToYield: CommandClause = copy(where = None, hasYield = true)(position)
}

object ShowIndexesClause {
  def apply(indexType: ShowIndexType, brief: Boolean, verbose: Boolean, where: Option[Where], hasYield: Boolean)(position: InputPosition): ShowIndexesClause = {
    val briefCols = Set(
      ShowColumn("id", CTInteger)(position),
      ShowColumn("name")(position),
      ShowColumn("state")(position),
      ShowColumn("populationPercent", CTFloat)(position),
      ShowColumn("uniqueness")(position),
      ShowColumn("type")(position),
      ShowColumn("entityType")(position),
      ShowColumn("labelsOrTypes", CTList(CTString))(position),
      ShowColumn("properties", CTList(CTString))(position),
      ShowColumn("indexProvider")(position)
    )
    val verboseCols = Set(
      ShowColumn("options", CTMap)(position),
      ShowColumn("failureMessage")(position),
      ShowColumn("createStatement")(position)
    )

    ShowIndexesClause(DefaultOrAllShowColumns(hasYield | verbose, briefCols, verboseCols), indexType, brief, verbose, where, hasYield)(position)
  }
}

case class ShowConstraintsClause(unfilteredColumns: DefaultOrAllShowColumns, constraintType: ShowConstraintType, brief: Boolean, verbose: Boolean, where: Option[Where], hasYield: Boolean)
                            (val position: InputPosition) extends CommandClause {
  override def name: String = "SHOW CONSTRAINTS"

  override def moveWhereToYield: CommandClause = copy(where = None, hasYield = true)(position)
}

object ShowConstraintsClause {
  def apply(constraintType: ShowConstraintType, brief: Boolean, verbose: Boolean, where: Option[Where], hasYield: Boolean)(position: InputPosition): ShowConstraintsClause = {
    val briefCols = Set(
      ShowColumn("id", CTInteger)(position),
      ShowColumn("name")(position),
      ShowColumn("type")(position),
      ShowColumn("entityType")(position),
      ShowColumn("labelsOrTypes", CTList(CTString))(position),
      ShowColumn("properties", CTList(CTString))(position),
      ShowColumn("ownedIndexId", CTInteger)(position)
    )
    val verboseCols = Set(
      ShowColumn("options", CTMap)(position),
      ShowColumn("createStatement")(position)
    )

    ShowConstraintsClause(DefaultOrAllShowColumns(hasYield | verbose, briefCols, verboseCols), constraintType, brief, verbose, where, hasYield)(position)
  }
}

case class ShowProceduresClause(unfilteredColumns: DefaultOrAllShowColumns, executable: Option[ExecutableBy], where: Option[Where], hasYield: Boolean)
                               (val position: InputPosition) extends CommandClause with CommandClauseAllowedOnSystem {
  override def name: String = "SHOW PROCEDURES"

  override def moveWhereToYield: CommandClause = copy(where = None, hasYield = true)(position)
}

object ShowProceduresClause {
  def apply(executable: Option[ExecutableBy], where: Option[Where], hasYield: Boolean)(position: InputPosition): ShowProceduresClause = {
    val briefCols = Set(
      ShowColumn("name")(position),
      ShowColumn("description")(position),
      ShowColumn("mode")(position),
      ShowColumn("worksOnSystem", CTBoolean)(position),
    )
    val verboseCols = Set(
      ShowColumn("signature")(position),
      ShowColumn("argumentDescription", CTList(CTMap))(position),
      ShowColumn("returnDescription", CTList(CTMap))(position),
      ShowColumn("admin", CTBoolean)(position),
      ShowColumn("rolesExecution", CTList(CTString))(position),
      ShowColumn("rolesBoostedExecution", CTList(CTString))(position),
      ShowColumn("option", CTMap)(position)
    )

    ShowProceduresClause(DefaultOrAllShowColumns(hasYield, briefCols, verboseCols), executable, where, hasYield)(position)
  }
}

case class ShowFunctionsClause(unfilteredColumns: DefaultOrAllShowColumns, functionType: ShowFunctionType, executable: Option[ExecutableBy], where: Option[Where], hasYield: Boolean)
                               (val position: InputPosition) extends CommandClause with CommandClauseAllowedOnSystem {
  override def name: String = "SHOW FUNCTIONS"

  override def moveWhereToYield: CommandClause = copy(where = None, hasYield = true)(position)
}

object ShowFunctionsClause {
  def apply(functionType: ShowFunctionType, executable: Option[ExecutableBy], where: Option[Where], hasYield: Boolean)(position: InputPosition): ShowFunctionsClause = {
    val briefCols = Set(
      ShowColumn("name")(position),
      ShowColumn("category")(position),
      ShowColumn("description")(position),
    )
    val verboseCols = Set(
      ShowColumn("signature")(position),
      ShowColumn("isBuiltIn", CTBoolean)(position),
      ShowColumn("argumentDescription", CTList(CTMap))(position),
      ShowColumn("returnDescription")(position),
      ShowColumn("aggregating", CTBoolean)(position),
      ShowColumn("rolesExecution", CTList(CTString))(position),
      ShowColumn("rolesBoostedExecution", CTList(CTString))(position)
    )

    ShowFunctionsClause(DefaultOrAllShowColumns(hasYield, briefCols, verboseCols), functionType, executable, where, hasYield)(position)
  }
}

case class Merge(pattern: Pattern, actions: Seq[MergeAction], where: Option[Where] = None)(val position: InputPosition)
  extends UpdateClause with SingleRelTypeCheck {

  override def name = "MERGE"

  override def semanticCheck: SemanticCheck =
    SemanticPatternCheck.check(Pattern.SemanticContext.Merge, pattern) chain
      actions.semanticCheck chain
      checkRelTypes(pattern)
}

case class Create(pattern: Pattern)(val position: InputPosition) extends UpdateClause with SingleRelTypeCheck {
  override def name = "CREATE"

  override def semanticCheck: SemanticCheck =
    SemanticPatternCheck.check(Pattern.SemanticContext.Create, pattern) chain
      checkRelTypes(pattern)
}

case class CreateUnique(pattern: Pattern)(val position: InputPosition) extends UpdateClause {
  override def name = "CREATE UNIQUE"

  override def semanticCheck =
    SemanticError("CREATE UNIQUE is no longer supported. Please use MERGE instead", position)

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
    expressions.filter(e => e.isInstanceOf[HasLabels] || e.isInstanceOf[HasLabelsOrTypes]) map {
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
      expectType(CTList(CTAny).covariant | CTAny.covariant, expression) ifOkChain
      FilteringExpressions.failIfAggregating(expression) chain {
        val possibleInnerTypes: TypeGenerator = types(expression)(_).unwrapPotentialLists
        declareVariable(variable, possibleInnerTypes)
      }
}

abstract class CallClause extends Clause {
  override def name = "CALL"

  def returnColumns: List[LogicalVariable]

  def containsNoUpdates: Boolean
}

case class UnresolvedCall(procedureNamespace: Namespace,
                          procedureName: ProcedureName,
                          // None: No arguments given
                          declaredArguments: Option[Seq[Expression]] = None,
                          // None: No results declared  (i.e. no "YIELD" part or "YIELD *")
                          declaredResult: Option[ProcedureResult] = None,
                          // YIELD *
                          yieldAll: Boolean = false
                         )(val position: InputPosition) extends CallClause {

  override def returnColumns: List[LogicalVariable] =
    declaredResult.map(_.items.map(_.variable).toList).getOrElse(List.empty)

  override def semanticCheck: SemanticCheck = {
    val argumentCheck = declaredArguments.map(
      SemanticExpressionCheck.check(SemanticContext.Results, _, Seq())).getOrElse(success)
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

object ProjectionClause {

  def unapply(arg: ProjectionClause): Option[(Boolean, ReturnItems, Option[OrderBy], Option[Skip], Option[Limit], Option[Where])] = {
    arg match {
      case With(distinct, ri, orderBy, skip, limit, where) => Some((distinct, ri, orderBy, skip, limit, where))
      case Return(distinct, ri, orderBy, skip, limit, _) => Some((distinct, ri, orderBy, skip, limit, None))
      case Yield(ri, orderBy, skip, limit, where) => Some((false, ri, orderBy, skip, limit, where))
    }
  }

  def checkAliasedReturnItems(returnItems: ReturnItems, clauseName: String): SemanticState => Seq[SemanticError] =
    state => returnItems match {
      case li: ReturnItems =>
        li.items.filter(item => item.alias.isEmpty).map(i => SemanticError(s"Expression in $clauseName must be aliased (use AS)", i.position))
      case _ => Seq()
    }
}

sealed trait ProjectionClause extends HorizonClause {
  def distinct: Boolean

  def returnItems: ReturnItems

  def orderBy: Option[OrderBy]

  def where: Option[Where]

  def skip: Option[Skip]

  def limit: Option[Limit]

  final def isWith: Boolean = !isReturn

  def isReturn: Boolean = false

  def copyProjection(distinct: Boolean = this.distinct,
                     returnItems: ReturnItems = this.returnItems,
                     orderBy: Option[OrderBy] = this.orderBy,
                     skip: Option[Skip] = this.skip,
                     limit: Option[Limit] = this.limit,
                     where: Option[Where] = this.where): ProjectionClause = {
    this match {
      case w:With => w.copy(distinct, returnItems, orderBy, skip, limit, where)(this.position)
      case r:Return => r.copy(distinct, returnItems, orderBy, skip, limit, r.excludedNames)(this.position)
      case y:Yield => y.copy(returnItems, orderBy, skip, limit, where)(this.position)
    }
  }

  /**
   * @return copy of this ProjectionClause with new return items
   */
  def withReturnItems(items: Seq[ReturnItem]): ProjectionClause

  override def semanticCheck: SemanticCheck =
    returnItems.semanticCheck

  override def semanticCheckContinuation(previousScope: Scope): SemanticCheck = {
    state =>

      def runChecks(scopeInUse: Scope): SemanticCheck = innerState => (
        returnItems.declareVariables(scopeInUse) chain
          orderBy.semanticCheck chain
          checkSkip chain
          checkLimit chain
          where.semanticCheck) (innerState)

      // The two clauses ORDER BY and WHERE, following a WITH clause where there is no DISTINCT nor aggregation, have a special scope such that they
      // can see both variables from before the WITH and variables introduced by the WITH
      // (SKIP and LIMIT clauses are not allowed to access variables anyway, so they do not need to be included in this condition even when they are standalone)
      val specialScopeForSubClausesNeeded = orderBy.isDefined || where.isDefined
      val canSeePreviousScope = (!(returnItems.containsAggregate || distinct || isInstanceOf[Yield])) || returnItems.includeExisting

      if (specialScopeForSubClausesNeeded && canSeePreviousScope) {
        /*
         * We have `WITH ... WHERE` or `WITH ... ORDER BY` with no aggregation nor distinct meaning we can
         *  see things from previous scopes when we are done here
         *  (incoming-scope)
         *        |      \
         *        |     (child scope) <-  semantic checking of `ORDER BY` and `WHERE` discarded, only used for errors
         *        |
         *  (outgoing-scope)
         *        |
         *       ...
         */

        // Special scope for ORDER BY and WHERE (SKIP and LIMIT are also checked in isolated scopes)
        val stateForSubClauses = state.newChildScope

        val SemanticCheckResult(nextState, errors1) = runChecks(previousScope)(stateForSubClauses)

        // New sibling scope for the WITH/RETURN clause itself and onwards.
        // Re-declare projected variables in the new scope since the sub-scope is discarded
        // (We do not need to check warnOnAccessToRestrictedVariableInOrderByOrWhere here since that only applies when we have distinct or aggregation)
        val returnState = nextState.popScope.newSiblingScope
        val SemanticCheckResult(finalState, errors2) = returnItems.declareVariables(state.currentScope.scope)(returnState)
        SemanticCheckResult(finalState, errors1 ++ errors2)
      } else if (specialScopeForSubClausesNeeded) {
        /*
         *  We have `WITH ... WHERE` or `WITH ... ORDER BY` with an aggregation or a distinct meaning we cannot
         *  see things from previous scopes after the aggregation (or distinct).
         *
         *  (incoming-scope)
         *         |
         *  (outgoing-scope)
         *         |      \
         *         |      (child-scope) <- semantic checking of `ORDER BY` and `WHERE` discarded only used for errors
         *        ...
         */

        //Introduce a new sibling scope first, and then a new child scope from that one
        //this child scope is used for errors only and will later be discarded.
        val siblingState = state.newSiblingScope
        val stateForSubClauses = siblingState.newChildScope
        val SemanticCheckResult(nextState, errors1) = runChecks(siblingState.currentScope.scope)(stateForSubClauses)

        //By popping the scope we will discard the special scope used for subclauses
        val returnState = nextState.popScope

        // Re-declare projected variables in the new scope since the sub-scope is discarded
        val SemanticCheckResult(finalState, errors2) = returnItems.declareVariables(returnState.currentScope.scope)(returnState)
        val niceErrors = (errors1 ++ errors2).map(warnOnAccessToRestrictedVariableInOrderByOrWhere(state.currentScope.symbolNames))
        SemanticCheckResult(finalState, niceErrors)
      } else {
        val returnState = state.newSiblingScope
        val SemanticCheckResult(finalState, errors) = runChecks(previousScope)(returnState)
        val niceErrors = errors.map(warnOnAccessToRestrictedVariableInOrderByOrWhere(state.currentScope.symbolNames))
        SemanticCheckResult(finalState, niceErrors)
      }
  }

  /**
   * If you access a previously defined variable in a WITH/RETURN with DISTINCT or aggregation, that is not OK. Example:
   * MATCH (a) RETURN sum(a.age) ORDER BY a.name
   *
   * This method takes the "Variable not defined" errors we get from the semantic analysis and provides a more helpful
   * error message
   * @param previousScopeVars all variables defined in the previous scope.
   * @param error the error
   * @return an error with a possibly better error message
   */
  private[ast] def warnOnAccessToRestrictedVariableInOrderByOrWhere(previousScopeVars: Set[String])(error: SemanticErrorDef): SemanticErrorDef = {
    previousScopeVars.collectFirst {
      case name if error.msg.equals(s"Variable `$name` not defined") => error.withMsg(
        s"In a WITH/RETURN with DISTINCT or an aggregation, it is not possible to access variables declared before the WITH/RETURN: $name")
    }.getOrElse(error)
  }

  // use an empty state when checking skip & limit, as these have entirely isolated context
  private def checkSkip: SemanticState => Seq[SemanticErrorDef] =
    _ => skip.semanticCheck(SemanticState.clean).errors

  private def checkLimit: SemanticState => Seq[SemanticErrorDef] =
    _ => limit.semanticCheck(SemanticState.clean).errors

  def verifyOrderByAggregationUse(fail: (String, InputPosition) => Nothing): Unit = {
    val aggregationInProjection = returnItems.containsAggregate
    val aggregationInOrderBy = orderBy.exists(_.sortItems.map(_.expression).exists(containsAggregate))
    if (!aggregationInProjection && aggregationInOrderBy)
      fail(s"Cannot use aggregation in ORDER BY if there are no aggregate expressions in the preceding $name", position)
  }
}

object With {
  def apply(returnItems: ReturnItems)(pos: InputPosition): With =
    With(distinct = false, returnItems, None, None, None, None)(pos)
}

case class With(distinct: Boolean,
                returnItems: ReturnItems,
                orderBy: Option[OrderBy],
                skip: Option[Skip],
                limit: Option[Limit],
                where: Option[Where])(val position: InputPosition) extends ProjectionClause {

  override def name = "WITH"

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      ProjectionClause.checkAliasedReturnItems(returnItems, "WITH") chain
      SemanticPatternCheck.checkValidPropertyKeyNamesInReturnItems(returnItems, this.position)

  override def withReturnItems(items: Seq[ReturnItem]): With =
    this.copy(returnItems = ReturnItems(returnItems.includeExisting, items)(returnItems.position))(this.position)
}

object Return {
  def apply(returnItems: ReturnItems)(pos: InputPosition): Return =
    Return(distinct = false, returnItems, None, None, None)(pos)
}

case class Return(distinct: Boolean,
                  returnItems: ReturnItems,
                  orderBy: Option[OrderBy],
                  skip: Option[Skip],
                  limit: Option[Limit],
                  excludedNames: Set[String] = Set.empty)(val position: InputPosition) extends ProjectionClause with ClauseAllowedOnSystem {

  override def name = "RETURN"

  override def isReturn: Boolean = true

  override def where: Option[Where] = None

  override def returnColumns: List[LogicalVariable] = returnItems.items.flatMap(_.alias).toList

  override def semanticCheck: SemanticCheck =
    super.semanticCheck chain
      checkVariableScope chain
      ProjectionClause.checkAliasedReturnItems(returnItems, "CALL { RETURN ... }") chain
      SemanticPatternCheck.checkValidPropertyKeyNamesInReturnItems(returnItems, this.position)

  override def withReturnItems(items: Seq[ReturnItem]): Return =
    this.copy(returnItems = ReturnItems(returnItems.includeExisting, items)(returnItems.position))(this.position)

  def withReturnItems(returnItems: ReturnItems): Return =
    this.copy(returnItems = returnItems)(this.position)


  private def checkVariableScope: SemanticState => Seq[SemanticError] = s =>
    returnItems match {
      case ReturnItems(star, _) if star && s.currentScope.isEmpty =>
        Seq(SemanticError("RETURN * is not allowed when there are no variables in scope", position))
      case _ =>
        Seq.empty
    }
}

case class Yield(returnItems: ReturnItems,
                 orderBy: Option[OrderBy],
                 skip: Option[Skip],
                 limit: Option[Limit],
                 where: Option[Where])(val position: InputPosition) extends ProjectionClause with ClauseAllowedOnSystem {
  override def distinct: Boolean = false

  override def name: String = "YIELD"

  override def withReturnItems(items: Seq[ReturnItem]): Yield =
    this.copy(returnItems = ReturnItems(returnItems.includeExisting, items)(returnItems.position))(this.position)

  override def warnOnAccessToRestrictedVariableInOrderByOrWhere(previousScopeVars: Set[String])
                                                               (error: SemanticErrorDef): SemanticErrorDef = error
}

case class SubQuery(part: QueryPart)(val position: InputPosition) extends HorizonClause with SemanticAnalysisTooling {

  override def name: String = "CALL"

  override def semanticCheck: SemanticCheck =
      checkSubquery

  def checkSubquery: SemanticCheck = { outer: SemanticState =>

    val SemanticCheckResult(outerStateWithImports, importingWithErrors) = part.checkImportingWith(outer)

    // Create empty scope under root
    val empty: SemanticState = outerStateWithImports.newBaseScope

    // Check inner query. Allow it to import from outer scope
    val inner: SemanticCheckResult = part.semanticCheckInSubqueryContext(outerStateWithImports)(empty)
    val innerCurrentScope = inner.state.currentScope.scope

    // Keep working from the latest state
    val after: SemanticState = inner.state
      // but jump back to scope tree of outerStateWithImports
      .copy(currentScope = outerStateWithImports.currentScope)
      // Copy in the scope tree from inner query (needed for Namespacer)
      .insertSiblingScope(innerCurrentScope)
      // Import variables from scope before subquery
      .newSiblingScope
      .importValuesFromScope(outerStateWithImports.currentScope.scope)

    // Declare variables that are in output from subquery
    val merged = mergeOutputVariablesIntoOuterScope(innerCurrentScope)(after)

    // Keep errors from inner check and from variable declarations
    SemanticCheckResult(merged.state, importingWithErrors ++ inner.errors ++ merged.errors)
  }

  override def semanticCheckContinuation(previousScope: Scope): SemanticCheck = { s =>
    SemanticCheckResult(s.importValuesFromScope(previousScope), Vector())
  }

  private def mergeOutputVariablesIntoOuterScope(rootScope: Scope): SemanticCheck = {
    val scopeForValidation = part.scopeForReturnVariablesValidation(rootScope)
    val scopeForDeclaringVariables = part.finalScope(rootScope)

    checkReturnVariablesNotDeclaredInOuterScope(scopeForValidation) ifOkChain
      declareVariables(scopeForDeclaringVariables.symbolTable.values)
  }

  private def checkReturnVariablesNotDeclaredInOuterScope(outputScope: Scope): SemanticCheck = { s =>
    val outerScopeSymbolNames = s.currentScope.symbolNames
    val outputSymbols = outputScope.symbolTable.values.toSeq
    val errors = outputSymbols.collect {
      case symbol if outerScopeSymbolNames.contains(symbol.name) =>
        val lastUsePosition = symbol.positions.max
        SemanticError(s"Variable `${symbol.name}` already declared in outer scope", lastUsePosition)
    }
    SemanticCheckResult(s, errors)
  }
}
