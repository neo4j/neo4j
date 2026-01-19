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

import org.neo4j.cypher.internal.ast.Clause
import org.neo4j.cypher.internal.ast.CommandClause
import org.neo4j.cypher.internal.ast.ConditionalQueryBranch
import org.neo4j.cypher.internal.ast.ConditionalQueryWhen
import org.neo4j.cypher.internal.ast.CreateOrInsert
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.ProjectionClause
import org.neo4j.cypher.internal.ast.Return
import org.neo4j.cypher.internal.ast.ReturnItem
import org.neo4j.cypher.internal.ast.ScopeClauseSubqueryCall
import org.neo4j.cypher.internal.ast.Union
import org.neo4j.cypher.internal.ast.semantics.SemanticError
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NamedPatternPart
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.expressions.PatternElement
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.InputPosition

trait VariableCheckerUtil {

  protected type VariableCheck = PartialFunction[(Acc, WorkingScope), Acc]

  sealed trait ReturnContext
  private case object Unopinionated extends ReturnContext
  sealed trait Opinionated extends ReturnContext { val constants: Set[LogicalVariable] }
  case class SubqueryExpression(override val constants: Set[LogicalVariable]) extends Opinionated
  case class NextStatement(override val constants: Set[LogicalVariable]) extends Opinionated

  sealed trait VariableContext {

    def inMatch: VariableContext = this match {
      case d: DeclaringContext => d.inMatchingPattern
      case vc                  => vc
    }
  }
  case object Default extends VariableContext

  sealed trait DeclaringContext extends VariableContext {
    def declared: Set[LogicalVariable]
    def patternVariables: Set[LogicalVariable]
    def ast: Clause
    def inRelationship: Boolean

    def inMatchingPattern: MatchingPattern = MatchingPattern(declared, patternVariables, ast, inRelationship)

    def updateDeclared(remove: Set[LogicalVariable]): DeclaringContext = this match {
      case up @ UpdatingPattern(d, _, _, _) => up.copy(declared = d.filterNot(remove))
      case mp @ MatchingPattern(d, _, _, _) => mp.copy(declared = d.filterNot(remove))
    }

  }

  protected case class MatchingPattern(
    declared: Set[LogicalVariable],
    patternVariables: Set[LogicalVariable],
    ast: Clause,
    inRelationship: Boolean = false
  ) extends DeclaringContext

  protected case class UpdatingPattern(
    declared: Set[LogicalVariable],
    patternVariables: Set[LogicalVariable],
    ast: Clause,
    inRelationship: Boolean = false
  ) extends DeclaringContext

  sealed trait ProjectionContext
  case class Aggregating(clause: String, incomingToClause: Set[LogicalVariable]) extends ProjectionContext
  case object NonAggregating extends ProjectionContext

  case class Acc(
    scopeContext: ReturnContext,
    variableContext: VariableContext,
    projectionContext: ProjectionContext,
    errors: Set[SemanticError]
  ) {
    def apply(errors: Iterable[SemanticError]): Acc = copy(errors = this.errors ++ errors)
    def apply(errors: SemanticError): Acc = copy(errors = this.errors + errors)
    def inReturnContext(context: ReturnContext): Acc = copy(scopeContext = context)
    def inVariableContext(context: VariableContext): Acc = copy(variableContext = context)
    def inMatchingPattern: Acc = copy(variableContext = variableContext.inMatch)
    def inProjectionContext(context: ProjectionContext): Acc = copy(projectionContext = context)

    def withPatternVariables(vars: Set[LogicalVariable], inRelationship: Boolean = false): Acc =
      variableContext match {
        case u: UpdatingPattern =>
          copy(variableContext = u.copy(patternVariables = vars, inRelationship = inRelationship))
        case _ => this
      }

    def hasPatternVariables: Boolean = variableContext match {
      case UpdatingPattern(_, vars, _, _) if vars.nonEmpty => true
      case _                                               => false
    }
  }

  case object Acc {
    def init: Acc = Acc(Unopinionated, Default, NonAggregating, Set.empty)

    object InRelationshipChain {

      def unapply(acc: Acc): Option[Acc] = acc match {
        case Acc(_, UpdatingPattern(_, _, _, false), _, _) =>
          Some(acc)
        case _ => None
      }
    }

    object UpdatingContext {

      def unapply(acc: Acc): Option[Acc] = acc match {
        case Acc(_, UpdatingPattern(_, _, _, _), _, _) => Some(acc)
        case _                                         => None
      }
    }

    object CreatePattern {

      def unapply(acc: Acc): Option[(Acc, Set[LogicalVariable], Set[LogicalVariable], CreateOrInsert, Boolean)] =
        acc match {
          case Acc(returnContext, MatchingPattern(topo, patternVariables, c: CreateOrInsert, _), _, _) =>
            val inScalarSubquery = returnContext.isInstanceOf[SubqueryExpression]
            Some((acc, topo, patternVariables, c, inScalarSubquery))
          case Acc(returnContext, UpdatingPattern(topo, patternVariables, c: CreateOrInsert, _), _, _) =>
            val inScalarSubquery = returnContext.isInstanceOf[SubqueryExpression]
            Some((acc, topo, patternVariables, c, inScalarSubquery))
          case _ => None
        }
    }

    object MergePattern {

      def unapply(acc: Acc): Option[(Acc, Set[LogicalVariable], Merge)] = acc match {
        case Acc(_, MatchingPattern(topo, _, merge: Merge, _), _, _) => Some((acc, topo, merge))
        case Acc(_, UpdatingPattern(topo, _, merge: Merge, _), _, _) => Some((acc, topo, merge))
        case _                                                       => None
      }
    }

    object Aggregation {

      def unapply(acc: Acc): Option[(Acc, String, Set[LogicalVariable])] = acc match {
        case Acc(_, _, Aggregating(clause, incomingToClause), _) => Some((acc, clause, incomingToClause))
        case _                                                   => None
      }
    }

    object Opinionated {

      def unapply(acc: Acc): Option[(Acc, Set[LogicalVariable])] = acc match {
        case Acc(o: Opinionated, _, _, _) => Some((acc, o.constants))
        case _                            => None
      }

    }

    object SubqueryExpr {

      def unapply(acc: Acc): Option[Acc] = acc match {
        case Acc(SubqueryExpression(_), _, _, _) => Some((acc))
        case _                                   => None
      }

    }
  }

  object Scope {

    /**
     * Expression Scopes
     */

    object Expr {

      object Variable {

        def unapply(scope: WorkingScope)
          : Option[(LogicalVariable, RegularContext)] =
          scope match {
            case ExpressionScope(v: LogicalVariable, incoming, _, _, _) =>
              Some((v, incoming))
            case _ => None
          }
      }

      object VariableAggregation {

        def unapply(scope: WorkingScope)
          : Option[(LogicalVariable, Set[LogicalVariable], Set[Expression])] =
          scope match {
            case ExpressionScope(v: LogicalVariable, AggregatingExpressionContext(const, _, _, keys, _), _, _, _) =>
              Some((v, const, keys))
            case _ => None
          }

      }

      object PropertyAggregation {

        def unapply(scope: WorkingScope)
          : Option[(Property, Set[Expression])] =
          scope match {
            case ExpressionScope(p: Property, AggregatingExpressionContext(_, _, _, keys, _), _, _, Seq()) =>
              Some((p, keys))
            case _ => None
          }

      }
    }

    /**
     * Statement scopes
     */

    object Clause {

      object SubqueryCall {

        def unapply(scope: WorkingScope)
          : Option[(Seq[LogicalVariable], RegularContext)] =
          scope match {
            case StatementScope(ScopeClauseSubqueryCall(_, false, imports, _, _), incoming, _, _, _, _, _) =>
              Some((imports, incoming))
            case _ => None
          }

      }

      object ReturnItems {

        def unapply(scope: WorkingScope)
          : Option[(Seq[ReturnItem], InputPosition)] =
          scope match {
            case StatementScope(r @ Return(_, ri, _, _, _, _, _, _), _, _, _, _, _, _) =>
              Some((ri.items, r.position))
            case _ => None
          }
      }

      object ReturnStar {

        def unapply(scope: WorkingScope)
          : Option[(RegularContext, InputPosition)] =
          scope match {
            case StatementScope(Return.WithStar(r), in, _, _, _, _, _) =>
              Some((in, r.position))
            case _ => None
          }
      }

      object Declaring {

        def unapply(scope: WorkingScope)
          : Option[(ASTNode, RegularContext, Declarations, Seq[WorkingScope])] =
          scope match {
            case StatementScope(astNode, incoming, _, declarations, _, _, children) =>
              Some((astNode, incoming, declarations, children))
            case _ => None
          }

      }

      object Command {

        def unapply(scope: WorkingScope)
          : Option[(RegularContext, Seq[WorkingScope])] =
          scope match {
            case StatementScope(_: CommandClause, incoming, _, _, _, _, children) =>
              Some((incoming, children))
            case _ => None
          }

      }
    }

    /**
     * Pattern Scopes
     */

    object Pattern {

      object NamedPath {

        def unapply(scope: WorkingScope)
          : Option[(LogicalVariable, Set[LogicalVariable], Declarations)] =
          scope match {
            case PatternScope(NamedPatternPart(path, _), PatternScope.Topo(topo), _, declarations, _, _) =>
              Some((path, topo, declarations))
            case _ => None
          }

      }

      object Quantified {

        def unapply(scope: WorkingScope)
          : Option[(Set[VariableGrouping], Set[LogicalVariable])] =
          scope match {
            case PatternScope(QuantifiedPath(_, _, _, groupings), _, referenced, _, _, _) =>
              Some((groupings, referenced))
            case _ => None
          }

      }

      object Element {

        def unapply(scope: WorkingScope)
          : Option[(LogicalVariable, Set[LogicalVariable], Set[LogicalVariable])] =
          scope match {
            case PatternScope(PatternScope.PatternVariable(variable), PatternScope.Group(group), referenced, _, _, _) =>
              Some((variable, group, referenced))
            case _ => None
          }

      }

      object ShortestPath {

        def unapply(scope: WorkingScope)
          : Option[(String, PatternElement, Set[LogicalVariable])] =
          scope match {
            case PatternScope(sppp @ ShortestPathsPatternPart(element, _), incoming, _, _, _, _) =>
              Some((sppp.name, element, incoming.topologicalConstants))
            case _ => None
          }

      }

      object VariableInPatternAlreadyDeclared {

        def unapply(scope: (Acc, WorkingScope)): Option[(Acc, String, InputPosition)] =
          scope match {
            case (
                Acc.UpdatingContext(acc),
                PatternScope(RelationshipPattern(Some(variable), _, _, _, _, _), _, referenced, _, _, _)
              )
              if referenced.exists(_.name == variable.name) => Some((acc, variable.name, variable.position))
            case (
                Acc.InRelationshipChain(acc),
                PatternScope(NodePattern(Some(variable), _, _, _), _, referenced, _, _, _)
              ) if referenced.exists(_.name == variable.name) => Some((acc, variable.name, variable.position))
            case _ => None
          }

      }
    }

  }

  protected def getAliasesShadowingConstants(
    items: Seq[ReturnItem],
    constants: Set[LogicalVariable],
    pos: InputPosition
  ): Seq[SemanticError] =
    items
      .filter(i => !i.isPassThrough && i.alias.isDefined && constants.contains(i.alias.get))
      .map(i => SemanticError.variableShadowingOuterScope(i.name, pos))

  protected def findMultipleDeclarationsIn(names: Seq[LogicalVariable], pc: ProjectionClause): Seq[SemanticError] =
    names.groupMapReduce(identity)(_ => 1)(_ + _).filter(_._2 > 1).map {
      _ => SemanticError.multipleReturnColumnsWithSameName(pc.returnItems.items.head.position)
    }.toSeq

  protected def getIncompatibleReturnColumnsForUnion(
    position: InputPosition,
    result: Result,
    children: Seq[WorkingScope]
  ): Seq[SemanticError] =
    children.tail
      .filter(_.result != result)
      .flatMap { x =>
        (x.result, result) match {
          case (TableResult(tailCols), TableResult(headCols)) =>
            if (tailCols.map(_.name).toSet != headCols.map(_.name).toSet)
              Seq(SemanticError.incompatibleReturnColumns(Union.errorParam, position))
            else
              Seq.empty
          case _ =>
            Seq(SemanticError.incompatibleReturnColumns(Union.errorParam, position))
        }
      }

  protected def getIncompatibleReturnColumnsForConditionalQuery(
    result: Result,
    children: Seq[WorkingScope]
  ): Seq[SemanticError] =
    children.tail
      .filter(_.result != result)
      .flatMap { x =>
        (x.result, result) match {
          case (TableResult(tailCols), TableResult(headCols)) if tailCols.size != headCols.size =>
            Seq(SemanticError.incompatibleNumberOfReturnColumns(ConditionalQueryWhen.name, x.astNode.position))
          case (TableResult(tailCols), TableResult(headCols))
            if tailCols.map(_.name).toSet != headCols.map(_.name).toSet =>
            Seq(SemanticError.incompatibleWhenReturnColumns(ConditionalQueryWhen.name, x.astNode.position))
          case (res1, res2) if res1.getClass == res2.getClass => Seq()
          case _ =>
            Seq(SemanticError.incompatibleSubqueryType(
              ConditionalQueryWhen.name,
              x.astNode.asInstanceOf[ConditionalQueryBranch].query.position
            ))
        }
      }
}
