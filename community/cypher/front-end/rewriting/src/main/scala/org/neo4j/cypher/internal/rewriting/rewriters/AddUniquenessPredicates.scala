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
package org.neo4j.cypher.internal.rewriting.rewriters

import org.neo4j.cypher.internal.ast.Match
import org.neo4j.cypher.internal.ast.Merge
import org.neo4j.cypher.internal.ast.Where
import org.neo4j.cypher.internal.ast.semantics.SemanticState
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.And
import org.neo4j.cypher.internal.expressions.Disjoint
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.In
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Not
import org.neo4j.cypher.internal.expressions.Pattern
import org.neo4j.cypher.internal.expressions.PatternPart
import org.neo4j.cypher.internal.expressions.QuantifiedPath
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.RelationshipChain
import org.neo4j.cypher.internal.expressions.RelationshipPattern
import org.neo4j.cypher.internal.expressions.ScopeExpression
import org.neo4j.cypher.internal.expressions.ShortestPaths
import org.neo4j.cypher.internal.expressions.SymbolicName
import org.neo4j.cypher.internal.expressions.Unique
import org.neo4j.cypher.internal.label_expressions.LabelExpression
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonConjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpression.ColonDisjunction
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Conjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Disjunctions
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Leaf
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Negation
import org.neo4j.cypher.internal.label_expressions.LabelExpression.Wildcard
import org.neo4j.cypher.internal.rewriting.conditions.SubqueryExpressionsHaveSemanticInfo
import org.neo4j.cypher.internal.rewriting.conditions.noUnnamedNodesAndRelationships
import org.neo4j.cypher.internal.rewriting.rewriters.factories.ASTRewriterFactory
import org.neo4j.cypher.internal.util.ASTNode
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.CypherExceptionFactory
import org.neo4j.cypher.internal.util.Foldable.SkipChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildren
import org.neo4j.cypher.internal.util.Foldable.TraverseChildrenNewAccForSiblings
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.StepSequencer
import org.neo4j.cypher.internal.util.StepSequencer.Step
import org.neo4j.cypher.internal.util.bottomUp
import org.neo4j.cypher.internal.util.symbols.CypherTypeInfo

import scala.util.control.TailCalls
import scala.util.control.TailCalls.TailRec

case object RelationshipUniquenessPredicatesInMatchAndMerge extends StepSequencer.Condition

case object AddUniquenessPredicates extends Step with ASTRewriterFactory {

  val rewriter: Rewriter = bottomUp(Rewriter.lift {
    case m @ Match(_, pattern: Pattern, _, where) =>
      val rels: Seq[NodeConnection] = collectRelationships(pattern)
      val newWhere = withPredicates(m, rels, where)
      m.copy(where = newWhere)(m.position)
    case m @ Merge(pattern: PatternPart, _, where) =>
      val rels: Seq[NodeConnection] = collectRelationships(pattern)
      val newWhere = withPredicates(m, rels, where)
      m.copy(where = newWhere)(m.position)
    case qpp @ QuantifiedPath(patternPart, _, where, _) =>
      val rels = collectRelationships(patternPart)
      val newWhere = withPredicates(qpp, rels, where.map(Where(_)(qpp.position))).map(_.expression)

      // We will generate Unique predicates for every relationship in a QPP.
      // If the relationship has an anonymous name, it is not yet included in the variableGroupings.
      // Since the Unique predicate lives outside of the QPP and needs to see the group variable, we need to add those
      // variables to the set of variableGroupings.
      val allRelationshipVariables = qpp.part.folder.treeCollect {
        case RelationshipPattern(Some(relVar), _, _, _, _, _) => relVar
      }.toSet
      val notYetExportedSingletonVars = allRelationshipVariables -- qpp.variableGroupings.map(_.singleton)
      val newGroupings = notYetExportedSingletonVars.map(QuantifiedPath.getGrouping(_, qpp.position))
      qpp.copy(optionalWhereExpression = newWhere, variableGroupings = qpp.variableGroupings ++ newGroupings)(
        qpp.position
      )
  })

  private def withPredicates(pattern: ASTNode, rels: Seq[NodeConnection], where: Option[Where]): Option[Where] = {
    val maybePredicate: Option[Expression] = createPredicateFor(rels, pattern.position)
    val newWhere: Option[Where] = (where, maybePredicate) match {
      case (Some(oldWhere), Some(newPredicate)) =>
        Some(oldWhere.copy(expression = And(oldWhere.expression, newPredicate)(pattern.position))(pattern.position))

      case (None, Some(newPredicate)) =>
        Some(Where(expression = newPredicate)(pattern.position))

      case (oldWhere, None) => oldWhere
    }

    newWhere
  }

  def collectRelationships(pattern: ASTNode): Seq[NodeConnection] =
    pattern.folder.treeFold(Seq.empty[NodeConnection]) {
      case _: ScopeExpression =>
        acc => SkipChildren(acc)

      case qpp: QuantifiedPath =>
        acc =>
          TraverseChildrenNewAccForSiblings(
            Seq.empty[SingleRelationship],
            innerAcc => {
              // Make sure that predicates we generate for QPPs use the group variable, not the singleton variable.
              // To ensure this, we need to change the position to that of the QPP.
              val innerRelsWithFixedPositions = innerAcc.asInstanceOf[Seq[SingleRelationship]]
                .map(x => x.copy(variable = x.variable.withPosition(qpp.position)))
              acc :+ RelationshipGroup(innerRelsWithFixedPositions)
            }
          )

      case _: ShortestPaths =>
        acc => SkipChildren(acc)

      case RelationshipChain(_, RelationshipPattern(optIdent, labelExpression, None, _, _, _), _) =>
        acc => {
          val ident =
            optIdent.getOrElse(throw new IllegalStateException("This rewriter cannot work with unnamed patterns"))
          TraverseChildren(acc :+ SingleRelationship(ident, labelExpression))
        }

      case RelationshipChain(_, RelationshipPattern(optIdent, labelExpression, Some(_), _, _, _), _) =>
        acc => {
          val ident =
            optIdent.getOrElse(throw new IllegalStateException("This rewriter cannot work with unnamed patterns"))
          TraverseChildren(acc :+ RelationshipGroup(Seq(SingleRelationship(ident, labelExpression))))
        }
    }

  private def createPredicateFor(nodeConnections: Seq[NodeConnection], pos: InputPosition): Option[Expression] = {
    createPredicatesFor(nodeConnections, pos).reduceOption(expressions.And(_, _)(pos))
  }

  def createPredicatesFor(nodeConnections: Seq[NodeConnection], pos: InputPosition): Seq[Expression] = {
    val pairs = for {
      (x, i) <- nodeConnections.zipWithIndex
      y <- nodeConnections.drop(i + 1)
    } yield (x, y)

    val interRelUniqueness = pairs.collect {
      case (x: SingleRelationship, y: SingleRelationship) if x.name == y.name =>
        Seq(False()(pos))

      case (x: SingleRelationship, y: SingleRelationship) if !x.isAlwaysDifferentFrom(y) =>
        Seq(Not(Equals(x.variable.copyId, y.variable.copyId)(pos))(pos))

      case (x: SingleRelationship, y: RelationshipGroup) =>
        y.innerRelationships
          .filterNot(_.isAlwaysDifferentFrom(x))
          .map(_.variable.copyId)
          .reduceRightOption[Expression]((y, x) => expressions.Add(x, y)(pos))
          .map { innerY =>
            Not(In(x.variable.copyId, innerY)(pos))(pos)
          }

      case (x: RelationshipGroup, y: SingleRelationship) =>
        x.innerRelationships
          .filterNot(_.isAlwaysDifferentFrom(y))
          .map(_.variable.copyId)
          .reduceRightOption[Expression]((y, x) => expressions.Add(x, y)(pos))
          .map { innerX =>
            Not(In(y.variable.copyId, innerX)(pos))(pos)
          }

      case (x: RelationshipGroup, y: RelationshipGroup) =>
        val xRels = x.innerRelationships.filter(innerX => y.innerRelationships.exists(!_.isAlwaysDifferentFrom(innerX)))
        val yRels = y.innerRelationships.filter(innerY => x.innerRelationships.exists(!_.isAlwaysDifferentFrom(innerY)))
        Option.when(xRels.nonEmpty && yRels.nonEmpty) {
          if (xRels.map(_.name).intersect(yRels.map(_.name)).nonEmpty) {
            False()(pos)
          } else {
            val xList = reduceLists(xRels.map(_.variable.copyId), pos)
            val yList = reduceLists(yRels.map(_.variable.copyId), pos)
            Disjoint(xList, yList)(pos)
          }
        }
    }.flatten

    val intraRelUniqueness = nodeConnections.collect {
      case rg: RelationshipGroup =>
        val singleList = reduceLists(rg.innerRelationships.map(_.variable.copyId), pos)
        Unique(singleList)(pos)
    }

    interRelUniqueness ++ intraRelUniqueness
  }

  private def reduceLists(vars: Seq[LogicalVariable], pos: InputPosition): Expression =
    vars.reduceRight[Expression]((y, x) => expressions.Add(x, y)(pos))

  sealed trait NodeConnection

  case class RelationshipGroup(innerRelationships: Seq[SingleRelationship]) extends NodeConnection

  case class SingleRelationship(
    variable: LogicalVariable,
    labelExpression: Option[LabelExpression]
  ) extends NodeConnection {
    def name: String = variable.name

    def isAlwaysDifferentFrom(other: SingleRelationship): Boolean = {
      val relTypesToConsider =
        getRelTypesToConsider(labelExpression).concat(getRelTypesToConsider(other.labelExpression)).distinct
      !overlaps(relTypesToConsider, labelExpression, other.labelExpression)
    }
  }

  override def preConditions: Set[StepSequencer.Condition] = Set(
    noUnnamedNodesAndRelationships
  )

  override def postConditions: Set[StepSequencer.Condition] = Set(RelationshipUniquenessPredicatesInMatchAndMerge)

  override def invalidatedConditions: Set[StepSequencer.Condition] = Set(
    ProjectionClausesHaveSemanticInfo, // It can invalidate this condition by rewriting things inside WITH/RETURN.
    SubqueryExpressionsHaveSemanticInfo // It can invalidate this condition by rewriting things inside Subquery Expressions.
  )

  override def getRewriter(
    semanticState: SemanticState,
    parameterTypeMapping: Map[String, CypherTypeInfo],
    cypherExceptionFactory: CypherExceptionFactory,
    anonymousVariableNameGenerator: AnonymousVariableNameGenerator
  ): Rewriter = rewriter

  private[rewriters] def evaluate(expression: LabelExpression, relType: SymbolicName): TailRec[Boolean] =
    expression match {
      case Conjunctions(children)               => ands(children, relType)
      case ColonConjunction(lhs, rhs)           => ands(Seq(lhs, rhs), relType)
      case Disjunctions(children)               => ors(children, relType)
      case ColonDisjunction(lhs, rhs)           => ors(Seq(lhs, rhs), relType)
      case Negation(e)                          => TailCalls.tailcall(evaluate(e, relType)).map(value => !value)
      case Wildcard()                           => TailCalls.done(true)
      case Leaf(expressionRelType: RelTypeName) => TailCalls.done(expressionRelType == relType)
      case x =>
        throw new IllegalArgumentException(s"Unexpected label expression $x when evaluating relationship overlap")
    }

  private def ors(exprs: Seq[LabelExpression], relType: SymbolicName): TailRec[Boolean] = {
    if (exprs.isEmpty) TailCalls.done(false)
    else {
      for {
        head <- TailCalls.tailcall(evaluate(exprs.head, relType))
        tail <- if (head) TailCalls.done(true) else ors(exprs.tail, relType)
      } yield head || tail
    }
  }

  private def ands(exprs: Seq[LabelExpression], relType: SymbolicName): TailRec[Boolean] = {
    if (exprs.isEmpty) TailCalls.done(true)
    else {
      for {
        head <- TailCalls.tailcall(evaluate(exprs.head, relType))
        tail <- if (!head) TailCalls.done(false) else ands(exprs.tail, relType)
      } yield head && tail
    }
  }

  private[rewriters] def overlaps(
    relTypesToConsider: Seq[SymbolicName],
    labelExpression0: Option[LabelExpression],
    labelExpression1: Option[LabelExpression]
  ): Boolean = {
    // if both labelExpression0 and labelExpression1 evaluate to true when relType is present on a rel, then there's an overlap between the label expressions
    relTypesToConsider.exists(relType => ands(Seq(labelExpression0, labelExpression1).flatten, relType).result)
  }

  private[rewriters] def getRelTypesToConsider(labelExpression: Option[LabelExpression]): Seq[SymbolicName] = {
    // also add the arbitrary rel type "" to check for rel types which are not explicitly named (such as in -[r]-> or -[r:%]->)
    labelExpression.map(_.flatten).getOrElse(Seq.empty) appended RelTypeName("")(InputPosition.NONE)
  }
}
