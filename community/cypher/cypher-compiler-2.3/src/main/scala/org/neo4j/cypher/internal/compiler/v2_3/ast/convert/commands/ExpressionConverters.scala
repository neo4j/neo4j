/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands

import org.neo4j.cypher.internal.compiler.v2_3._
import org.neo4j.cypher.internal.compiler.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.PatternConverters._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.ProjectedPath._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{Expression => CommandExpression, InequalitySeekRangeExpression, ProjectedPath}
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.TokenType._
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.UnresolvedRelType
import org.neo4j.cypher.internal.compiler.v2_3.commands.{expressions => commandexpressions, predicates, values => commandvalues}
import org.neo4j.cypher.internal.compiler.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.compiler.v2_3.parser.{LikePatternParser, ParsedLikePattern, convertLikePatternToRegex}
import org.neo4j.graphdb.Direction

object ExpressionConverters {

  def toCommandExpression(expression: ast.Expression): CommandExpression = expression match {
    case e: ast.Null => commandexpressions.Null()
    case e: ast.True => predicates.True()
    case e: ast.False => predicates.Not(predicates.True())
    case e: ast.Literal => commandexpressions.Literal(e.value)
    case e: ast.Identifier => identifier(e)
    case e: ast.Or => predicates.Or(toCommandPredicate(e.lhs), toCommandPredicate(e.rhs))
    case e: ast.Xor => predicates.Xor(toCommandPredicate(e.lhs), toCommandPredicate(e.rhs))
    case e: ast.And => predicates.And(toCommandPredicate(e.lhs), toCommandPredicate(e.rhs))
    case e: ast.Ands => predicates.Ands(NonEmptyList.from(e.exprs.map(toCommandPredicate)))
    case e: ast.Ors => predicates.Ors(NonEmptyList.from(e.exprs.map(toCommandPredicate)))
    case e: ast.Not => predicates.Not(toCommandPredicate(e.rhs))
    case e: ast.Equals => predicates.Equals(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
    case e: ast.NotEquals => predicates.Not(predicates.Equals(toCommandExpression(e.lhs), toCommandExpression(e.rhs)))
    case e: ast.RegexMatch => regexMatch(e)
    case e: ast.In => in(e)
    case e: ast.Like => like(e)
    case e: ast.NotLike => predicates.Not(toCommandPredicate(ast.Like(e.lhs, e.pattern, e.caseInsensitive)(e.position)))
    case e: ast.IsNull => predicates.IsNull(toCommandExpression(e.lhs))
    case e: ast.IsNotNull => predicates.Not(predicates.IsNull(toCommandExpression(e.lhs)))
    case e: ast.InequalityExpression => inequalityExpression(e)
    case e: ast.Add => commandexpressions.Add(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
    case e: ast.UnaryAdd => toCommandExpression(e.rhs)
    case e: ast.Subtract => commandexpressions.Subtract(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
    case e: ast.UnarySubtract => commandexpressions.Subtract(commandexpressions.Literal(0), toCommandExpression(e.rhs))
    case e: ast.Multiply => commandexpressions.Multiply(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
    case e: ast.Divide => commandexpressions.Divide(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
    case e: ast.Modulo => commandexpressions.Modulo(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
    case e: ast.Pow => commandexpressions.Pow(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
    case e: ast.FunctionInvocation => e.function.get.asCommandExpression(e)
    case e: ast.CountStar => commandexpressions.CountStar()
    case e: ast.Property => toCommandProperty(e)
    case e: ast.Parameter => toCommandParameter(e)
    case e: ast.CaseExpression => caseExpression(e)
    case e: ast.PatternExpression => commands.PathExpression(e.pattern.asLegacyPatterns)
    case e: ast.ShortestPathExpression => commandexpressions.ShortestPathExpression(e.pattern.asLegacyPatterns(None).head)
    case e: ast.HasLabels => hasLabels(e)
    case e: ast.Collection => commandexpressions.Collection(toCommandExpression(e.expressions): _*)
    case e: ast.MapExpression => mapExpression(e)
    case e: ast.CollectionSlice => commandexpressions.CollectionSliceExpression(toCommandExpression(e.collection), toCommandExpression(e.from), toCommandExpression(e.to))
    case e: ast.ContainerIndex => commandexpressions.ContainerIndex(toCommandExpression(e.expr), toCommandExpression(e.idx))
    case e: ast.FilterExpression => commandexpressions.FilterFunction(toCommandExpression(e.expression), e.identifier.name, e.innerPredicate.map(toCommandPredicate).getOrElse(predicates.True()))
    case e: ast.ExtractExpression => commandexpressions.ExtractFunction(toCommandExpression(e.expression), e.identifier.name, toCommandExpression(e.scope.extractExpression.get))
    case e: ast.ListComprehension => listComprehension(e)
    case e: ast.AllIterablePredicate => commands.AllInCollection(toCommandExpression(e.expression), e.identifier.name, e.innerPredicate.map(toCommandPredicate).getOrElse(predicates.True()))
    case e: ast.AnyIterablePredicate => commands.AnyInCollection(toCommandExpression(e.expression), e.identifier.name, e.innerPredicate.map(toCommandPredicate).getOrElse(predicates.True()))
    case e: ast.NoneIterablePredicate => commands.NoneInCollection(toCommandExpression(e.expression), e.identifier.name, e.innerPredicate.map(toCommandPredicate).getOrElse(predicates.True()))
    case e: ast.SingleIterablePredicate => commands.SingleInCollection(toCommandExpression(e.expression), e.identifier.name, e.innerPredicate.map(toCommandPredicate).getOrElse(predicates.True()))
    case e: ast.ReduceExpression => commandexpressions.ReduceFunction(toCommandExpression(e.collection), e.identifier.name, toCommandExpression(e.expression), e.accumulator.name, toCommandExpression(e.init))
    case e: ast.PathExpression => toCommandProjectedPath(e)
    case e: ast.NestedPipeExpression => commandexpressions.NestedPipeExpression(e.pipe, toCommandProjectedPath(e.path))
    case e: ast.GetDegree => getDegree(e)
    case e: ast.PrefixSeekRangeWrapper => commandexpressions.PrefixSeekRangeExpression(e.range)
    case e: ast.InequalitySeekRangeWrapper => InequalitySeekRangeExpression(e.range.mapBounds(toCommandExpression))
    case e: ast.AndedPropertyInequalities => predicates.AndedPropertyComparablePredicates(identifier(e.identifier), toCommandProperty(e.property), e.inequalities.map(inequalityExpression))
    case _ =>
      throw new InternalException(s"Unknown expression type during transformation (${expression.getClass})")
  }

  def toCommandPredicate(e: ast.Expression): Predicate = e match {
    case e: ast.PatternExpression => predicates.NonEmpty(toCommandExpression(e))
    case e: ast.FilterExpression => predicates.NonEmpty(toCommandExpression(e))
    case e: ast.ExtractExpression => predicates.NonEmpty(toCommandExpression(e))
    case e: ast.ListComprehension => predicates.NonEmpty(toCommandExpression(e))
    case e => toCommandExpression(e) match {
      case c: Predicate => c
      case c => predicates.CoercedPredicate(c)
    }
  }

  def toCommandParameter(e: ast.Parameter) = commandexpressions.ParameterExpression(e.name)

  def toCommandProperty(e: ast.Property): commandexpressions.Property =
    commandexpressions.Property(toCommandExpression(e.map), PropertyKey(e.propertyKey.name))

  def toCommandExpression(expression: Option[ast.Expression]): Option[CommandExpression] =
    expression.map(toCommandExpression)

  def toCommandExpression(expressions: Seq[ast.Expression]): Seq[CommandExpression] =
    expressions.map(toCommandExpression)

  private def identifier(e: ast.Identifier) = commands.expressions.Identifier(e.name)

  private def inequalityExpression(original: ast.InequalityExpression): predicates.ComparablePredicate = original match {
    case e: ast.LessThan => predicates.LessThan(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
    case e: ast.LessThanOrEqual => predicates.LessThanOrEqual(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
    case e: ast.GreaterThan => predicates.GreaterThan(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
    case e: ast.GreaterThanOrEqual => predicates.GreaterThanOrEqual(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
  }

  private def getDegree(original: ast.GetDegree) = {
    val typ = original.relType.map(relType => UnresolvedRelType(relType.name))
    commandexpressions.GetDegree(toCommandExpression(original.node), typ, original.dir)
  }


  private def regexMatch(e: ast.RegexMatch) = toCommandExpression(e.rhs) match {
    case literal: commandexpressions.Literal =>
      predicates.LiteralRegularExpression(toCommandExpression(e.lhs), literal)
    case command =>
      predicates.RegularExpression(toCommandExpression(e.lhs), command)
  }

  private def like(e: ast.Like) = {
    def stringToRegex(s: String): String =
      patternToRegex(LikePatternParser(s))

    def patternToRegex(likePattern: ParsedLikePattern): String =
      convertLikePatternToRegex(likePattern, e.caseInsensitive)

    toCommandExpression(e.rhs) match {
      case nullLiteral@commandexpressions.Literal(null) =>
        nullLiteral

      case commandexpressions.Literal(v) =>
        val pattern = LikePatternParser(v.asInstanceOf[String])
        val literal = commandexpressions.Literal(patternToRegex(pattern))
        val regularExpression = predicates.LiteralRegularExpression(toCommandExpression(e.lhs), literal)
        predicates.LiteralLikePattern(regularExpression, pattern, e.caseInsensitive)

      case command =>
        predicates.RegularExpression(toCommandExpression(e.lhs), command)(stringToRegex)
    }
  }

  private def in(e: ast.In) = {
    val innerEquals = predicates.Equals(toCommandExpression(e.lhs), commandexpressions.Identifier("-_-INNER-_-"))
    commands.AnyInCollection(toCommandExpression(e.rhs), "-_-INNER-_-", innerEquals)
  }

  private def caseExpression(e: ast.CaseExpression) = e.expression match {
    case Some(innerExpression) =>
      val legacyAlternatives = e.alternatives.map { a => (toCommandExpression(a._1), toCommandExpression(a._2)) }
      commandexpressions.SimpleCase(toCommandExpression(innerExpression), legacyAlternatives, toCommandExpression(e.default))
    case None =>
      val predicateAlternatives = e.alternatives.map { a => (toCommandPredicate(a._1), toCommandExpression(a._2)) }
      commandexpressions.GenericCase(predicateAlternatives, toCommandExpression(e.default))
  }

  private def hasLabels(e: ast.HasLabels) = e.labels.map {
    l => predicates.HasLabel(toCommandExpression(e.expression), commandvalues.KeyToken.Unresolved(l.name, commandvalues.TokenType.Label)): Predicate
  } reduceLeft {
    predicates.And(_, _)
  }

  private def mapExpression(e: ast.MapExpression): commandexpressions.LiteralMap = {
    val literalMap: Map[String, CommandExpression] = e.items.map {
      case (id, ex) => id.name -> toCommandExpression(ex)
    }.toMap
    commandexpressions.LiteralMap(literalMap)
  }

  private def listComprehension(e: ast.ListComprehension): CommandExpression = {
    val filter = e.innerPredicate match {
      case Some(_: ast.True) | None =>
        toCommandExpression(e.expression)
      case Some(inner) =>
        commandexpressions.FilterFunction(toCommandExpression(e.expression), e.identifier.name, toCommandPredicate(inner))
    }
    e.extractExpression match {
      case Some(extractExpression) =>
        commandexpressions.ExtractFunction(filter, e.identifier.name, toCommandExpression(extractExpression))
      case None =>
        filter
    }
  }

  def toCommandProjectedPath(e: ast.PathExpression): ProjectedPath = {
    def project(pathStep: PathStep): Projector = pathStep match {

      case NodePathStep(Identifier(node), next) =>
        singleNodeProjector(node, project(next))

      case SingleRelationshipPathStep(Identifier(rel), Direction.INCOMING, next) =>
        singleIncomingRelationshipProjector(rel, project(next))

      case SingleRelationshipPathStep(Identifier(rel), Direction.OUTGOING, next) =>
        singleOutgoingRelationshipProjector(rel, project(next))

      case SingleRelationshipPathStep(Identifier(rel), Direction.BOTH, next) =>
        singleUndirectedRelationshipProjector(rel, project(next))

      case MultiRelationshipPathStep(Identifier(rel), Direction.INCOMING, next) =>
        multiIncomingRelationshipProjector(rel, project(next))

      case MultiRelationshipPathStep(Identifier(rel), Direction.OUTGOING, next) =>
        multiOutgoingRelationshipProjector(rel, project(next))

      case MultiRelationshipPathStep(Identifier(rel), Direction.BOTH, next) =>
        multiUndirectedRelationshipProjector(rel, project(next))

      case NilPathStep =>
        nilProjector
    }

    val projector = project(e.step)
    val dependencies = e.step.dependencies.map(_.name)

    ProjectedPath(dependencies, projector)
  }
}
