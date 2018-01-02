/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
import org.neo4j.cypher.internal.compiler.v2_3.ast.convert.commands.PatternConverters._
import org.neo4j.cypher.internal.compiler.v2_3.ast.{InequalitySeekRangeWrapper, NestedPipeExpression, PrefixSeekRangeWrapper}
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.ProjectedPath._
import org.neo4j.cypher.internal.compiler.v2_3.commands.expressions.{InequalitySeekRangeExpression, ProjectedPath, Expression => CommandExpression}
import org.neo4j.cypher.internal.compiler.v2_3.commands.predicates.Predicate
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.TokenType._
import org.neo4j.cypher.internal.compiler.v2_3.commands.values.UnresolvedRelType
import org.neo4j.cypher.internal.compiler.v2_3.commands.{predicates, expressions => commandexpressions, values => commandvalues}
import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.frontend.v2_3.ast.functions._
import org.neo4j.cypher.internal.frontend.v2_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v2_3.{InternalException, SemanticDirection, ast}
import org.neo4j.graphdb.Direction

object ExpressionConverters {
  def toCommandExpression(expression: ast.Function, invocation: ast.FunctionInvocation): CommandExpression =
    expression match {
      case Abs => commandexpressions.AbsFunction(toCommandExpression(invocation.arguments.head))
      case Acos => commandexpressions.AcosFunction(toCommandExpression(invocation.arguments.head))
      case Asin => commandexpressions.AsinFunction(toCommandExpression(invocation.arguments.head))
      case Atan => commandexpressions.AtanFunction(toCommandExpression(invocation.arguments.head))
      case Atan2 =>
        commandexpressions.Atan2Function(
          toCommandExpression(invocation.arguments.head),
          toCommandExpression(invocation.arguments(1)))
      case Avg =>
        val inner = toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Avg(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Ceil => commandexpressions.CeilFunction(toCommandExpression(invocation.arguments.head))
      case Coalesce => commandexpressions.CoalesceFunction(toCommandExpression(invocation.arguments): _*)
      case Collect =>
        val inner = toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Collect(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Cos => commandexpressions.CosFunction(toCommandExpression(invocation.arguments.head))
      case Cot => commandexpressions.CotFunction(toCommandExpression(invocation.arguments.head))
      case Count =>
        val inner = toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Count(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Degrees => commandexpressions.DegreesFunction(toCommandExpression(invocation.arguments.head))
      case E => commandexpressions.EFunction()
      case EndNode => commandexpressions.RelationshipEndPoints(toCommandExpression(invocation.arguments.head), start = false)
      case Exists =>
        invocation.arguments.head match {
          case property: ast.Property =>
            commands.predicates.PropertyExists(toCommandExpression(property.map), PropertyKey(property.propertyKey.name))
          case expression: ast.PatternExpression =>
            toCommandPredicate(expression)
          case expression: NestedPipeExpression =>
            toCommandPredicate(expression)
          case e: ast.ContainerIndex=>
            commandexpressions.ContainerIndex(toCommandExpression(e.expr), toCommandExpression(e.idx))
        }
      case Exp => commandexpressions.ExpFunction(toCommandExpression(invocation.arguments.head))
      case Floor => commandexpressions.FloorFunction(toCommandExpression(invocation.arguments.head))
      case Has =>
        val property = invocation.arguments.head.asInstanceOf[ast.Property]
        commands.predicates.PropertyExists(toCommandExpression(property.map), PropertyKey(property.propertyKey.name))

      case Haversin => commandexpressions.HaversinFunction(toCommandExpression(invocation.arguments.head))
      case Head =>
        commandexpressions.ContainerIndex(
          toCommandExpression(invocation.arguments.head),
          commandexpressions.Literal(0)
        )
      case Id => commandexpressions.IdFunction(toCommandExpression(invocation.arguments.head))
      case Keys => commandexpressions.KeysFunction(toCommandExpression(invocation.arguments.head))
      case Labels => commandexpressions.LabelsFunction(toCommandExpression(invocation.arguments.head))
      case Last =>
        commandexpressions.ContainerIndex(
          toCommandExpression(invocation.arguments.head),
          commandexpressions.Literal(-1)
        )
      case Left =>
        commandexpressions.LeftFunction(
          toCommandExpression(invocation.arguments.head),
          toCommandExpression(invocation.arguments(1))
        )
      case Length => commandexpressions.LengthFunction(toCommandExpression(invocation.arguments.head))
      case Log => commandexpressions.LogFunction(toCommandExpression(invocation.arguments.head))
      case Log10 => commandexpressions.Log10Function(toCommandExpression(invocation.arguments.head))
      case Lower => commandexpressions.LowerFunction(toCommandExpression(invocation.arguments.head))
      case LTrim => commandexpressions.LTrimFunction(toCommandExpression(invocation.arguments.head))
      case Max =>
        val inner = toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Max(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Min =>
        val inner = toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Min(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Nodes => commandexpressions.NodesFunction(toCommandExpression(invocation.arguments.head))
      case PercentileCont =>
        val firstArg = toCommandExpression(invocation.arguments.head)
        val secondArg = toCommandExpression(invocation.arguments(1))

        val command = commandexpressions.PercentileCont(firstArg, secondArg)
        if (invocation.distinct)
          commandexpressions.Distinct(command, firstArg)
        else
          command
      case PercentileDisc =>
        val firstArg = toCommandExpression(invocation.arguments.head)
        val secondArg = toCommandExpression(invocation.arguments(1))

        val command = commandexpressions.PercentileDisc(firstArg, secondArg)
        if (invocation.distinct)
          commandexpressions.Distinct(command, firstArg)
        else
          command
      case Pi => commandexpressions.PiFunction()
      case Radians => commandexpressions.RadiansFunction(toCommandExpression(invocation.arguments.head))
      case Rand => commandexpressions.RandFunction()
      case functions.Range =>
        commandexpressions.RangeFunction(
          toCommandExpression(invocation.arguments.head),
          toCommandExpression(invocation.arguments(1)),
          toCommandExpression(invocation.arguments.lift(2)).getOrElse(commandexpressions.Literal(1))
        )
      case Relationships | Rels => commandexpressions.RelationshipFunction(toCommandExpression(invocation.arguments.head))
      case Replace =>
        commandexpressions.ReplaceFunction(
          toCommandExpression(invocation.arguments.head),
          toCommandExpression(invocation.arguments(1)),
          toCommandExpression(invocation.arguments(2))
        )
      case Reverse => commandexpressions.ReverseFunction(toCommandExpression(invocation.arguments.head))
      case Right =>
        commandexpressions.RightFunction(
          toCommandExpression(invocation.arguments.head),
          toCommandExpression(invocation.arguments(1))
        )
      case Round => commandexpressions.RoundFunction(toCommandExpression(invocation.arguments.head))
      case RTrim => commandexpressions.RTrimFunction(toCommandExpression(invocation.arguments.head))
      case Sign => commandexpressions.SignFunction(toCommandExpression(invocation.arguments.head))
      case Sin => commandexpressions.SinFunction(toCommandExpression(invocation.arguments.head))
      case Size => commandexpressions.SizeFunction(toCommandExpression(invocation.arguments.head))
      case Split =>
        commandexpressions.SplitFunction(
          toCommandExpression(invocation.arguments.head),
          toCommandExpression(invocation.arguments(1))
        )
      case Sqrt => commandexpressions.SqrtFunction(toCommandExpression(invocation.arguments.head))
      case StartNode => commandexpressions.RelationshipEndPoints(toCommandExpression(invocation.arguments.head), start = true)
      case StdDev =>
        val inner = toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Stdev(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case StdDevP =>
        val inner = toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.StdevP(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Str => commandexpressions.StrFunction(toCommandExpression(invocation.arguments.head))
      case Substring =>
        commandexpressions.SubstringFunction(
          toCommandExpression(invocation.arguments.head),
          toCommandExpression(invocation.arguments(1)),
          toCommandExpression(invocation.arguments.lift(2))
        )
      case Sum =>
        val inner = toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Sum(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Tail =>
        commandexpressions.CollectionSliceExpression(
          toCommandExpression(invocation.arguments.head),
          Some(commandexpressions.Literal(1)),
          None
        )
      case Tan => commandexpressions.TanFunction(toCommandExpression(invocation.arguments.head))
      case Timestamp => commandexpressions.TimestampFunction()
      case ToFloat => commandexpressions.ToFloatFunction(toCommandExpression(invocation.arguments.head))
      case ToInt => commandexpressions.ToIntFunction(toCommandExpression(invocation.arguments.head))
      case ToLower => commandexpressions.LowerFunction(toCommandExpression(invocation.arguments.head))
      case ToStr => commandexpressions.StrFunction(toCommandExpression(invocation.arguments.head))
      case ToString => commandexpressions.ToStringFunction(toCommandExpression(invocation.arguments.head))
      case ToUpper => commandexpressions.UpperFunction(toCommandExpression(invocation.arguments.head))
      case Trim => commandexpressions.TrimFunction(toCommandExpression(invocation.arguments.head))
      case Type => commandexpressions.RelationshipTypeFunction(toCommandExpression(invocation.arguments.head))
      case Upper => commandexpressions.UpperFunction(toCommandExpression(invocation.arguments.head))
    }

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
    case e: ast.StartsWith => predicates.StartsWith(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
    case e: ast.EndsWith => predicates.EndsWith(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
    case e: ast.Contains => predicates.Contains(toCommandExpression(e.lhs), toCommandExpression(e.rhs))
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
    case e: ast.FunctionInvocation => toCommandExpression(e.function.get, e)
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
    case e: NestedPipeExpression => commandexpressions.NestedPipeExpression(e.pipe, toCommandProjectedPath(e.path))
    case e: ast.GetDegree => getDegree(e)
    case e: PrefixSeekRangeWrapper => commandexpressions.PrefixSeekRangeExpression(e.range.map(toCommandExpression))
    case e: InequalitySeekRangeWrapper => InequalitySeekRangeExpression(e.range.mapBounds(toCommandExpression))
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

      case SingleRelationshipPathStep(Identifier(rel), SemanticDirection.INCOMING, next) =>
        singleIncomingRelationshipProjector(rel, project(next))

      case SingleRelationshipPathStep(Identifier(rel), SemanticDirection.OUTGOING, next) =>
        singleOutgoingRelationshipProjector(rel, project(next))

      case SingleRelationshipPathStep(Identifier(rel), SemanticDirection.BOTH, next) =>
        singleUndirectedRelationshipProjector(rel, project(next))

      case MultiRelationshipPathStep(Identifier(rel), SemanticDirection.INCOMING, next) =>
        multiIncomingRelationshipProjector(rel, project(next))

      case MultiRelationshipPathStep(Identifier(rel), SemanticDirection.OUTGOING, next) =>
        multiOutgoingRelationshipProjector(rel, project(next))

      case MultiRelationshipPathStep(Identifier(rel), SemanticDirection.BOTH, next) =>
        multiUndirectedRelationshipProjector(rel, project(next))

      case NilPathStep =>
        nilProjector
    }

    val projector = project(e.step)
    val dependencies = e.step.dependencies.map(_.name)

    ProjectedPath(dependencies, projector)
  }
}

object DirectionConverter {
  def toGraphDb(dir: SemanticDirection): Direction = dir match {
    case SemanticDirection.INCOMING => Direction.INCOMING
    case SemanticDirection.OUTGOING => Direction.OUTGOING
    case SemanticDirection.BOTH => Direction.BOTH
  }
}
