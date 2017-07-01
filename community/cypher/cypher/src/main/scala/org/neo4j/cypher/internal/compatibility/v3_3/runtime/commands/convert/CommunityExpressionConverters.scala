/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.convert

import org.neo4j.cypher.internal.compatibility.v3_3.runtime._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.ProjectedPath._
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions.{InequalitySeekRangeExpression, ProjectedPath, Expression => CommandExpression}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.predicates.Predicate
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.values.UnresolvedRelType
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.{PathExtractorExpression, predicates, expressions => commandexpressions, values => commandvalues}
import org.neo4j.cypher.internal.compiler.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.ast._
import org.neo4j.cypher.internal.frontend.v3_3.ast.functions._
import org.neo4j.cypher.internal.frontend.v3_3.ast.rewriters.DesugaredMapProjection
import org.neo4j.cypher.internal.frontend.v3_3.helpers.NonEmptyList
import org.neo4j.cypher.internal.frontend.v3_3.{InternalException, SemanticDirection, ast}

object CommunityExpressionConverters extends ExpressionConverters {

  import PatternConverters._

  private def toCommandExpression(expression: ast.Function, invocation: ast.FunctionInvocation, self: ExpressionConverters): CommandExpression =
    expression match {
      case Abs => commandexpressions.AbsFunction(self.toCommandExpression(invocation.arguments.head))
      case Acos => commandexpressions.AcosFunction(self.toCommandExpression(invocation.arguments.head))
      case Asin => commandexpressions.AsinFunction(self.toCommandExpression(invocation.arguments.head))
      case Atan => commandexpressions.AtanFunction(self.toCommandExpression(invocation.arguments.head))
      case Atan2 =>
        commandexpressions.Atan2Function(
          self.toCommandExpression(invocation.arguments.head),
          self.toCommandExpression(invocation.arguments(1)))
      case Avg =>
        val inner = self.toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Avg(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Ceil => commandexpressions.CeilFunction(self.toCommandExpression(invocation.arguments.head))
      case Coalesce => commandexpressions.CoalesceFunction(toCommandExpression(invocation.arguments, self): _*)
      case Collect =>
        val inner = self.toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Collect(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Cos => commandexpressions.CosFunction(self.toCommandExpression(invocation.arguments.head))
      case Cot => commandexpressions.CotFunction(self.toCommandExpression(invocation.arguments.head))
      case Count =>
        val inner = self.toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Count(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Degrees => commandexpressions.DegreesFunction(self.toCommandExpression(invocation.arguments.head))
      case E => commandexpressions.EFunction()
      case EndNode => commandexpressions.RelationshipEndPoints(self.toCommandExpression(invocation.arguments.head), start = false)
      case Exists =>
        invocation.arguments.head match {
          case property: ast.Property =>
            commands.predicates.PropertyExists(self.toCommandExpression(property.map), PropertyKey(property.propertyKey.name))
          case expression: ast.PatternExpression =>
            toCommandPredicate(expression)
          case expression: pipes.NestedPipeExpression =>
            toCommandPredicate(expression)
          case e: ast.ContainerIndex=>
            commandexpressions.ContainerIndex(self.toCommandExpression(e.expr), self.toCommandExpression(e.idx))
          case e: NestedPlanExpression =>
            commands.expressions.NestedPlanExpression(e.plan)
        }
      case Exp => commandexpressions.ExpFunction(self.toCommandExpression(invocation.arguments.head))
      case Floor => commandexpressions.FloorFunction(self.toCommandExpression(invocation.arguments.head))
      case Haversin => commandexpressions.HaversinFunction(self.toCommandExpression(invocation.arguments.head))
      case Head =>
        commandexpressions.ContainerIndex(
          self.toCommandExpression(invocation.arguments.head),
          commandexpressions.Literal(0)
        )
      case Id => commandexpressions.IdFunction(self.toCommandExpression(invocation.arguments.head))
      case Keys => commandexpressions.KeysFunction(self.toCommandExpression(invocation.arguments.head))
      case Labels => commandexpressions.LabelsFunction(self.toCommandExpression(invocation.arguments.head))
      case Last =>
        commandexpressions.ContainerIndex(
          self.toCommandExpression(invocation.arguments.head),
          commandexpressions.Literal(-1)
        )
      case Left =>
        commandexpressions.LeftFunction(
          self.toCommandExpression(invocation.arguments.head),
          self.toCommandExpression(invocation.arguments(1))
        )
      case Length => commandexpressions.LengthFunction(self.toCommandExpression(invocation.arguments.head))
      case Log => commandexpressions.LogFunction(self.toCommandExpression(invocation.arguments.head))
      case Log10 => commandexpressions.Log10Function(self.toCommandExpression(invocation.arguments.head))
      case LTrim => commandexpressions.LTrimFunction(self.toCommandExpression(invocation.arguments.head))
      case Max =>
        val inner = self.toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Max(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Min =>
        val inner = self.toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Min(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Nodes => commandexpressions.NodesFunction(self.toCommandExpression(invocation.arguments.head))
      case PercentileCont =>
        val firstArg = self.toCommandExpression(invocation.arguments.head)
        val secondArg = self.toCommandExpression(invocation.arguments(1))

        val command = commandexpressions.PercentileCont(firstArg, secondArg)
        if (invocation.distinct)
          commandexpressions.Distinct(command, firstArg)
        else
          command
      case PercentileDisc =>
        val firstArg = self.toCommandExpression(invocation.arguments.head)
        val secondArg = self.toCommandExpression(invocation.arguments(1))

        val command = commandexpressions.PercentileDisc(firstArg, secondArg)
        if (invocation.distinct)
          commandexpressions.Distinct(command, firstArg)
        else
          command
      case Pi => commandexpressions.PiFunction()
      case Distance =>
        val firstArg = self.toCommandExpression(invocation.arguments.head)
        val secondArg = self.toCommandExpression(invocation.arguments(1))
        commandexpressions.DistanceFunction(firstArg, secondArg)
      case Point => commandexpressions.PointFunction(self.toCommandExpression(invocation.arguments.head))
      case Radians => commandexpressions.RadiansFunction(self.toCommandExpression(invocation.arguments.head))
      case Rand => commandexpressions.RandFunction()
      case functions.Range =>
        commandexpressions.RangeFunction(
          self.toCommandExpression(invocation.arguments.head),
          self.toCommandExpression(invocation.arguments(1)),
          toCommandExpression(invocation.arguments.lift(2),self).getOrElse(commandexpressions.Literal(1))
        )
      case Relationships => commandexpressions.RelationshipFunction(self.toCommandExpression(invocation.arguments.head))
      case Replace =>
        commandexpressions.ReplaceFunction(
          self.toCommandExpression(invocation.arguments.head),
          self.toCommandExpression(invocation.arguments(1)),
          self.toCommandExpression(invocation.arguments(2))
        )
      case Reverse => commandexpressions.ReverseFunction(self.toCommandExpression(invocation.arguments.head))
      case Right =>
        commandexpressions.RightFunction(
          self.toCommandExpression(invocation.arguments.head),
          self.toCommandExpression(invocation.arguments(1))
        )
      case Round => commandexpressions.RoundFunction(self.toCommandExpression(invocation.arguments.head))
      case RTrim => commandexpressions.RTrimFunction(self.toCommandExpression(invocation.arguments.head))
      case Sign => commandexpressions.SignFunction(self.toCommandExpression(invocation.arguments.head))
      case Sin => commandexpressions.SinFunction(self.toCommandExpression(invocation.arguments.head))
      case Size => commandexpressions.SizeFunction(self.toCommandExpression(invocation.arguments.head))
      case Split =>
        commandexpressions.SplitFunction(
          self.toCommandExpression(invocation.arguments.head),
          self.toCommandExpression(invocation.arguments(1))
        )
      case Sqrt => commandexpressions.SqrtFunction(self.toCommandExpression(invocation.arguments.head))
      case StartNode => commandexpressions.RelationshipEndPoints(self.toCommandExpression(invocation.arguments.head), start = true)
      case StdDev =>
        val inner = self.toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Stdev(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case StdDevP =>
        val inner = self.toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.StdevP(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Substring =>
        commandexpressions.SubstringFunction(
          self.toCommandExpression(invocation.arguments.head),
          self.toCommandExpression(invocation.arguments(1)),
          toCommandExpression(invocation.arguments.lift(2), self)
        )
      case Sum =>
        val inner = self.toCommandExpression(invocation.arguments.head)
        val command = commandexpressions.Sum(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Tail =>
        commandexpressions.ListSlice(
          self.toCommandExpression(invocation.arguments.head),
          Some(commandexpressions.Literal(1)),
          None
        )
      case Tan => commandexpressions.TanFunction(self.toCommandExpression(invocation.arguments.head))
      case Timestamp => commandexpressions.TimestampFunction()
      case ToBoolean => commandexpressions.ToBooleanFunction(self.toCommandExpression(invocation.arguments.head))
      case ToFloat => commandexpressions.ToFloatFunction(self.toCommandExpression(invocation.arguments.head))
      case ToInteger => commandexpressions.ToIntegerFunction(self.toCommandExpression(invocation.arguments.head))
      case ToLower => commandexpressions.ToLowerFunction(self.toCommandExpression(invocation.arguments.head))
      case ToString => commandexpressions.ToStringFunction(self.toCommandExpression(invocation.arguments.head))
      case ToUpper => commandexpressions.ToUpperFunction(self.toCommandExpression(invocation.arguments.head))
      case Properties => commandexpressions.PropertiesFunction(self.toCommandExpression(invocation.arguments.head))
      case Trim => commandexpressions.TrimFunction(self.toCommandExpression(invocation.arguments.head))
      case Type => commandexpressions.RelationshipTypeFunction(self.toCommandExpression(invocation.arguments.head))
    }

  override def toCommandExpression(expression: ast.Expression, self: ExpressionConverters): CommandExpression =
    expression match {
    case e: ast.Null => commandexpressions.Null()
    case e: ast.True => predicates.True()
    case e: ast.False => predicates.Not(predicates.True())
    case e: ast.Literal => commandexpressions.Literal(e.value)
    case e: ast.Variable => variable(e)
    case e: ast.Or => predicates.Or(toCommandPredicate(e.lhs), toCommandPredicate(e.rhs))
    case e: ast.Xor => predicates.Xor(toCommandPredicate(e.lhs), toCommandPredicate(e.rhs))
    case e: ast.And => predicates.And(toCommandPredicate(e.lhs), toCommandPredicate(e.rhs))
    case e: ast.Ands => predicates.Ands(NonEmptyList.from(e.exprs.map(toCommandPredicate)))
    case e: ast.Ors => predicates.Ors(NonEmptyList.from(e.exprs.map(toCommandPredicate)))
    case e: ast.Not => predicates.Not(toCommandPredicate(e.rhs))
    case e: ast.Equals => predicates.Equals(self.toCommandExpression(e.lhs), self.toCommandExpression(e.rhs))
    case e: ast.NotEquals => predicates.Not(predicates.Equals(self.toCommandExpression(e.lhs), self.toCommandExpression(e.rhs)))
    case e: ast.RegexMatch => regexMatch(e, self)
    case e: ast.In => in(e, self)
    case e: ast.StartsWith => predicates.StartsWith(self.toCommandExpression(e.lhs), self.toCommandExpression(e.rhs))
    case e: ast.EndsWith => predicates.EndsWith(self.toCommandExpression(e.lhs), self.toCommandExpression(e.rhs))
    case e: ast.CoerceTo => commandexpressions.CoerceTo(self.toCommandExpression(e.expr), e.typ)
    case e: ast.Contains => predicates.Contains(self.toCommandExpression(e.lhs), self.toCommandExpression(e.rhs))
    case e: ast.IsNull => predicates.IsNull(self.toCommandExpression(e.lhs))
    case e: ast.IsNotNull => predicates.Not(predicates.IsNull(self.toCommandExpression(e.lhs)))
    case e: ast.InequalityExpression => inequalityExpression(e, self)
    case e: ast.Add => commandexpressions.Add(self.toCommandExpression(e.lhs), self.toCommandExpression(e.rhs))
    case e: ast.UnaryAdd => self.toCommandExpression(e.rhs)
    case e: ast.Subtract => commandexpressions.Subtract(self.toCommandExpression(e.lhs), self.toCommandExpression(e.rhs))
    case e: ast.UnarySubtract => commandexpressions.Subtract(commandexpressions.Literal(0), self.toCommandExpression(e.rhs))
    case e: ast.Multiply => commandexpressions.Multiply(self.toCommandExpression(e.lhs), self.toCommandExpression(e.rhs))
    case e: ast.Divide => commandexpressions.Divide(self.toCommandExpression(e.lhs), self.toCommandExpression(e.rhs))
    case e: ast.Modulo => commandexpressions.Modulo(self.toCommandExpression(e.lhs), self.toCommandExpression(e.rhs))
    case e: ast.Pow => commandexpressions.Pow(self.toCommandExpression(e.lhs), self.toCommandExpression(e.rhs))
    case e: ast.FunctionInvocation => toCommandExpression(e.function, e, self)
    case e: ast.CountStar => commandexpressions.CountStar()
    case e: ast.Property => toCommandProperty(e, self)
    case e: ast.Parameter => toCommandParameter(e)
    case e: ast.CaseExpression => caseExpression(e, self)
    case e: ast.PatternExpression =>
      val legacyPatterns = e.pattern.asLegacyPatterns(self)
      commands.PathExpression(legacyPatterns, predicates.True(), PathExtractorExpression(legacyPatterns), allowIntroducingNewIdentifiers = false)
    case e: ast.PatternComprehension => commands.PathExpression(e.pattern.asLegacyPatterns(self), toCommandPredicate(e.predicate), self.toCommandExpression(e.projection), allowIntroducingNewIdentifiers = true)
    case e: ast.ShortestPathExpression => commandexpressions.ShortestPathExpression(e.pattern.asLegacyPatterns(None, self).head)
    case e: ast.HasLabels => hasLabels(e, self)
    case e: ast.ListLiteral => commandexpressions.ListLiteral(toCommandExpression(e.expressions, self): _*)
    case e: ast.MapExpression => commandexpressions.LiteralMap(mapItems(e.items, self))
    case e: ast.ListSlice => commandexpressions.ListSlice(self.toCommandExpression(e.list), toCommandExpression(e.from, self), toCommandExpression(e.to, self))
    case e: ast.ContainerIndex => commandexpressions.ContainerIndex(self.toCommandExpression(e.expr), self.toCommandExpression(e.idx))
    case e: ast.FilterExpression => commandexpressions.FilterFunction(self.toCommandExpression(e.expression), e.variable.name, e.innerPredicate.map(toCommandPredicate).getOrElse(predicates.True()))
    case e: ast.ExtractExpression => commandexpressions.ExtractFunction(self.toCommandExpression(e.expression), e.variable.name, self.toCommandExpression(e.scope.extractExpression.get))
    case e: ast.ListComprehension => listComprehension(e, self)
    case e: ast.AllIterablePredicate => commands.AllInList(self.toCommandExpression(e.expression), e.variable.name, e.innerPredicate.map(toCommandPredicate).getOrElse(predicates.True()))
    case e: ast.AnyIterablePredicate => commands.AnyInList(self.toCommandExpression(e.expression), e.variable.name, e.innerPredicate.map(toCommandPredicate).getOrElse(predicates.True()))
    case e: ast.NoneIterablePredicate => commands.NoneInList(self.toCommandExpression(e.expression), e.variable.name, e.innerPredicate.map(toCommandPredicate).getOrElse(predicates.True()))
    case e: ast.SingleIterablePredicate => commands.SingleInList(self.toCommandExpression(e.expression), e.variable.name, e.innerPredicate.map(toCommandPredicate).getOrElse(predicates.True()))
    case e: ast.ReduceExpression => commandexpressions.ReduceFunction(self.toCommandExpression(e.list), e.variable.name, self.toCommandExpression(e.expression), e.accumulator.name, self.toCommandExpression(e.init))
    case e: ast.PathExpression => toCommandProjectedPath(e)
    case e: pipes.NestedPipeExpression => commandexpressions.NestedPipeExpression(e.pipe, self.toCommandExpression(e.projection))
    case e: ast.GetDegree => getDegree(e, self)
    case e: PrefixSeekRangeWrapper => commandexpressions.PrefixSeekRangeExpression(e.range.map(self.toCommandExpression))
    case e: InequalitySeekRangeWrapper => InequalitySeekRangeExpression(e.range.mapBounds(self.toCommandExpression))
    case e: ast.AndedPropertyInequalities => predicates.AndedPropertyComparablePredicates(variable(e.variable), toCommandProperty(e.property, self), e.inequalities.map(e => inequalityExpression(e, self)))
    case e: DesugaredMapProjection => commandexpressions.DesugaredMapProjection(e.name.name, e.includeAllProps, mapProjectionItems(e.items, self))
    case e: ResolvedFunctionInvocation =>
      val callArgumentCommands = e.callArguments.map(Some(_)).zipAll(e.fcnSignature.get.inputSignature.map(_.default.map(_.value)), None, None).map {
        case (given, default) => given.map(self.toCommandExpression).getOrElse(commandexpressions.Literal(default.get))
      }
      val signature = e.fcnSignature.get
      if (signature.isAggregate) commandexpressions.AggregationFunctionInvocation(signature, callArgumentCommands)
      else commandexpressions.FunctionInvocation(signature, callArgumentCommands)
    case e: ast.MapProjection => throw new InternalException("should have been rewritten away")
    case e: NestedPlanExpression => commandexpressions.NestedPlanExpression(e.plan)
    case _ =>
      throw new InternalException(s"Unknown expression type during transformation (${expression.getClass})")
  }

  private def toCommandPredicate(e: Option[ast.Expression]): Predicate =
    e.map(toCommandPredicate).getOrElse(predicates.True())

  private def toCommandParameter(e: ast.Parameter) = commandexpressions.ParameterExpression(e.name)

  private def toCommandProperty(e: ast.Property, self: ExpressionConverters): commandexpressions.Property =
    commandexpressions.Property(self.toCommandExpression(e.map), PropertyKey(e.propertyKey.name))

  private def toCommandExpression(expression: Option[ast.Expression], self: ExpressionConverters): Option[CommandExpression] =
    expression.map(self.toCommandExpression)

  private def toCommandExpression(expressions: Seq[ast.Expression], self: ExpressionConverters): Seq[CommandExpression] =
    expressions.map(self.toCommandExpression)

  private def variable(e: ast.Variable) = commands.expressions.Variable(e.name)

  private def inequalityExpression(original: ast.InequalityExpression, self: ExpressionConverters): predicates.ComparablePredicate = original match {
    case e: ast.LessThan => predicates.LessThan(self.toCommandExpression(e.lhs), self.toCommandExpression(e.rhs))
    case e: ast.LessThanOrEqual => predicates.LessThanOrEqual(self.toCommandExpression(e.lhs), self.toCommandExpression(e.rhs))
    case e: ast.GreaterThan => predicates.GreaterThan(self.toCommandExpression(e.lhs), self.toCommandExpression(e.rhs))
    case e: ast.GreaterThanOrEqual => predicates.GreaterThanOrEqual(self.toCommandExpression(e.lhs), self.toCommandExpression(e.rhs))
  }

  private def getDegree(original: ast.GetDegree, self: ExpressionConverters) = {
    val typ = original.relType.map(relType => UnresolvedRelType(relType.name))
    commandexpressions.GetDegree(self.toCommandExpression(original.node), typ, original.dir)
  }

  private def regexMatch(e: ast.RegexMatch, self: ExpressionConverters) = self.toCommandExpression(e.rhs) match {
    case literal: commandexpressions.Literal =>
      predicates.LiteralRegularExpression(self.toCommandExpression(e.lhs), literal)
    case command =>
      predicates.RegularExpression(self.toCommandExpression(e.lhs), command)
  }

  private def in(e: ast.In, self: ExpressionConverters) = e.rhs match {
    case value: Parameter =>
      predicates.ConstantCachedIn(self.toCommandExpression(e.lhs), self.toCommandExpression(value))

    case value@ListLiteral(expressions) if expressions.isEmpty =>
      predicates.Not(predicates.True())

    case value@ListLiteral(expressions) if expressions.forall(_.isInstanceOf[Literal]) =>
      predicates.ConstantCachedIn(self.toCommandExpression(e.lhs), self.toCommandExpression(value))

    case _ =>
      predicates.DynamicCachedIn(self.toCommandExpression(e.lhs), self.toCommandExpression(e.rhs))
  }

  private def caseExpression(e: ast.CaseExpression, self: ExpressionConverters) = e.expression match {
    case Some(innerExpression) =>
      val legacyAlternatives = e.alternatives.map { a => (self.toCommandExpression(a._1), self.toCommandExpression(a._2)) }
      commandexpressions.SimpleCase(self.toCommandExpression(innerExpression), legacyAlternatives, toCommandExpression(e.default, self))
    case None =>
      val predicateAlternatives = e.alternatives.map { a => (toCommandPredicate(a._1), self.toCommandExpression(a._2)) }
      commandexpressions.GenericCase(predicateAlternatives, toCommandExpression(e.default, self))
  }

  private def hasLabels(e: ast.HasLabels, self: ExpressionConverters) = e.labels.map {
    l => predicates.HasLabel(self.toCommandExpression(e.expression), commandvalues.KeyToken.Unresolved(l.name, commandvalues.TokenType.Label)): Predicate
  } reduceLeft {
    predicates.And(_, _)
  }

  private def mapItems(items: Seq[(PropertyKeyName, Expression)], self: ExpressionConverters): Map[String, CommandExpression] =
    items.map {
      case (id, ex) => id.name -> self.toCommandExpression(ex)
    }.toMap

  private def mapProjectionItems(items: Seq[LiteralEntry], self: ExpressionConverters): Map[String, CommandExpression] =
    items.map {
      case LiteralEntry(id, ex) => id.name -> self.toCommandExpression(ex)
    }.toMap

  private def listComprehension(e: ast.ListComprehension, self: ExpressionConverters): CommandExpression = {
    val filter = e.innerPredicate match {
      case Some(_: ast.True) | None =>
        self.toCommandExpression(e.expression)
      case Some(inner) =>
        commandexpressions.FilterFunction(self.toCommandExpression(e.expression), e.variable.name, toCommandPredicate(inner))
    }
    e.extractExpression match {
      case Some(extractExpression) =>
        commandexpressions.ExtractFunction(filter, e.variable.name, self.toCommandExpression(extractExpression))
      case None =>
        filter
    }
  }

  def toCommandProjectedPath(e: ast.PathExpression): ProjectedPath = {
    def project(pathStep: PathStep): Projector = pathStep match {

      case NodePathStep(Variable(node), next) =>
        singleNodeProjector(node, project(next))

      case SingleRelationshipPathStep(Variable(rel), SemanticDirection.INCOMING, next) =>
        singleIncomingRelationshipProjector(rel, project(next))

      case SingleRelationshipPathStep(Variable(rel), SemanticDirection.OUTGOING, next) =>
        singleOutgoingRelationshipProjector(rel, project(next))

      case SingleRelationshipPathStep(Variable(rel), SemanticDirection.BOTH, next) =>
        singleUndirectedRelationshipProjector(rel, project(next))

      case MultiRelationshipPathStep(Variable(rel), SemanticDirection.INCOMING, next) =>
        multiIncomingRelationshipProjector(rel, project(next))

      case MultiRelationshipPathStep(Variable(rel), SemanticDirection.OUTGOING, next) =>
        multiOutgoingRelationshipProjector(rel, project(next))

      case MultiRelationshipPathStep(Variable(rel), SemanticDirection.BOTH, next) =>
        multiUndirectedRelationshipProjector(rel, project(next))

      case NilPathStep =>
        nilProjector
    }

    val projector = project(e.step)
    val dependencies = e.step.dependencies.map(_.name)

    ProjectedPath(dependencies, projector)
  }
}
