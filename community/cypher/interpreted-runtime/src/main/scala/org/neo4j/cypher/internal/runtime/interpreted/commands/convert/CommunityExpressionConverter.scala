/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.runtime.interpreted.commands.convert

import org.neo4j.cypher.internal.planner.v3_5.spi.TokenContext
import org.neo4j.cypher.internal.runtime.interpreted._
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.InequalitySeekRangeExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.PointDistanceSeekRangeExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.{Expression => CommandExpression}
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.UnresolvedRelType
import org.neo4j.cypher.internal.runtime.interpreted.commands.PathExtractorExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates
import org.neo4j.cypher.internal.runtime.interpreted.commands.{values => commandvalues}
import org.neo4j.cypher.internal.runtime.interpreted.commands.{expressions => commandexpressions}
import org.neo4j.cypher.internal.v3_5.logical.plans._
import org.neo4j.cypher.internal.v3_5.expressions.functions._
import org.neo4j.cypher.internal.v3_5.expressions.DesugaredMapProjection
import org.neo4j.cypher.internal.v3_5.expressions.Expression
import org.neo4j.cypher.internal.v3_5.expressions.PropertyKeyName
import org.neo4j.cypher.internal.v3_5.expressions.functions
import org.neo4j.cypher.internal.v3_5.util.attribution.Id
import org.neo4j.cypher.internal.v3_5.util.InternalException
import org.neo4j.cypher.internal.v3_5.util.NonEmptyList
import org.neo4j.cypher.internal.v3_5.{expressions => ast}

case class CommunityExpressionConverter(tokenContext: TokenContext) extends ExpressionConverter {

  import PatternConverters._

  override def toCommandProjection(id: Id, projections: Map[String, Expression],
                                   self: ExpressionConverters): Option[CommandProjection] = {
    val projected = for ((k,Some(v)) <- projections.mapValues(e => toCommandExpression(id, e, self))) yield (k,v)
    if (projected.size < projections.size) None else Some(InterpretedCommandProjection(projected))
  }

  override def toCommandExpression(id: Id, expression: ast.Expression,
                                   self: ExpressionConverters): Option[CommandExpression] = {
    val result = expression match {
      case e: ast.Null => commandexpressions.Null()
      case e: ast.True => predicates.True()
      case e: ast.False => predicates.Not(predicates.True())
      case e: ast.Literal => commandexpressions.Literal(e.value)
      case e: ast.Variable => variable(e)
      case e: ast.Or => predicates.Or(self.toCommandPredicate(id, e.lhs), self.toCommandPredicate(id, e.rhs))
      case e: ast.Xor => predicates.Xor(self.toCommandPredicate(id, e.lhs), self.toCommandPredicate(id, e.rhs))
      case e: ast.And => predicates.And(self.toCommandPredicate(id, e.lhs), self.toCommandPredicate(id, e.rhs))
      case e: ast.Ands => predicates.Ands(NonEmptyList.from(e.exprs.map(self.toCommandPredicate(id,_))))
      case e: ast.Ors => predicates.Ors(NonEmptyList.from(e.exprs.map(self.toCommandPredicate(id,_))))
      case e: ast.Not => predicates.Not(self.toCommandPredicate(id, e.rhs))
      case e: ast.Equals => predicates.Equals(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: ast.NotEquals => predicates
        .Not(predicates.Equals(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs)))
      case e: ast.RegexMatch => regexMatch(id, e, self)
      case e: ast.In => in(id, e, self)
      case e: ast.StartsWith => predicates.StartsWith(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: ast.EndsWith => predicates.EndsWith(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: ast.CoerceTo => commandexpressions.CoerceTo(self.toCommandExpression(id, e.expr), e.typ)
      case e: ast.Contains => predicates.Contains(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: ast.IsNull => predicates.IsNull(self.toCommandExpression(id, e.lhs))
      case e: ast.IsNotNull => predicates.Not(predicates.IsNull(self.toCommandExpression(id, e.lhs)))
      case e: ast.InequalityExpression => inequalityExpression(id, e, self)
      case e: ast.Add => commandexpressions.Add(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: ast.UnaryAdd => self.toCommandExpression(id, e.rhs)
      case e: ast.Subtract => commandexpressions
        .Subtract(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: ast.UnarySubtract => commandexpressions
        .Subtract(commandexpressions.Literal(0), self.toCommandExpression(id, e.rhs))
      case e: ast.Multiply => commandexpressions
        .Multiply(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: ast.Divide => commandexpressions.Divide(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: ast.Modulo => commandexpressions.Modulo(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: ast.Pow => commandexpressions.Pow(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: ast.FunctionInvocation => toCommandExpression(id, e.function, e, self)
      case e: ast.CountStar => commandexpressions.CountStar()
      case e: ast.Property => toCommandProperty(id, e, self)
      case e: CachedNodeProperty => commandexpressions.CachedNodeProperty(e.nodeVariableName, getPropertyKey(e.propertyKey), e)
      case e: ast.Parameter => toCommandParameter(e)
      case e: ast.CaseExpression => caseExpression(id, e, self)
      case e: ast.PatternExpression =>
        val legacyPatterns = e.pattern.asLegacyPatterns(id, self)
        commands.PathExpression(legacyPatterns, predicates.True(), PathExtractorExpression(legacyPatterns),
                                allowIntroducingNewIdentifiers = false)
      case e: ast.PatternComprehension => commands
        .PathExpression(e.pattern.asLegacyPatterns(id, self), self.toCommandPredicate(id, e.predicate),
                        self.toCommandExpression(id, e.projection), allowIntroducingNewIdentifiers = true)
      case e: ast.ShortestPathExpression => commandexpressions
        .ShortestPathExpression(e.pattern.asLegacyPatterns(id, None, self).head)
      case e: ast.HasLabels => hasLabels(id, e, self)
      case e: ast.ListLiteral => commandexpressions.ListLiteral(toCommandExpression(id, e.expressions, self): _*)
      case e: ast.MapExpression => commandexpressions.LiteralMap(mapItems(id, e.items, self))
      case e: ast.ListSlice => commandexpressions
        .ListSlice(self.toCommandExpression(id, e.list), toCommandExpression(id, e.from, self), toCommandExpression(id, e.to, self))
      case e: ast.ContainerIndex => commandexpressions
        .ContainerIndex(self.toCommandExpression(id, e.expr), self.toCommandExpression(id, e.idx))
      case e: ast.FilterExpression => commandexpressions
        .FilterFunction(self.toCommandExpression(id, e.expression), e.variable.name,
                        e.innerPredicate.map(self.toCommandPredicate(id, _)).getOrElse(predicates.True()))
      case e: ast.ExtractExpression => commandexpressions
        .ExtractFunction(self.toCommandExpression(id, e.expression), e.variable.name,
                         self.toCommandExpression(id, e.scope.extractExpression.get))
      case e: ast.ListComprehension => listComprehension(id, e, self)
      case e: ast.AllIterablePredicate => commands.AllInList(self.toCommandExpression(id, e.expression), e.variable.name,
                                                             e.innerPredicate.map(self.toCommandPredicate(id,_))
                                                               .getOrElse(predicates.True()))
      case e: ast.AnyIterablePredicate => commands.AnyInList(self.toCommandExpression(id, e.expression), e.variable.name,
                                                             e.innerPredicate.map(self.toCommandPredicate(id,_))
                                                               .getOrElse(predicates.True()))
      case e: ast.NoneIterablePredicate => commands.NoneInList(self.toCommandExpression(id, e.expression), e.variable.name,
                                                               e.innerPredicate.map(self.toCommandPredicate(id,_))
                                                                 .getOrElse(predicates.True()))
      case e: ast.SingleIterablePredicate => commands
        .SingleInList(self.toCommandExpression(id, e.expression), e.variable.name,
                      e.innerPredicate.map(self.toCommandPredicate(id,_)).getOrElse(predicates.True()))
      case e: ast.ReduceExpression => commandexpressions
        .ReduceFunction(self.toCommandExpression(id, e.list), e.variable.name, self.toCommandExpression(id, e.expression),
                        e.accumulator.name, self.toCommandExpression(id, e.init))
      case e: ast.PathExpression => self.toCommandProjectedPath(e)
      case e: pipes.NestedPipeExpression => commandexpressions
        .NestedPipeExpression(e.pipe, self.toCommandExpression(id, e.projection))
      case e: ast.GetDegree => getDegree(id, e, self)
      case e: PrefixSeekRangeWrapper => commandexpressions
        .PrefixSeekRangeExpression(e.range.map(self.toCommandExpression(id,_)))
      case e: InequalitySeekRangeWrapper => InequalitySeekRangeExpression(e.range.mapBounds(self.toCommandExpression(id,_)))
      case e: PointDistanceSeekRangeWrapper => PointDistanceSeekRangeExpression(e.range.map(self.toCommandExpression(id,_)))
      case e: ast.AndedPropertyInequalities => predicates
        .AndedPropertyComparablePredicates(variable(e.variable), toCommandProperty(id, e.property, self),
                                           e.inequalities.map(e => inequalityExpression(id, e, self)))
      case e: DesugaredMapProjection => commandexpressions
        .DesugaredMapProjection(e.name.name, e.includeAllProps, mapProjectionItems(id, e.items, self))
      case e: ResolvedFunctionInvocation =>
        val callArgumentCommands = e.callArguments.map(Some(_))
          .zipAll(e.fcnSignature.get.inputSignature.map(_.default.map(_.value)), None, None).map {
          case (given, default) => given.map(self.toCommandExpression(id,_))
            .getOrElse(commandexpressions.Literal(default.get))
        }
        val signature = e.fcnSignature.get
        (signature.isAggregate, signature.id) match {
          case (true, Some(_)) => commandexpressions.AggregationFunctionInvocationById(signature, callArgumentCommands)
          case (true, None) => commandexpressions.AggregationFunctionInvocationByName(signature, callArgumentCommands)
          case (false, Some(_)) => commandexpressions.FunctionInvocationById(signature, callArgumentCommands)
          case (false, None) => commandexpressions.FunctionInvocationByName(signature, callArgumentCommands)
        }
      case e: ast.MapProjection => throw new InternalException("should have been rewritten away")
      case e: NestedPlanExpression => commandexpressions.NestedPlanExpression(e.plan)
      case CoerceToPredicate(inner) => predicates.CoercedPredicate(self.toCommandExpression(id, inner))
      case _ => null
    }

    Option(result)
  }

  private def toCommandExpression(id: Id, expression: Function, invocation: ast.FunctionInvocation,
                                  self: ExpressionConverters): CommandExpression =
    expression match {
      case Abs => commandexpressions.AbsFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Acos => commandexpressions.AcosFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Asin => commandexpressions.AsinFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Atan => commandexpressions.AtanFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Atan2 =>
        commandexpressions.Atan2Function(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1)))
      case Avg =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commandexpressions.Avg(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Ceil => commandexpressions.CeilFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Coalesce => commandexpressions.CoalesceFunction(toCommandExpression(id, invocation.arguments, self): _*)
      case Collect =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commandexpressions.Collect(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Cos => commandexpressions.CosFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Cot => commandexpressions.CotFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Count =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commandexpressions.Count(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Degrees => commandexpressions.DegreesFunction(self.toCommandExpression(id, invocation.arguments.head))
      case E => commandexpressions.EFunction()
      case EndNode => commandexpressions
        .RelationshipEndPoints(self.toCommandExpression(id, invocation.arguments.head), start = false)
      case Exists =>
        invocation.arguments.head match {
          case property: ast.Property =>
            val propertyKey = getPropertyKey(property.propertyKey)
            commands.predicates.PropertyExists(self.toCommandExpression(id, property.map), propertyKey)
          case property: ASTCachedNodeProperty =>
            commands.predicates.CachedNodePropertyExists(self.toCommandExpression(id, property))
          case expression: ast.PatternExpression =>
            self.toCommandPredicate(id, expression)
          case expression: pipes.NestedPipeExpression =>
            self.toCommandPredicate(id, expression)
          case e: ast.ContainerIndex =>
            commandexpressions.ContainerIndex(self.toCommandExpression(id, e.expr), self.toCommandExpression(id, e.idx))
          case e: NestedPlanExpression =>
            commands.expressions.NestedPlanExpression(e.plan)
        }
      case Exp => commandexpressions.ExpFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Floor => commandexpressions.FloorFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Haversin => commandexpressions.HaversinFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Head =>
        commandexpressions.ContainerIndex(
          self.toCommandExpression(id, invocation.arguments.head),
          commandexpressions.Literal(0)
        )
      case functions.Id => commandexpressions.IdFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Keys => commandexpressions.KeysFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Labels => commandexpressions.LabelsFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Last =>
        commandexpressions.ContainerIndex(
          self.toCommandExpression(id, invocation.arguments.head),
          commandexpressions.Literal(-1)
        )
      case Left =>
        commandexpressions.LeftFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1))
        )
      case Length => commandexpressions.LengthFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Log => commandexpressions.LogFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Log10 => commandexpressions.Log10Function(self.toCommandExpression(id, invocation.arguments.head))
      case LTrim => commandexpressions.LTrimFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Max =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commandexpressions.Max(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Min =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commandexpressions.Min(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Nodes => commandexpressions.NodesFunction(self.toCommandExpression(id, invocation.arguments.head))
      case PercentileCont =>
        val firstArg = self.toCommandExpression(id, invocation.arguments.head)
        val secondArg = self.toCommandExpression(id, invocation.arguments(1))

        val command = commandexpressions.PercentileCont(firstArg, secondArg)
        if (invocation.distinct)
          commandexpressions.Distinct(command, firstArg)
        else
          command
      case PercentileDisc =>
        val firstArg = self.toCommandExpression(id, invocation.arguments.head)
        val secondArg = self.toCommandExpression(id, invocation.arguments(1))

        val command = commandexpressions.PercentileDisc(firstArg, secondArg)
        if (invocation.distinct)
          commandexpressions.Distinct(command, firstArg)
        else
          command
      case Pi => commandexpressions.PiFunction()
      case Distance =>
        val firstArg = self.toCommandExpression(id, invocation.arguments.head)
        val secondArg = self.toCommandExpression(id, invocation.arguments(1))
        commandexpressions.DistanceFunction(firstArg, secondArg)
      case Point => commandexpressions.PointFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Radians => commandexpressions.RadiansFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Rand => commandexpressions.RandFunction()
      case functions.Range =>
        commandexpressions.RangeFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1)),
          toCommandExpression(id, invocation.arguments.lift(2), self).getOrElse(commandexpressions.Literal(1))
        )
      case Relationships => commandexpressions.RelationshipFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Replace =>
        commandexpressions.ReplaceFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1)),
          self.toCommandExpression(id, invocation.arguments(2))
        )
      case Reverse => commandexpressions.ReverseFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Right =>
        commandexpressions.RightFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1))
        )
      case Round => commandexpressions.RoundFunction(self.toCommandExpression(id, invocation.arguments.head))
      case RTrim => commandexpressions.RTrimFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Sign => commandexpressions.SignFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Sin => commandexpressions.SinFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Size => commandexpressions.SizeFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Split =>
        commandexpressions.SplitFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1))
        )
      case Sqrt => commandexpressions.SqrtFunction(self.toCommandExpression(id, invocation.arguments.head))
      case StartNode => commandexpressions
        .RelationshipEndPoints(self.toCommandExpression(id, invocation.arguments.head), start = true)
      case StdDev =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commandexpressions.Stdev(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case StdDevP =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commandexpressions.StdevP(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Substring =>
        commandexpressions.SubstringFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1)),
          toCommandExpression(id, invocation.arguments.lift(2), self)
        )
      case Sum =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commandexpressions.Sum(inner)
        if (invocation.distinct)
          commandexpressions.Distinct(command, inner)
        else
          command
      case Tail =>
        commandexpressions.ListSlice(
          self.toCommandExpression(id, invocation.arguments.head),
          Some(commandexpressions.Literal(1)),
          None
        )
      case Tan => commandexpressions.TanFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToBoolean => commandexpressions.ToBooleanFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToFloat => commandexpressions.ToFloatFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToInteger => commandexpressions.ToIntegerFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToLower => commandexpressions.ToLowerFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToString => commandexpressions.ToStringFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToUpper => commandexpressions.ToUpperFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Properties => commandexpressions.PropertiesFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Trim => commandexpressions.TrimFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Type => commandexpressions.RelationshipTypeFunction(self.toCommandExpression(id, invocation.arguments.head))
    }

  private def toCommandParameter(e: ast.Parameter) = commandexpressions.ParameterExpression(e.name)

  private def toCommandProperty(id: Id, e: ast.LogicalProperty, self: ExpressionConverters): commandexpressions.Property =
    commandexpressions.Property(self.toCommandExpression(id, e.map), getPropertyKey(e.propertyKey))

  private def toCommandExpression(id: Id, expression: Option[ast.Expression],
                                  self: ExpressionConverters): Option[CommandExpression] =
    expression.map(self.toCommandExpression(id,_))

  private def toCommandExpression(id: Id, expressions: Seq[ast.Expression],
                                  self: ExpressionConverters): Seq[CommandExpression] =
    expressions.map(self.toCommandExpression(id,_))

  private def variable(e: ast.LogicalVariable) = commands.expressions.Variable(e.name)

  private def inequalityExpression(id: Id, original: ast.InequalityExpression,
                                   self: ExpressionConverters): predicates.ComparablePredicate = original match {
    case e: ast.LessThan => predicates.LessThan(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
    case e: ast.LessThanOrEqual => predicates
      .LessThanOrEqual(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
    case e: ast.GreaterThan => predicates.GreaterThan(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
    case e: ast.GreaterThanOrEqual => predicates
      .GreaterThanOrEqual(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
  }

  private def getDegree(id: Id, original: ast.GetDegree, self: ExpressionConverters) = {
    val typ = original.relType.map(relType => UnresolvedRelType(relType.name))
    commandexpressions.GetDegree(self.toCommandExpression(id, original.node), typ, original.dir)
  }

  private def regexMatch(id: Id, e: ast.RegexMatch, self: ExpressionConverters) = self.toCommandExpression(id, e.rhs) match {
    case literal: commandexpressions.Literal =>
      predicates.LiteralRegularExpression(self.toCommandExpression(id, e.lhs), literal)
    case command =>
      predicates.RegularExpression(self.toCommandExpression(id, e.lhs), command)
  }

  private def in(id: Id, e: ast.In, self: ExpressionConverters) = e.rhs match {
    case value: ast.Parameter =>
      predicates.ConstantCachedIn(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, value))

    case value@ast.ListLiteral(expressions) if expressions.isEmpty =>
      predicates.Not(predicates.True())

    case value@ast.ListLiteral(expressions) if expressions.forall(_.isInstanceOf[ast.Literal]) =>
      predicates.ConstantCachedIn(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, value))

    case _ =>
      predicates.DynamicCachedIn(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
  }

  private def caseExpression(id: Id, e: ast.CaseExpression, self: ExpressionConverters) = e.expression match {
    case Some(innerExpression) =>
      val legacyAlternatives = e.alternatives
        .map { a => (self.toCommandExpression(id, a._1), self.toCommandExpression(id, a._2)) }
      commandexpressions
        .SimpleCase(self.toCommandExpression(id, innerExpression), legacyAlternatives, toCommandExpression(id, e.default, self))
    case None =>
      val predicateAlternatives = e.alternatives
        .map { a => (self.toCommandPredicate(id, a._1), self.toCommandExpression(id, a._2)) }
      commandexpressions.GenericCase(predicateAlternatives, toCommandExpression(id, e.default, self))
  }

  private def hasLabels(id: Id, e: ast.HasLabels, self: ExpressionConverters) = e.labels.map {
    l => predicates.HasLabel(self.toCommandExpression(id, e.expression),
                             commandvalues.KeyToken.Unresolved(l.name, commandvalues.TokenType.Label)): Predicate
  } reduceLeft {
    predicates.And(_, _)
  }

  private def mapItems(id: Id, items: Seq[(ast.PropertyKeyName, ast.Expression)],
                       self: ExpressionConverters): Map[String, CommandExpression] =
    items.map {
      case (name, ex) => name.name -> self.toCommandExpression(id, ex)
    }.toMap

  private def mapProjectionItems(id: Id, items: Seq[ast.LiteralEntry],
                                 self: ExpressionConverters): Map[String, CommandExpression] =
    items.map {
      case ast.LiteralEntry(name, ex) => name.name -> self.toCommandExpression(id, ex)
    }.toMap

  private def listComprehension(id: Id, e: ast.ListComprehension, self: ExpressionConverters): CommandExpression = {
    val filter = e.innerPredicate match {
      case Some(_: ast.True) | None =>
        self.toCommandExpression(id, e.expression)
      case Some(inner) =>
        commandexpressions
          .FilterFunction(self.toCommandExpression(id, e.expression), e.variable.name, self.toCommandPredicate(id, inner))
    }
    e.extractExpression match {
      case Some(extractExpression) =>
        commandexpressions.ExtractFunction(filter, e.variable.name, self.toCommandExpression(id, extractExpression))
      case None =>
        filter
    }
  }

  private def getPropertyKey(propertyKey: PropertyKeyName) = tokenContext.getOptPropertyKeyId(propertyKey.name) match {
    case Some(propertyKeyId) =>
      PropertyKey(propertyKey.name, propertyKeyId)
    case _ =>
      PropertyKey(propertyKey.name)
  }
}
