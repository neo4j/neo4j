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

import org.neo4j.cypher.internal
import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.expressions.DesugaredMapProjection
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.functions
import org.neo4j.cypher.internal.expressions.functions.Abs
import org.neo4j.cypher.internal.expressions.functions.Acos
import org.neo4j.cypher.internal.expressions.functions.Asin
import org.neo4j.cypher.internal.expressions.functions.Atan
import org.neo4j.cypher.internal.expressions.functions.Atan2
import org.neo4j.cypher.internal.expressions.functions.Avg
import org.neo4j.cypher.internal.expressions.functions.Ceil
import org.neo4j.cypher.internal.expressions.functions.Coalesce
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.expressions.functions.Cos
import org.neo4j.cypher.internal.expressions.functions.Cot
import org.neo4j.cypher.internal.expressions.functions.Count
import org.neo4j.cypher.internal.expressions.functions.Degrees
import org.neo4j.cypher.internal.expressions.functions.Distance
import org.neo4j.cypher.internal.expressions.functions.E
import org.neo4j.cypher.internal.expressions.functions.EndNode
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.expressions.functions.Exp
import org.neo4j.cypher.internal.expressions.functions.File
import org.neo4j.cypher.internal.expressions.functions.Floor
import org.neo4j.cypher.internal.expressions.functions.Function
import org.neo4j.cypher.internal.expressions.functions.Haversin
import org.neo4j.cypher.internal.expressions.functions.Head
import org.neo4j.cypher.internal.expressions.functions.Keys
import org.neo4j.cypher.internal.expressions.functions.LTrim
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Last
import org.neo4j.cypher.internal.expressions.functions.Left
import org.neo4j.cypher.internal.expressions.functions.Length
import org.neo4j.cypher.internal.expressions.functions.Linenumber
import org.neo4j.cypher.internal.expressions.functions.Log
import org.neo4j.cypher.internal.expressions.functions.Log10
import org.neo4j.cypher.internal.expressions.functions.Max
import org.neo4j.cypher.internal.expressions.functions.Min
import org.neo4j.cypher.internal.expressions.functions.Nodes
import org.neo4j.cypher.internal.expressions.functions.PercentileCont
import org.neo4j.cypher.internal.expressions.functions.PercentileDisc
import org.neo4j.cypher.internal.expressions.functions.Pi
import org.neo4j.cypher.internal.expressions.functions.Point
import org.neo4j.cypher.internal.expressions.functions.Properties
import org.neo4j.cypher.internal.expressions.functions.RTrim
import org.neo4j.cypher.internal.expressions.functions.Radians
import org.neo4j.cypher.internal.expressions.functions.Rand
import org.neo4j.cypher.internal.expressions.functions.Relationships
import org.neo4j.cypher.internal.expressions.functions.Replace
import org.neo4j.cypher.internal.expressions.functions.Reverse
import org.neo4j.cypher.internal.expressions.functions.Right
import org.neo4j.cypher.internal.expressions.functions.Round
import org.neo4j.cypher.internal.expressions.functions.Sign
import org.neo4j.cypher.internal.expressions.functions.Sin
import org.neo4j.cypher.internal.expressions.functions.Size
import org.neo4j.cypher.internal.expressions.functions.Split
import org.neo4j.cypher.internal.expressions.functions.Sqrt
import org.neo4j.cypher.internal.expressions.functions.StartNode
import org.neo4j.cypher.internal.expressions.functions.StdDev
import org.neo4j.cypher.internal.expressions.functions.StdDevP
import org.neo4j.cypher.internal.expressions.functions.Substring
import org.neo4j.cypher.internal.expressions.functions.Sum
import org.neo4j.cypher.internal.expressions.functions.Tail
import org.neo4j.cypher.internal.expressions.functions.Tan
import org.neo4j.cypher.internal.expressions.functions.ToBoolean
import org.neo4j.cypher.internal.expressions.functions.ToFloat
import org.neo4j.cypher.internal.expressions.functions.ToInteger
import org.neo4j.cypher.internal.expressions.functions.ToLower
import org.neo4j.cypher.internal.expressions.functions.ToString
import org.neo4j.cypher.internal.expressions.functions.ToUpper
import org.neo4j.cypher.internal.expressions.functions.Trim
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.logical.plans.CoerceToPredicate
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PrefixSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.planner.spi.TokenContext
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.runtime.interpreted.CommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.PatternConverters.ShortestPathsConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.InequalitySeekRangeExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.PointDistanceSeekRangeExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.VariableCommand
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.UnresolvedRelType
import org.neo4j.cypher.internal.runtime.interpreted.pipes
import org.neo4j.cypher.internal.util.NonEmptyList
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.exceptions.InternalException

case class CommunityExpressionConverter(tokenContext: TokenContext) extends ExpressionConverter {

  override def toCommandProjection(id: Id, projections: Map[String, Expression],
                                   self: ExpressionConverters): Option[CommandProjection] = {
    val projected = for ((k,Some(v)) <- projections.mapValues(e => toCommandExpression(id, e, self))) yield (k,v)
    if (projected.size < projections.size) None else Some(InterpretedCommandProjection(projected))
  }


  override def toGroupingExpression(id: Id,
                                    groupings: Map[String, Expression],
                                    orderToLeverage: Seq[Expression],
                                    self: ExpressionConverters): Option[GroupingExpression] = {
    throw new IllegalStateException("CommunityExpressionConverter cannot create grouping expressions")
  }

  override def toCommandExpression(id: Id, expression: internal.expressions.Expression,
                                   self: ExpressionConverters): Option[commands.expressions.Expression] = {
    val result = expression match {
      case _: internal.expressions.Null => commands.expressions.Null()
      case _: internal.expressions.True => predicates.True()
      case _: internal.expressions.False => predicates.Not(predicates.True())
      case e: internal.expressions.Literal => commands.expressions.Literal(e.value)
      case e: internal.expressions.Variable => variable(e)
      case e: ExpressionVariable => commands.expressions.ExpressionVariable.of(e)
      case e: internal.expressions.Or => predicates.Or(self.toCommandPredicate(id, e.lhs), self.toCommandPredicate(id, e.rhs))
      case e: internal.expressions.Xor => predicates.Xor(self.toCommandPredicate(id, e.lhs), self.toCommandPredicate(id, e.rhs))
      case e: internal.expressions.And => predicates.And(self.toCommandPredicate(id, e.lhs), self.toCommandPredicate(id, e.rhs))
      case e: internal.expressions.Ands => predicates.Ands(NonEmptyList.from(e.exprs.map(self.toCommandPredicate(id,_))))
      case e: internal.expressions.Ors => predicates.Ors(NonEmptyList.from(e.exprs.map(self.toCommandPredicate(id,_))))
      case e: internal.expressions.Not => predicates.Not(self.toCommandPredicate(id, e.rhs))
      case e: internal.expressions.Equals => predicates.Equals(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.NotEquals => predicates
        .Not(predicates.Equals(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs)))
      case e: internal.expressions.RegexMatch => regexMatch(id, e, self)
      case e: internal.expressions.In => in(id, e, self)
      case e: internal.expressions.StartsWith => predicates.StartsWith(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.EndsWith => predicates.EndsWith(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.CoerceTo => commands.expressions.CoerceTo(self.toCommandExpression(id, e.expr), e.typ)
      case e: internal.expressions.Contains => predicates.Contains(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.IsNull => predicates.IsNull(self.toCommandExpression(id, e.lhs))
      case e: internal.expressions.IsNotNull => predicates.Not(predicates.IsNull(self.toCommandExpression(id, e.lhs)))
      case e: internal.expressions.InequalityExpression => inequalityExpression(id, e, self)
      case e: internal.expressions.Add => commands.expressions.Add(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.UnaryAdd => self.toCommandExpression(id, e.rhs)
      case e: internal.expressions.Subtract => commands.expressions
        .Subtract(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.UnarySubtract => commands.expressions
        .Subtract(commands.expressions.Literal(0), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.Multiply => commands.expressions
        .Multiply(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.Divide => commands.expressions.Divide(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.Modulo => commands.expressions.Modulo(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.Pow => commands.expressions.Pow(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.FunctionInvocation => toCommandExpression(id, e.function, e, self)
      case _: internal.expressions.CountStar => commands.expressions.CountStar()
      case e: internal.expressions.LogicalProperty => toCommandProperty(id, e, self)
      case ParameterFromSlot(offset, name, _) => commands.expressions.ParameterFromSlot(offset, name)
      case e: internal.expressions.CaseExpression => caseExpression(id, e, self)
      case e: internal.expressions.ShortestPathExpression => commands.expressions
        .ShortestPathExpression(e.pattern.asLegacyPatterns(id, None, self).head, operatorId = id)
      case e: internal.expressions.HasLabels => hasLabels(id, e, self)
      case e: internal.expressions.ListLiteral => commands.expressions.ListLiteral(toCommandExpression(id, e.expressions, self): _*)
      case e: internal.expressions.MapExpression => commands.expressions.LiteralMap(mapItems(id, e.items, self))
      case e: internal.expressions.ListSlice => commands.expressions
        .ListSlice(self.toCommandExpression(id, e.list), toCommandExpression(id, e.from, self), toCommandExpression(id, e.to, self))
      case e: internal.expressions.ContainerIndex => commands.expressions
        .ContainerIndex(self.toCommandExpression(id, e.expr), self.toCommandExpression(id, e.idx))

      case e: internal.expressions.ListComprehension => listComprehension(id, e, self)
      case e: internal.expressions.AllIterablePredicate =>
        val ev = ExpressionVariable.cast(e.variable)
        commands.AllInList(self.toCommandExpression(id, e.expression),
          ev.name,
          ev.offset,
          e.innerPredicate.map(self.toCommandPredicate(id, _)).getOrElse(predicates.True()))

      case e: internal.expressions.AnyIterablePredicate =>
        val ev = ExpressionVariable.cast(e.variable)
        commands.AnyInList(self.toCommandExpression(id, e.expression),
          ev.name,
          ev.offset,
          e.innerPredicate.map(self.toCommandPredicate(id, _)).getOrElse(predicates.True()))

      case e: internal.expressions.NoneIterablePredicate =>
        val ev = ExpressionVariable.cast(e.variable)
        commands.NoneInList(self.toCommandExpression(id, e.expression),
          ev.name,
          ev.offset,
          e.innerPredicate.map(self.toCommandPredicate(id, _)).getOrElse(predicates.True()))

      case e: internal.expressions.SingleIterablePredicate =>
        val ev = ExpressionVariable.cast(e.variable)
        commands.SingleInList(self.toCommandExpression(id, e.expression),
          ev.name,
          ev.offset,
          e.innerPredicate.map(self.toCommandPredicate(id, _)).getOrElse(predicates.True()))

      case e: internal.expressions.ReduceExpression =>
        val innerVariable = ExpressionVariable.cast(e.variable)
        val accVariable = ExpressionVariable.cast(e.accumulator)
        commands.expressions.ReduceFunction(self.toCommandExpression(id, e.list),
          innerVariable.name,
          innerVariable.offset,
          self.toCommandExpression(id, e.expression),
          accVariable.name,
          accVariable.offset,
          self.toCommandExpression(id, e.init))

      case e: internal.expressions.PathExpression => self.toCommandProjectedPath(e)
      case e: pipes.NestedPipeCollectExpression =>
        commands.expressions.NestedPipeCollectExpression(
          e.pipe,
          self.toCommandExpression(id, e.projection),
          e.availableExpressionVariables.map(commands.expressions.ExpressionVariable.of).toArray,
          id)
      case e: pipes.NestedPipeExistsExpression =>
        commands.expressions.NestedPipeExistsExpression(
          e.pipe,
          e.availableExpressionVariables.map(commands.expressions.ExpressionVariable.of).toArray,
          id)

      case e: internal.expressions.GetDegree => getDegree(id, e, self)
      case e: PrefixSeekRangeWrapper => commands.expressions
        .PrefixSeekRangeExpression(e.range.map(self.toCommandExpression(id,_)))
      case e: InequalitySeekRangeWrapper => InequalitySeekRangeExpression(e.range.mapBounds(self.toCommandExpression(id,_)))
      case e: PointDistanceSeekRangeWrapper => PointDistanceSeekRangeExpression(e.range.map(self.toCommandExpression(id,_)))
      case e: internal.expressions.AndedPropertyInequalities => predicates
        .AndedPropertyComparablePredicates(variable(e.variable), toCommandProperty(id, e.property, self),
          e.inequalities.map(e => inequalityExpression(id, e, self)))
      case e: DesugaredMapProjection => commands.expressions
        .DesugaredMapProjection(variable(e.variable), e.includeAllProps, mapProjectionItems(id, e.items, self))
      case e: ResolvedFunctionInvocation =>
        val callArgumentCommands = e.callArguments.map(Some(_))
          .zipAll(e.fcnSignature.get.inputSignature.map(_.default.map(_.value)), None, None).map {
          case (given, default) => given.map(self.toCommandExpression(id,_))
            .getOrElse(commands.expressions.Literal(default.get))
        }
        val signature = e.fcnSignature.get
        if (signature.isAggregate)
          commands.expressions.AggregationFunctionInvocation(signature, callArgumentCommands)
        else
          commands.expressions.FunctionInvocation(signature, callArgumentCommands.toArray)
      case _: internal.expressions.MapProjection => throw new InternalException("`MapProjection` should have been rewritten away")
      case _: internal.expressions.PatternComprehension => throw new InternalException("`PatternComprehension` should have been rewritten away")
      case _: internal.expressions.PatternExpression => throw new InternalException("`PatternExpression` should have been rewritten away")
      case _: NestedPlanExpression => throw new InternalException("`NestedPlanExpression` should have been rewritten away")
      case _: internal.expressions.Parameter => throw new InternalException("`Parameter` should have been rewritten away")
      case _: ExistsSubClause => throw new InternalException("`ExistsSubClause` should have been rewritten away")
      case CoerceToPredicate(inner) => predicates.CoercedPredicate(self.toCommandExpression(id, inner))
      case _ => null
    }

    Option(result)
  }

  private def toCommandExpression(id: Id, expression: Function, invocation: internal.expressions.FunctionInvocation,
                                  self: ExpressionConverters): commands.expressions.Expression =
    expression match {
      case Abs => commands.expressions.AbsFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Acos => commands.expressions.AcosFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Asin => commands.expressions.AsinFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Atan => commands.expressions.AtanFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Atan2 =>
        commands.expressions.Atan2Function(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1)))
      case Avg =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commands.expressions.Avg(inner)
        if (invocation.distinct)
          commands.expressions.Distinct(command, inner)
        else
          command
      case Ceil => commands.expressions.CeilFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Coalesce => commands.expressions.CoalesceFunction(toCommandExpression(id, invocation.arguments, self): _*)
      case Collect =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commands.expressions.Collect(inner)
        if (invocation.distinct)
          commands.expressions.Distinct(command, inner)
        else
          command
      case Cos => commands.expressions.CosFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Cot => commands.expressions.CotFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Count =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commands.expressions.Count(inner)
        if (invocation.distinct)
          commands.expressions.Distinct(command, inner)
        else
          command
      case Degrees => commands.expressions.DegreesFunction(self.toCommandExpression(id, invocation.arguments.head))
      case E => commands.expressions.EFunction()
      case EndNode => commands.expressions
        .RelationshipEndPoints(self.toCommandExpression(id, invocation.arguments.head), start = false)
      case Exists =>
        invocation.arguments.head match {
          case property: internal.expressions.Property =>
            val propertyKey = getPropertyKey(property.propertyKey)
            commands.predicates.PropertyExists(self.toCommandExpression(id, property.map), propertyKey)
          case property: ASTCachedProperty if property.entityType == NODE_TYPE =>
            commands.predicates.CachedNodePropertyExists(self.toCommandExpression(id, property))
          case property: ASTCachedProperty if property.entityType == RELATIONSHIP_TYPE =>
            commands.predicates.CachedRelationshipPropertyExists(self.toCommandExpression(id, property))
          case expression: internal.expressions.PatternExpression =>
            self.toCommandPredicate(id, expression)
          case expression: pipes.NestedPipeCollectExpression =>
            self.toCommandPredicate(id, expression)
          case e: internal.expressions.ContainerIndex =>
            commands.expressions.ContainerIndexExists(self.toCommandExpression(id, e.expr), self.toCommandExpression(id, e.idx))
          case _: NestedPlanExpression =>
            throw new InternalException("should have been rewritten away")
        }
      case Exp => commands.expressions.ExpFunction(self.toCommandExpression(id, invocation.arguments.head))
      case File => commands.expressions.File()
      case Floor => commands.expressions.FloorFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Haversin => commands.expressions.HaversinFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Head =>
        commands.expressions.ContainerIndex(
          self.toCommandExpression(id, invocation.arguments.head),
          commands.expressions.Literal(0)
        )
      case functions.Id => commands.expressions.IdFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Keys => commands.expressions.KeysFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Labels => commands.expressions.LabelsFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Last =>
        commands.expressions.ContainerIndex(
          self.toCommandExpression(id, invocation.arguments.head),
          commands.expressions.Literal(-1)
        )
      case Left =>
        commands.expressions.LeftFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1))
        )
      case Length => commands.expressions.LengthFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Linenumber => commands.expressions.Linenumber()
      case Log => commands.expressions.LogFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Log10 => commands.expressions.Log10Function(self.toCommandExpression(id, invocation.arguments.head))
      case LTrim => commands.expressions.LTrimFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Max =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commands.expressions.Max(inner)
        if (invocation.distinct)
          commands.expressions.Distinct(command, inner)
        else
          command
      case Min =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commands.expressions.Min(inner)
        if (invocation.distinct)
          commands.expressions.Distinct(command, inner)
        else
          command
      case Nodes => commands.expressions.NodesFunction(self.toCommandExpression(id, invocation.arguments.head))
      case PercentileCont =>
        val firstArg = self.toCommandExpression(id, invocation.arguments.head)
        val secondArg = self.toCommandExpression(id, invocation.arguments(1))

        val command = commands.expressions.PercentileCont(firstArg, secondArg)
        if (invocation.distinct)
          commands.expressions.Distinct(command, firstArg)
        else
          command
      case PercentileDisc =>
        val firstArg = self.toCommandExpression(id, invocation.arguments.head)
        val secondArg = self.toCommandExpression(id, invocation.arguments(1))

        val command = commands.expressions.PercentileDisc(firstArg, secondArg)
        if (invocation.distinct)
          commands.expressions.Distinct(command, firstArg)
        else
          command
      case Pi => commands.expressions.PiFunction()
      case Distance =>
        val firstArg = self.toCommandExpression(id, invocation.arguments.head)
        val secondArg = self.toCommandExpression(id, invocation.arguments(1))
        commands.expressions.DistanceFunction(firstArg, secondArg)
      case Point => commands.expressions.PointFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Radians => commands.expressions.RadiansFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Rand => commands.expressions.RandFunction()
      case functions.Range =>
        commands.expressions.RangeFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1)),
          toCommandExpression(id, invocation.arguments.lift(2), self).getOrElse(commands.expressions.Literal(1))
        )
      case Relationships => commands.expressions.RelationshipFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Replace =>
        commands.expressions.ReplaceFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1)),
          self.toCommandExpression(id, invocation.arguments(2))
        )
      case Reverse => commands.expressions.ReverseFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Right =>
        commands.expressions.RightFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1))
        )
      case Round => commands.expressions.RoundFunction(self.toCommandExpression(id, invocation.arguments.head))
      case RTrim => commands.expressions.RTrimFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Sign => commands.expressions.SignFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Sin => commands.expressions.SinFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Size => commands.expressions.SizeFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Split =>
        commands.expressions.SplitFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1))
        )
      case Sqrt => commands.expressions.SqrtFunction(self.toCommandExpression(id, invocation.arguments.head))
      case StartNode => commands.expressions
        .RelationshipEndPoints(self.toCommandExpression(id, invocation.arguments.head), start = true)
      case StdDev =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commands.expressions.Stdev(inner)
        if (invocation.distinct)
          commands.expressions.Distinct(command, inner)
        else
          command
      case StdDevP =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commands.expressions.StdevP(inner)
        if (invocation.distinct)
          commands.expressions.Distinct(command, inner)
        else
          command
      case Substring =>
        commands.expressions.SubstringFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1)),
          toCommandExpression(id, invocation.arguments.lift(2), self)
        )
      case Sum =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commands.expressions.Sum(inner)
        if (invocation.distinct)
          commands.expressions.Distinct(command, inner)
        else
          command
      case Tail =>
        commands.expressions.ListSlice(
          self.toCommandExpression(id, invocation.arguments.head),
          Some(commands.expressions.Literal(1)),
          None
        )
      case Tan => commands.expressions.TanFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToBoolean => commands.expressions.ToBooleanFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToFloat => commands.expressions.ToFloatFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToInteger => commands.expressions.ToIntegerFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToLower => commands.expressions.ToLowerFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToString => commands.expressions.ToStringFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToUpper => commands.expressions.ToUpperFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Properties => commands.expressions.PropertiesFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Trim => commands.expressions.TrimFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Type => commands.expressions.RelationshipTypeFunction(self.toCommandExpression(id, invocation.arguments.head))
    }

  private def toCommandProperty(id: Id, e: internal.expressions.LogicalProperty, self: ExpressionConverters): commands.expressions.Expression =
    e match {
      case e:ASTCachedProperty if e.entityType == NODE_TYPE => commands.expressions.CachedNodeProperty(e.entityName, getPropertyKey(e.propertyKey), e)
      case e:ASTCachedProperty if e.entityType == RELATIONSHIP_TYPE => commands.expressions.CachedRelationshipProperty(e.entityName, getPropertyKey(e.propertyKey), e)
      case e: LogicalProperty => commands.expressions.Property(self.toCommandExpression(id, e.map), getPropertyKey(e.propertyKey))
    }

  private def toCommandExpression(id: Id, expression: Option[internal.expressions.Expression],
                                  self: ExpressionConverters): Option[commands.expressions.Expression] =
    expression.map(self.toCommandExpression(id,_))

  private def toCommandExpression(id: Id, expressions: Seq[internal.expressions.Expression],
                                  self: ExpressionConverters): Seq[commands.expressions.Expression] =
    expressions.map(self.toCommandExpression(id,_))

  private def variable(e: internal.expressions.LogicalVariable): VariableCommand =
    e match {
      case ExpressionVariable(offset, name) => commands.expressions.ExpressionVariable(offset, name)
      case x => commands.expressions.Variable(x.name)
    }

  private def inequalityExpression(id: Id, original: internal.expressions.InequalityExpression,
                                   self: ExpressionConverters): predicates.ComparablePredicate = original match {
    case e: internal.expressions.LessThan => predicates.LessThan(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
    case e: internal.expressions.LessThanOrEqual => predicates
      .LessThanOrEqual(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
    case e: internal.expressions.GreaterThan => predicates.GreaterThan(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
    case e: internal.expressions.GreaterThanOrEqual => predicates
      .GreaterThanOrEqual(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
  }

  private def getDegree(id: Id, original: internal.expressions.GetDegree, self: ExpressionConverters) = {
    val typ = original.relType.map(relType => UnresolvedRelType(relType.name))
    commands.expressions.GetDegree(self.toCommandExpression(id, original.node), typ, original.dir)
  }

  private def regexMatch(id: Id, e: internal.expressions.RegexMatch, self: ExpressionConverters) = self.toCommandExpression(id, e.rhs) match {
    case literal: commands.expressions.Literal =>
      predicates.LiteralRegularExpression(self.toCommandExpression(id, e.lhs), literal)
    case command =>
      predicates.RegularExpression(self.toCommandExpression(id, e.lhs), command)
  }

  private def in(id: Id, e: internal.expressions.In, self: ExpressionConverters) = e.rhs match {
    case value: internal.expressions.Parameter =>
      predicates.ConstantCachedIn(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, value))

    case value@internal.expressions.ListLiteral(expressions) if expressions.isEmpty =>
      predicates.Not(predicates.True())

    case value@internal.expressions.ListLiteral(expressions) if expressions.forall(_.isInstanceOf[internal.expressions.Literal]) =>
      predicates.ConstantCachedIn(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, value))

    case _ =>
      predicates.DynamicCachedIn(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
  }

  private def caseExpression(id: Id, e: internal.expressions.CaseExpression, self: ExpressionConverters) = e.expression match {
    case Some(innerExpression) =>
      val legacyAlternatives = e.alternatives
        .map { a => (self.toCommandExpression(id, a._1), self.toCommandExpression(id, a._2)) }
      commands.expressions
        .SimpleCase(self.toCommandExpression(id, innerExpression), legacyAlternatives, toCommandExpression(id, e.default, self))
    case None =>
      val predicateAlternatives = e.alternatives
        .map { a => (self.toCommandPredicate(id, a._1), self.toCommandExpression(id, a._2)) }
      commands.expressions.GenericCase(predicateAlternatives, toCommandExpression(id, e.default, self))
  }

  private def hasLabels(id: Id, e: internal.expressions.HasLabels, self: ExpressionConverters): Predicate = {
    val preds = e.labels.map {
      l =>
        predicates.HasLabel(self.toCommandExpression(id, e.expression),
          commands.values.KeyToken.Unresolved(l.name, commands.values.TokenType.Label)): Predicate
    }
    commands.predicates.Ands(preds: _*)
  }

  private def mapItems(id: Id, items: Seq[(internal.expressions.PropertyKeyName, internal.expressions.Expression)],
                       self: ExpressionConverters): Map[String, commands.expressions.Expression] =
    items.map {
      case (name, ex) => name.name -> self.toCommandExpression(id, ex)
    }.toMap

  private def mapProjectionItems(id: Id, items: Seq[internal.expressions.LiteralEntry],
                                 self: ExpressionConverters): Map[String, commands.expressions.Expression] =
    items.map {
      case internal.expressions.LiteralEntry(name, ex) => name.name -> self.toCommandExpression(id, ex)
    }.toMap

  private def listComprehension(id: Id, e: internal.expressions.ListComprehension, self: ExpressionConverters): commands.expressions.Expression = {
    val ev = ExpressionVariable.cast(e.variable)
    val filter = e.innerPredicate match {
      case Some(_: internal.expressions.True) | None =>
        self.toCommandExpression(id, e.expression)
      case Some(inner) =>
        commands.expressions.FilterFunction(self.toCommandExpression(id, e.expression),
          ev.name,
          ev.offset,
          self.toCommandPredicate(id, inner))
    }
    e.extractExpression match {
      case Some(extractExpression) =>
        commands.expressions.ExtractFunction(filter,
          ev.name,
          ev.offset,
          self.toCommandExpression(id, extractExpression))
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
