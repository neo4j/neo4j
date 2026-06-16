/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.runtime.interpreted.commands.convert

import org.neo4j.cypher.internal
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.GraphDirectReference
import org.neo4j.cypher.internal.ast.GraphFunctionReference
import org.neo4j.cypher.internal.expressions.ASTCachedProperty
import org.neo4j.cypher.internal.expressions.AssertIsNode
import org.neo4j.cypher.internal.expressions.CachedHasProperty
import org.neo4j.cypher.internal.expressions.DesugaredMapProjection
import org.neo4j.cypher.internal.expressions.ElementIdToLongId
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.IsRepeatAcyclic
import org.neo4j.cypher.internal.expressions.IsRepeatTrailUnique
import org.neo4j.cypher.internal.expressions.LogicalProperty
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.NODE_TYPE
import org.neo4j.cypher.internal.expressions.NonCompilable
import org.neo4j.cypher.internal.expressions.NullCheckAssert
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RELATIONSHIP_TYPE
import org.neo4j.cypher.internal.expressions.functions
import org.neo4j.cypher.internal.expressions.functions.Abs
import org.neo4j.cypher.internal.expressions.functions.Acos
import org.neo4j.cypher.internal.expressions.functions.Asin
import org.neo4j.cypher.internal.expressions.functions.Atan
import org.neo4j.cypher.internal.expressions.functions.Atan2
import org.neo4j.cypher.internal.expressions.functions.Avg
import org.neo4j.cypher.internal.expressions.functions.BTrim
import org.neo4j.cypher.internal.expressions.functions.Cardinality
import org.neo4j.cypher.internal.expressions.functions.Ceil
import org.neo4j.cypher.internal.expressions.functions.CharacterLength
import org.neo4j.cypher.internal.expressions.functions.Coalesce
import org.neo4j.cypher.internal.expressions.functions.CollDistinct
import org.neo4j.cypher.internal.expressions.functions.CollFlatten
import org.neo4j.cypher.internal.expressions.functions.CollIndexOf
import org.neo4j.cypher.internal.expressions.functions.CollInsert
import org.neo4j.cypher.internal.expressions.functions.CollMax
import org.neo4j.cypher.internal.expressions.functions.CollMin
import org.neo4j.cypher.internal.expressions.functions.CollRemove
import org.neo4j.cypher.internal.expressions.functions.CollSort
import org.neo4j.cypher.internal.expressions.functions.Collect
import org.neo4j.cypher.internal.expressions.functions.Cos
import org.neo4j.cypher.internal.expressions.functions.Cosh
import org.neo4j.cypher.internal.expressions.functions.Cot
import org.neo4j.cypher.internal.expressions.functions.Coth
import org.neo4j.cypher.internal.expressions.functions.Count
import org.neo4j.cypher.internal.expressions.functions.Degrees
import org.neo4j.cypher.internal.expressions.functions.Distance
import org.neo4j.cypher.internal.expressions.functions.E
import org.neo4j.cypher.internal.expressions.functions.EndNode
import org.neo4j.cypher.internal.expressions.functions.Exists
import org.neo4j.cypher.internal.expressions.functions.Exp
import org.neo4j.cypher.internal.expressions.functions.File
import org.neo4j.cypher.internal.expressions.functions.Floor
import org.neo4j.cypher.internal.expressions.functions.Format
import org.neo4j.cypher.internal.expressions.functions.Function
import org.neo4j.cypher.internal.expressions.functions.GraphByElementId
import org.neo4j.cypher.internal.expressions.functions.GraphByName
import org.neo4j.cypher.internal.expressions.functions.Haversin
import org.neo4j.cypher.internal.expressions.functions.Head
import org.neo4j.cypher.internal.expressions.functions.IsEmpty
import org.neo4j.cypher.internal.expressions.functions.IsNaN
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
import org.neo4j.cypher.internal.expressions.functions.Normalize
import org.neo4j.cypher.internal.expressions.functions.PercentileCont
import org.neo4j.cypher.internal.expressions.functions.PercentileDisc
import org.neo4j.cypher.internal.expressions.functions.Percentiles
import org.neo4j.cypher.internal.expressions.functions.Pi
import org.neo4j.cypher.internal.expressions.functions.Point
import org.neo4j.cypher.internal.expressions.functions.Properties
import org.neo4j.cypher.internal.expressions.functions.RTrim
import org.neo4j.cypher.internal.expressions.functions.Radians
import org.neo4j.cypher.internal.expressions.functions.Rand
import org.neo4j.cypher.internal.expressions.functions.RandomUUID
import org.neo4j.cypher.internal.expressions.functions.Relationships
import org.neo4j.cypher.internal.expressions.functions.Replace
import org.neo4j.cypher.internal.expressions.functions.Reverse
import org.neo4j.cypher.internal.expressions.functions.Right
import org.neo4j.cypher.internal.expressions.functions.Round
import org.neo4j.cypher.internal.expressions.functions.Sign
import org.neo4j.cypher.internal.expressions.functions.Sin
import org.neo4j.cypher.internal.expressions.functions.Sinh
import org.neo4j.cypher.internal.expressions.functions.Size
import org.neo4j.cypher.internal.expressions.functions.Split
import org.neo4j.cypher.internal.expressions.functions.Sqrt
import org.neo4j.cypher.internal.expressions.functions.StartNode
import org.neo4j.cypher.internal.expressions.functions.StdDev
import org.neo4j.cypher.internal.expressions.functions.StdDevP
import org.neo4j.cypher.internal.expressions.functions.StringIndexOf
import org.neo4j.cypher.internal.expressions.functions.StringJoin
import org.neo4j.cypher.internal.expressions.functions.StringRegexReplace
import org.neo4j.cypher.internal.expressions.functions.Substring
import org.neo4j.cypher.internal.expressions.functions.Sum
import org.neo4j.cypher.internal.expressions.functions.Tail
import org.neo4j.cypher.internal.expressions.functions.Tan
import org.neo4j.cypher.internal.expressions.functions.Tanh
import org.neo4j.cypher.internal.expressions.functions.ToBoolean
import org.neo4j.cypher.internal.expressions.functions.ToBooleanList
import org.neo4j.cypher.internal.expressions.functions.ToBooleanOrNull
import org.neo4j.cypher.internal.expressions.functions.ToFloat
import org.neo4j.cypher.internal.expressions.functions.ToFloatList
import org.neo4j.cypher.internal.expressions.functions.ToFloatOrNull
import org.neo4j.cypher.internal.expressions.functions.ToInteger
import org.neo4j.cypher.internal.expressions.functions.ToIntegerList
import org.neo4j.cypher.internal.expressions.functions.ToIntegerOrNull
import org.neo4j.cypher.internal.expressions.functions.ToLower
import org.neo4j.cypher.internal.expressions.functions.ToString
import org.neo4j.cypher.internal.expressions.functions.ToStringList
import org.neo4j.cypher.internal.expressions.functions.ToStringOrNull
import org.neo4j.cypher.internal.expressions.functions.ToUpper
import org.neo4j.cypher.internal.expressions.functions.Trim
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.expressions.functions.UUIDConstructor
import org.neo4j.cypher.internal.expressions.functions.UUIDLeastSignificantBits
import org.neo4j.cypher.internal.expressions.functions.UUIDMostSignificantBits
import org.neo4j.cypher.internal.expressions.functions.ValueType
import org.neo4j.cypher.internal.expressions.functions.VectorDimensionCount
import org.neo4j.cypher.internal.expressions.functions.VectorDistance
import org.neo4j.cypher.internal.expressions.functions.VectorNorm
import org.neo4j.cypher.internal.expressions.functions.VectorSimilarityCosine
import org.neo4j.cypher.internal.expressions.functions.VectorSimilarityEuclidean
import org.neo4j.cypher.internal.expressions.functions.WithinBBox
import org.neo4j.cypher.internal.frontend.phases.ResolvedFunctionInvocation
import org.neo4j.cypher.internal.logical.plans.CoerceToPredicate
import org.neo4j.cypher.internal.logical.plans.InequalitySeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.NestedPlanExpression
import org.neo4j.cypher.internal.logical.plans.PointBoundingBoxSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PointDistanceSeekRangeWrapper
import org.neo4j.cypher.internal.logical.plans.PrefixSeekRangeWrapper
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.runtime.SelectivityTrackerRegistrator
import org.neo4j.cypher.internal.runtime.ast.DefaultValueLiteral
import org.neo4j.cypher.internal.runtime.ast.ExpressionVariable
import org.neo4j.cypher.internal.runtime.ast.MakeTraversable
import org.neo4j.cypher.internal.runtime.ast.ParameterFromSlot
import org.neo4j.cypher.internal.runtime.ast.PropertiesUsingCachedProperties
import org.neo4j.cypher.internal.runtime.ast.RuntimeConstant
import org.neo4j.cypher.internal.runtime.ast.TraversalEndpoint
import org.neo4j.cypher.internal.runtime.interpreted.CommandProjection
import org.neo4j.cypher.internal.runtime.interpreted.GroupingExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.PatternConverters.ShortestPathsConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.ConstantGraphReference
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Float32VectorValueConstructorFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Float64VectorValueConstructorFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.IdExpressionGraphReference
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.InequalitySeekRangeExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Int16VectorValueConstructorFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Int32VectorValueConstructorFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Int64VectorValueConstructorFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.Int8VectorValueConstructorFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.NameExpressionGraphReference
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.PointBoundingBoxSeekRangeExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.PointDistanceSeekRangeExpression
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.PropertiesUsingCachedExpressionsFunction
import org.neo4j.cypher.internal.runtime.interpreted.commands.expressions.VariableCommand
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates
import org.neo4j.cypher.internal.runtime.interpreted.commands.predicates.Predicate
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.TokenType.PropertyKey
import org.neo4j.cypher.internal.runtime.interpreted.commands.values.UnresolvedRelType
import org.neo4j.cypher.internal.runtime.interpreted.pipes
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyLabel
import org.neo4j.cypher.internal.runtime.interpreted.pipes.LazyPropertyKey
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols.Float32Type
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.Integer16Type
import org.neo4j.cypher.internal.util.symbols.Integer32Type
import org.neo4j.cypher.internal.util.symbols.Integer8Type
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.exceptions.InternalException
import org.neo4j.kernel.api.impl.schema.vector.VectorSimilarity
import org.neo4j.kernel.impl.util.ValueUtils
import org.neo4j.values.storable.Values
import org.neo4j.values.storable.Values.NO_VALUE
import org.neo4j.values.storable.Values.ZERO_INT
import org.neo4j.values.storable.Values.intValue

case class CommunityExpressionConverter(
  tokenContext: ReadTokenContext,
  anonymousVariableNameGenerator: AnonymousVariableNameGenerator,
  selectivityTrackerRegistrator: SelectivityTrackerRegistrator,
  runtimeConfig: CypherRuntimeConfiguration,
  cypherVersion: CypherVersion
) extends ExpressionConverter {

  override def toCommandProjection(
    id: Id,
    projections: Map[LogicalVariable, Expression],
    self: ExpressionConverters
  ): Option[CommandProjection] = {
    projections
      .foldLeft(Option(Map.empty[String, commands.expressions.Expression])) {
        case (Some(acc), (variable, e)) =>
          toCommandExpression(id, e, self).map(cmdExpr => acc.updated(variable.name, cmdExpr))
        case _ => None
      }
      .map(InterpretedCommandProjection.apply)
  }

  override def toGroupingExpression(
    id: Id,
    groupings: Map[LogicalVariable, Expression],
    orderToLeverage: collection.Seq[Expression],
    self: ExpressionConverters
  ): Option[GroupingExpression] = {
    throw new IllegalStateException("CommunityExpressionConverter cannot create grouping expressions")
  }

  override def toCommandExpression(
    id: Id,
    expression: Expression,
    self: ExpressionConverters
  ): Option[commands.expressions.Expression] = {
    val result = expression match {
      case _: internal.expressions.Null                   => commands.expressions.Null()
      case _: internal.expressions.True                   => predicates.True()
      case _: internal.expressions.False                  => predicates.Not(predicates.True())
      case e: internal.expressions.Literal                => commands.expressions.Literal(ValueUtils.of(e.value))
      case e: internal.expressions.SensitiveStringLiteral => commands.expressions.Literal(Values.byteArray(e.value))
      case e: internal.expressions.Variable               => variable(e)
      case e: ExpressionVariable                          => commands.expressions.ExpressionVariable.of(e)
      case e: internal.expressions.Or =>
        predicates.Ors(Array(self.toCommandPredicate(id, e.lhs), self.toCommandPredicate(id, e.rhs)))
      case e: internal.expressions.Xor =>
        predicates.Xor(self.toCommandPredicate(id, e.lhs), self.toCommandPredicate(id, e.rhs))
      case e: internal.expressions.And =>
        predicates.Ands(Array(self.toCommandPredicate(id, e.lhs), self.toCommandPredicate(id, e.rhs)))
      case e: internal.expressions.Ands =>
        predicates.Ands.fromPredicates(e.exprs.map(self.toCommandPredicate(id, _)).toSeq)
      case e: internal.expressions.AndsReorderable =>
        val trackerIndex = selectivityTrackerRegistrator.register()
        predicates.AndsWithSelectivityTracking(e.exprs.toVector.map(self.toCommandPredicate(id, _)), trackerIndex)
      case e: internal.expressions.Ors => predicates.Ors(e.exprs.map(self.toCommandPredicate(id, _)).toArray)
      case e: internal.expressions.Not => predicates.Not(self.toCommandPredicate(id, e.rhs))
      case e: internal.expressions.Equals =>
        predicates.Equals(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.NotEquals => predicates
          .Not(predicates.Equals(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs)))
      case e: internal.expressions.RegexMatch => regexMatch(id, e, self)
      case e: internal.expressions.In         => in(id, e, self)
      case e: internal.expressions.StartsWith =>
        predicates.StartsWith(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.EndsWith =>
        predicates.EndsWith(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.CoerceTo =>
        commands.expressions.CoerceTo(self.toCommandExpression(id, e.expr), e.typ)
      case e: internal.expressions.Contains =>
        predicates.Contains(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.IsNull    => predicates.IsNull(self.toCommandExpression(id, e.lhs))
      case e: internal.expressions.IsNotNull => predicates.Not(predicates.IsNull(self.toCommandExpression(id, e.lhs)))
      case e: internal.ast.IsTyped =>
        predicates.IsTyped(self.toCommandExpression(id, e.lhs), e.typeName)
      case _: internal.ast.IsNotTyped =>
        throw InternalException.internalError(
          this.getClass.getSimpleName,
          "`IsNotTyped` should have been rewritten away"
        )
      case e: internal.ast.IsNormalized =>
        predicates.IsNormalized(self.toCommandExpression(id, e.lhs), e.normalForm)
      case _: internal.ast.IsNotNormalized =>
        throw InternalException.internalError(
          this.getClass.getSimpleName,
          "`IsNotNormalized` should have been rewritten away"
        )
      case e: internal.expressions.InequalityExpression => inequalityExpression(id, e, self)
      case e: internal.expressions.Add =>
        commands.expressions.Add(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.Concatenate =>
        commands.expressions.Concatenate(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.UnaryAdd => self.toCommandExpression(id, e.rhs)
      case e: internal.expressions.Subtract => commands.expressions
          .Subtract(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.UnarySubtract => commands.expressions
          .Subtract(commands.expressions.Literal(ZERO_INT), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.Multiply => commands.expressions
          .Multiply(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.Divide =>
        commands.expressions.Divide(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.Modulo =>
        commands.expressions.Modulo(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.Pow =>
        commands.expressions.Pow(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
      case e: internal.expressions.FunctionInvocation => toCommandExpression(id, e.function, e, self)
      case e: internal.ast.VectorValueConstructor =>
        val vectorExpression = self.toCommandExpression(id, e.vectorCandidate)
        val dimensionExpression = self.toCommandExpression(id, e.dimension)
        e.typeName match {
          case _: Integer8Type =>
            Int8VectorValueConstructorFunction(vectorExpression, dimensionExpression)
          case _: Integer16Type =>
            Int16VectorValueConstructorFunction(vectorExpression, dimensionExpression)
          case _: Integer32Type =>
            Int32VectorValueConstructorFunction(vectorExpression, dimensionExpression)
          case _: IntegerType =>
            Int64VectorValueConstructorFunction(vectorExpression, dimensionExpression)
          case _: Float32Type =>
            Float32VectorValueConstructorFunction(vectorExpression, dimensionExpression)
          case _: FloatType =>
            Float64VectorValueConstructorFunction(vectorExpression, dimensionExpression)
          case x => throw InternalException.internalError(getClass.getSimpleName, s"Unexpected vector type: $x")
        }
      case _: internal.expressions.CountStar       => commands.expressions.CountStar()
      case e: internal.expressions.LogicalProperty => toCommandProperty(id, e, self)
      case ParameterFromSlot(offset, name, _)      => commands.expressions.ParameterFromSlot(offset, name)
      case e: internal.expressions.CaseExpression  => caseExpression(id, e, self)
      case e: internal.expressions.ShortestPathExpression =>
        commands.expressions
          .ShortestPathExpression(
            e.pattern.asLegacyPatterns(id, None, self, anonymousVariableNameGenerator).head,
            disallowSameNode = runtimeConfig.errorIfShortestPathHasCommonNodesAtRuntime,
            operatorId = id
          )
      case e: internal.expressions.HasLabelsOrTypes           => hasLabelsOrTypes(id, e, self)
      case e: internal.expressions.HasALabelOrType            => hasALabelOrType(id, e, self)
      case e: internal.expressions.HasALabel                  => hasALabel(id, e, self)
      case e: internal.expressions.HasLabels                  => hasLabels(id, e, self)
      case e: internal.expressions.HasDynamicLabels           => hasDynamicLabels(id, e, self)
      case e: internal.expressions.HasAnyLabel                => hasAnyLabel(id, e, self)
      case e: internal.expressions.HasAnyDynamicLabel         => hasAnyDynamicLabel(id, e, self)
      case e: internal.expressions.HasAnyDynamicLabelsOrTypes => hasAnyDynamicLabelsOrTypes(id, e, self)
      case e: internal.expressions.HasDynamicLabelsOrTypes    => hasDynamicLabelsOrTypes(id, e, self)
      case e: internal.expressions.HasTypes                   => hasTypes(id, e, self)
      case e: internal.expressions.HasDynamicType             => hasDynamicType(id, e, self)
      case e: internal.expressions.HasAnyDynamicType          => hasAnyDynamicType(id, e, self)

      case e: internal.expressions.ListLiteral =>
        commands.expressions.ListLiteral(toCommandExpression(id, e.expressions, self): _*)
      case e: internal.expressions.MapExpression => commands.expressions.LiteralMap(mapItems(id, e.items, self))
      case e: internal.expressions.ListSlice => commands.expressions
          .ListSlice(
            self.toCommandExpression(id, e.list),
            toCommandExpression(id, e.from, self),
            toCommandExpression(id, e.to, self)
          )
      case e: internal.expressions.ContainerIndex => commands.expressions
          .ContainerIndex(self.toCommandExpression(id, e.expr), self.toCommandExpression(id, e.idx))

      case e: internal.expressions.ListComprehension => listComprehension(id, e, self)
      case e: internal.expressions.AllIterablePredicate =>
        val ev = ExpressionVariable.cast(e.variable)
        commands.AllInList(
          self.toCommandExpression(id, e.expression),
          ev.name,
          ev.offset,
          e.innerPredicate.map(self.toCommandPredicate(id, _)).getOrElse(predicates.True())
        )

      case e: internal.expressions.AnyIterablePredicate =>
        val ev = ExpressionVariable.cast(e.variable)
        commands.AnyInList(
          self.toCommandExpression(id, e.expression),
          ev.name,
          ev.offset,
          e.innerPredicate.map(self.toCommandPredicate(id, _)).getOrElse(predicates.True())
        )

      case e: internal.expressions.NoneIterablePredicate =>
        val ev = ExpressionVariable.cast(e.variable)
        commands.NoneInList(
          self.toCommandExpression(id, e.expression),
          ev.name,
          ev.offset,
          e.innerPredicate.map(self.toCommandPredicate(id, _)).getOrElse(predicates.True())
        )

      case e: internal.expressions.SingleIterablePredicate =>
        val ev = ExpressionVariable.cast(e.variable)
        commands.SingleInList(
          self.toCommandExpression(id, e.expression),
          ev.name,
          ev.offset,
          e.innerPredicate.map(self.toCommandPredicate(id, _)).getOrElse(predicates.True())
        )

      case e: internal.expressions.ReduceExpression =>
        val innerVariable = ExpressionVariable.cast(e.variable)
        val accVariable = ExpressionVariable.cast(e.accumulator)
        commands.expressions.ReduceFunction(
          self.toCommandExpression(id, e.list),
          innerVariable.name,
          innerVariable.offset,
          self.toCommandExpression(id, e.expression),
          accVariable.name,
          accVariable.offset,
          self.toCommandExpression(id, e.init)
        )

      case e: internal.expressions.PathExpression => self.toCommandProjectedPath(e)
      case e: pipes.NestedPipeCollectExpression =>
        commands.expressions.NestedPipeCollectExpression(
          e.pipe,
          self.toCommandExpression(id, e.projection),
          e.availableExpressionVariables.map(commands.expressions.ExpressionVariable.of).toArray,
          id
        )
      case e: pipes.NestedPipeExistsExpression =>
        commands.expressions.NestedPipeExistsExpression(
          e.pipe,
          e.availableExpressionVariables.map(commands.expressions.ExpressionVariable.of).toArray,
          id
        )
      case e: pipes.NestedPipeGetByNameExpression =>
        commands.expressions.NestedPipeGetByNameExpression(
          e.pipe,
          e.columnNameToGet,
          e.availableExpressionVariables.map(commands.expressions.ExpressionVariable.of).toArray,
          id
        )
      case e: internal.expressions.GetDegree => getDegree(id, e, self)
      case internal.expressions.HasDegreeGreaterThan(node, relType, dir, degree) =>
        val typ = relType.map(relType => UnresolvedRelType(relType.name))
        commands.expressions.HasDegreeGreaterThan(
          self.toCommandExpression(id, node),
          typ,
          dir,
          self.toCommandExpression(id, degree)
        )
      case internal.expressions.HasDegreeGreaterThanOrEqual(node, relType, dir, degree) =>
        val typ = relType.map(relType => UnresolvedRelType(relType.name))
        commands.expressions.HasDegreeGreaterThanOrEqual(
          self.toCommandExpression(id, node),
          typ,
          dir,
          self.toCommandExpression(id, degree)
        )
      case internal.expressions.HasDegree(node, relType, dir, degree) =>
        val typ = relType.map(relType => UnresolvedRelType(relType.name))
        commands.expressions.HasDegree(
          self.toCommandExpression(id, node),
          typ,
          dir,
          self.toCommandExpression(id, degree)
        )
      case internal.expressions.HasDegreeLessThan(node, relType, dir, degree) =>
        val typ = relType.map(relType => UnresolvedRelType(relType.name))
        commands.expressions.HasDegreeLessThan(
          self.toCommandExpression(id, node),
          typ,
          dir,
          self.toCommandExpression(id, degree)
        )
      case internal.expressions.HasDegreeLessThanOrEqual(node, relType, dir, degree) =>
        val typ = relType.map(relType => UnresolvedRelType(relType.name))
        commands.expressions.HasDegreeLessThanOrEqual(
          self.toCommandExpression(id, node),
          typ,
          dir,
          self.toCommandExpression(id, degree)
        )
      case e: PrefixSeekRangeWrapper => commands.expressions
          .PrefixSeekRangeExpression(e.range.map(self.toCommandExpression(id, _)))
      case e: InequalitySeekRangeWrapper =>
        InequalitySeekRangeExpression(e.range.mapBounds(self.toCommandExpression(id, _)))
      case e: PointDistanceSeekRangeWrapper =>
        PointDistanceSeekRangeExpression(e.range.map(self.toCommandExpression(id, _)))
      case e: PointBoundingBoxSeekRangeWrapper =>
        PointBoundingBoxSeekRangeExpression(e.range.map(self.toCommandExpression(id, _)))
      case e: internal.expressions.AndedPropertyInequalities => predicates
          .AndedPropertyComparablePredicates(
            variable(e.variable),
            toCommandProperty(id, e.property, self),
            e.inequalities.map(e => inequalityExpression(id, e, self)).toIndexedSeq.toArray
          )
      case e: DesugaredMapProjection => commands.expressions
          .DesugaredMapProjection(
            self.toCommandExpression(id, e.entity),
            e.includeAllProps,
            mapProjectionItems(id, e.items, self)
          )
      case e: PropertiesUsingCachedProperties =>
        val cached: Set[(LazyPropertyKey, expressions.Expression)] =
          e.cachedProperties.map(e => LazyPropertyKey(e.propertyKey, tokenContext) -> self.toCommandExpression(id, e))
        PropertiesUsingCachedExpressionsFunction(self.toCommandExpression(id, e.variable), cached.toArray)
      case e: ResolvedFunctionInvocation =>
        val callArgumentCommands = e.callArguments.map(Some(_))
          .zipAll(e.fcnSignature.get.inputSignature.map(_.default), None, None).map {
            case (givenArg, default) => givenArg.map(self.toCommandExpression(id, _))
                .getOrElse(commands.expressions.Literal(default.get))
          }
        val signature = e.fcnSignature.get
        if (signature.isAggregate)
          commands.expressions.AggregationFunctionInvocation(id, signature, callArgumentCommands)
        else if (signature.builtIn) {
          commands.expressions.BuiltInFunctionInvocation(id, signature, callArgumentCommands.toArray)
        } else
          commands.expressions.UserFunctionInvocation(id, signature, callArgumentCommands.toArray)

      case _: internal.expressions.MapProjection =>
        throw InternalException.internalError(
          this.getClass.getSimpleName,
          "`MapProjection` should have been rewritten away"
        )
      case _: internal.expressions.PatternComprehension =>
        throw InternalException.internalError(
          this.getClass.getSimpleName,
          "`PatternComprehension` should have been rewritten away"
        )
      case _: internal.expressions.PatternExpression =>
        throw InternalException.internalError(
          this.getClass.getSimpleName,
          "`PatternExpression` should have been rewritten away"
        )
      case _: NestedPlanExpression =>
        throw InternalException.internalError(
          this.getClass.getSimpleName,
          "`NestedPlanExpression` should have been rewritten away"
        )
      case _: internal.expressions.Parameter =>
        throw InternalException.internalError(
          this.getClass.getSimpleName,
          "`Parameter` should have been rewritten away"
        )
      case _: ExistsExpression => throw InternalException.internalError(
          this.getClass.getSimpleName,
          "`ExistsExpression` should have been rewritten away"
        )
      case CoerceToPredicate(inner) => predicates.CoercedPredicate(self.toCommandExpression(id, inner))
      case e: internal.expressions.CollectAll =>
        commands.expressions.CollectAll(self.toCommandExpression(id, e.arguments.head))
      case e: internal.expressions.CollectDistinct =>
        commands.expressions.CollectDistinct(self.toCommandExpression(id, e.arguments.head), e.isOrdered)
      case e: internal.expressions.CollectDistinctIds =>
        commands.expressions.CollectDistinctIds(self.toCommandExpression(id, e.arguments.head))
      case e: DefaultValueLiteral => commands.expressions.Literal(e.value)
      case e: RuntimeConstant =>
        commands.expressions.RuntimeConstant(
          ExpressionVariable.cast(e.variable).offset,
          self.toCommandExpression(id, e.inner)
        )
      case AssertIsNode(expr) => commands.expressions.AssertIsNodeFunction(self.toCommandExpression(id, expr))
      case MakeTraversable(e) => commands.expressions.MakeTraversable(self.toCommandExpression(id, e))
      case ElementIdToLongId(NODE_TYPE, ElementIdToLongId.Mode.Single, rhs) =>
        commands.expressions.ElementIdToNodeIdFunction(self.toCommandExpression(id, rhs))
      case ElementIdToLongId(NODE_TYPE, ElementIdToLongId.Mode.Many, rhs) =>
        commands.expressions.ElementIdListToNodeIdListFunction(self.toCommandExpression(id, rhs))
      case ElementIdToLongId(RELATIONSHIP_TYPE, ElementIdToLongId.Mode.Single, rhs) =>
        commands.expressions.ElementIdToRelationshipIdFunction(self.toCommandExpression(id, rhs))
      case ElementIdToLongId(RELATIONSHIP_TYPE, ElementIdToLongId.Mode.Many, rhs) =>
        commands.expressions.ElementIdListToRelationshipIdListFunction(self.toCommandExpression(id, rhs))
      case _: IsRepeatTrailUnique            => predicates.True()
      case _: IsRepeatAcyclic                => predicates.True() // TODO: This correct?
      case _: NullCheckAssert                => commands.expressions.Literal(NO_VALUE)
      case _: NonCompilable                  => commands.expressions.Literal(NO_VALUE)
      case GraphDirectReference(catalogName) => ConstantGraphReference(catalogName)
      case GraphFunctionReference(GraphByName(name), parseStringGraphReferences) =>
        NameExpressionGraphReference(self.toCommandExpression(id, name), parseStringGraphReferences)
      case GraphFunctionReference(GraphByElementId(elementId), _) =>
        IdExpressionGraphReference(self.toCommandExpression(id, elementId))
      case TraversalEndpoint(v, _)                   => variable(v)
      case _: internal.expressions.ObfuscatedLiteral => commands.expressions.Null()
      case _                                         => null
    }

    Option(result)
  }

  private def toCommandExpression(
    id: Id,
    expression: Function,
    invocation: internal.expressions.FunctionInvocation,
    self: ExpressionConverters
  ): commands.expressions.Expression =
    expression match {
      case Abs  => commands.expressions.AbsFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Acos => commands.expressions.AcosFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Asin => commands.expressions.AsinFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Atan => commands.expressions.AtanFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Atan2 =>
        commands.expressions.Atan2Function(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1))
        )
      case Avg =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commands.expressions.Avg(inner)
        if (invocation.distinct)
          commands.expressions.Distinct(command, inner, invocation.isOrdered)
        else
          command
      case BTrim => commands.expressions.BTrimFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          toCommandExpression(id, invocation.arguments.lift(1), self)
        )
      case Ceil => commands.expressions.CeilFunction(self.toCommandExpression(id, invocation.arguments.head))
      case CharacterLength =>
        commands.expressions.CharacterLengthFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Coalesce => commands.expressions.CoalesceFunction(toCommandExpression(id, invocation.arguments, self): _*)
      case CollDistinct => commands.expressions.CollDistinctFunction(
          self.toCommandExpression(id, invocation.arguments.head)
        )
      case CollRemove => commands.expressions.CollRemoveFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1))
        )
      case CollInsert => commands.expressions.CollInsertFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1)),
          self.toCommandExpression(id, invocation.arguments(2))
        )
      case Collect =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commands.expressions.Collect(inner)
        if (invocation.distinct)
          commands.expressions.Distinct(command, inner, invocation.isOrdered)
        else
          command

      case Cos  => commands.expressions.CosFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Cosh => commands.expressions.CoshFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Cot  => commands.expressions.CotFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Coth => commands.expressions.CothFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Count =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commands.expressions.Count(inner)
        if (invocation.distinct)
          commands.expressions.Distinct(command, inner, invocation.isOrdered)
        else
          command
      case Degrees => commands.expressions.DegreesFunction(self.toCommandExpression(id, invocation.arguments.head))
      case E       => commands.expressions.EFunction()
      case EndNode => commands.expressions
          .RelationshipEndPoints(self.toCommandExpression(id, invocation.arguments.head), start = false)
      case Exists =>
        invocation.arguments.head match {
          case patternExpression: internal.expressions.PatternExpression =>
            self.toCommandPredicate(id, patternExpression)
          case nestedExpression: pipes.NestedPipeCollectExpression =>
            self.toCommandPredicate(id, nestedExpression)
          case _: NestedPlanExpression =>
            throw InternalException.internalError(this.getClass.getSimpleName, "should have been rewritten away")
          case x =>
            throw InternalException.internalError(
              this.getClass.getSimpleName,
              s"unexpected Exists argument ${x.getClass.getSimpleName}"
            )
        }
      case Exp   => commands.expressions.ExpFunction(self.toCommandExpression(id, invocation.arguments.head))
      case File  => commands.expressions.File()
      case Floor => commands.expressions.FloorFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Format => if (invocation.arguments.size == 2) {
          commands.expressions.FormatFunction(
            self.toCommandExpression(id, invocation.arguments.head),
            Some(self.toCommandExpression(id, invocation.arguments(1)))
          )
        } else {
          commands.expressions.FormatFunction(self.toCommandExpression(id, invocation.arguments.head), None)
        }
      case Haversin => commands.expressions.HaversinFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Head =>
        commands.expressions.Head(
          self.toCommandExpression(id, invocation.arguments.head)
        )
      case functions.Id => commands.expressions.IdFunction(self.toCommandExpression(id, invocation.arguments.head))
      case IsNaN        => commands.expressions.IsNaNFunction(self.toCommandExpression(id, invocation.arguments.head))
      case functions.ElementId =>
        commands.expressions.ElementIdFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Keys   => commands.expressions.KeysFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Labels => commands.expressions.LabelsFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Last =>
        commands.expressions.Last(
          self.toCommandExpression(id, invocation.arguments.head)
        )
      case Left =>
        commands.expressions.LeftFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1))
        )
      case Length => commands.expressions.LengthFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Cardinality =>
        commands.expressions.CardinalityFunction(self.toCommandExpression(id, invocation.arguments.head))
      case IsEmpty    => commands.expressions.IsEmptyFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Linenumber => commands.expressions.Linenumber()
      case Log        => commands.expressions.LogFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Log10      => commands.expressions.Log10Function(self.toCommandExpression(id, invocation.arguments.head))
      case LTrim => commands.expressions.LTrimFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          toCommandExpression(id, invocation.arguments.lift(1), self)
        )
      case Max =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commands.expressions.Max(inner)
        if (invocation.distinct)
          commands.expressions.Distinct(command, inner, invocation.isOrdered)
        else
          command
      case Min =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commands.expressions.Min(inner)
        if (invocation.distinct)
          commands.expressions.Distinct(command, inner, invocation.isOrdered)
        else
          command
      case Nodes => commands.expressions.NodesFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Normalize =>
        val maybeNormalForm = toCommandExpression(id, invocation.arguments.lift(1), self)
        val form = maybeNormalForm match {
          case Some(mode) => mode
          case None       => commands.expressions.Literal(Values.stringValue("NFC"))
        }
        commands.expressions.NormalizeFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          form
        )
      case PercentileCont =>
        val firstArg = self.toCommandExpression(id, invocation.arguments.head)
        val secondArg = self.toCommandExpression(id, invocation.arguments(1))

        val command = commands.expressions.PercentileCont(firstArg, secondArg, invocation.order)
        if (invocation.distinct)
          commands.expressions.Distinct(command, firstArg, invocation.isOrdered)
        else
          command
      case PercentileDisc =>
        val firstArg = self.toCommandExpression(id, invocation.arguments.head)
        val secondArg = self.toCommandExpression(id, invocation.arguments(1))

        val command = commands.expressions.PercentileDisc(firstArg, secondArg, invocation.order)
        if (invocation.distinct)
          commands.expressions.Distinct(command, firstArg, invocation.isOrdered)
        else
          command
      case Percentiles =>
        val inputArg = self.toCommandExpression(id, invocation.arguments.head)
        val percentilesArg = self.toCommandExpression(id, invocation.arguments(1))
        val keysArg = self.toCommandExpression(id, invocation.arguments(2))
        val isDiscretesArg = self.toCommandExpression(id, invocation.arguments(3))

        val command =
          commands.expressions.Percentiles(inputArg, percentilesArg, keysArg, isDiscretesArg, invocation.order)
        if (invocation.distinct) {
          commands.expressions.Distinct(command, inputArg, invocation.isOrdered)
        } else
          command
      case Pi => commands.expressions.PiFunction()
      case Distance =>
        val firstArg = self.toCommandExpression(id, invocation.arguments.head)
        val secondArg = self.toCommandExpression(id, invocation.arguments(1))
        commands.expressions.DistanceFunction(firstArg, secondArg)
      case Point => commands.expressions.PointFunction(self.toCommandExpression(id, invocation.arguments.head))
      case WithinBBox =>
        commands.expressions.WithinBBoxFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1)),
          self.toCommandExpression(id, invocation.arguments(2))
        )
      case Radians    => commands.expressions.RadiansFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Rand       => commands.expressions.RandFunction()
      case RandomUUID => commands.expressions.RandomUUIDFunction()
      case functions.Range =>
        commands.expressions.RangeFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1)),
          toCommandExpression(id, invocation.arguments.lift(2), self).getOrElse(
            commands.expressions.Literal(intValue(1))
          )
        )
      case Relationships =>
        commands.expressions.RelationshipFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Replace => if (invocation.arguments.size == 3) {
          commands.expressions.ReplaceFunction(
            self.toCommandExpression(id, invocation.arguments.head),
            self.toCommandExpression(id, invocation.arguments(1)),
            self.toCommandExpression(id, invocation.arguments(2)),
            None
          )
        } else {
          commands.expressions.ReplaceFunction(
            self.toCommandExpression(id, invocation.arguments.head),
            self.toCommandExpression(id, invocation.arguments(1)),
            self.toCommandExpression(id, invocation.arguments(2)),
            Some(self.toCommandExpression(id, invocation.arguments(3)))
          )

        }
      case Reverse => commands.expressions.ReverseFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Right =>
        commands.expressions.RightFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1))
        )
      case Round =>
        val maybeMode = toCommandExpression(id, invocation.arguments.lift(2), self)
        val (mode, explicitMode) = maybeMode match {
          case Some(mode) => (mode, true)
          case None       => (commands.expressions.Literal(Values.stringValue("HALF_UP")), false)
        }
        commands.expressions.RoundFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          toCommandExpression(id, invocation.arguments.lift(1), self).getOrElse(
            commands.expressions.Literal(intValue(0))
          ),
          mode,
          commands.expressions.Literal(Values.booleanValue(explicitMode))
        )
      case RTrim => commands.expressions.RTrimFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          toCommandExpression(id, invocation.arguments.lift(1), self)
        )
      case Sign => commands.expressions.SignFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Sin  => commands.expressions.SinFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Sinh => commands.expressions.SinhFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Size =>
        cypherVersion match {
          case CypherVersion.Cypher5 =>
            commands.expressions.SizeFunctionCypher5(self.toCommandExpression(id, invocation.arguments.head))
          case CypherVersion.Cypher25 =>
            commands.expressions.SizeFunctionCypher25(self.toCommandExpression(id, invocation.arguments.head))
        }
      case Split =>
        commands.expressions.SplitFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1))
        )
      case StringIndexOf =>
        commands.expressions.StringIndexOfFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1))
        )
      case StringJoin =>
        commands.expressions.StringJoinFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1))
        )
      case StringRegexReplace =>
        val regexExpr = self.toCommandExpression(id, invocation.arguments(1))
        regexExpr match {
          case lit: commands.expressions.Literal =>
            commands.expressions.LiteralStringRegexReplaceFunction(
              self.toCommandExpression(id, invocation.arguments.head),
              lit,
              self.toCommandExpression(id, invocation.arguments(2))
            )
          case _ =>
            commands.expressions.StringRegexReplaceFunction(
              self.toCommandExpression(id, invocation.arguments.head),
              regexExpr,
              self.toCommandExpression(id, invocation.arguments(2))
            )
        }
      case Sqrt => commands.expressions.SqrtFunction(self.toCommandExpression(id, invocation.arguments.head))
      case StartNode => commands.expressions
          .RelationshipEndPoints(self.toCommandExpression(id, invocation.arguments.head), start = true)
      case StdDev =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commands.expressions.Stdev(inner)
        if (invocation.distinct)
          commands.expressions.Distinct(command, inner, invocation.isOrdered)
        else
          command
      case StdDevP =>
        val inner = self.toCommandExpression(id, invocation.arguments.head)
        val command = commands.expressions.StdevP(inner)
        if (invocation.distinct)
          commands.expressions.Distinct(command, inner, invocation.isOrdered)
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
          commands.expressions.Distinct(command, inner, invocation.isOrdered)
        else
          command
      case Tail =>
        commands.expressions.Tail(
          self.toCommandExpression(id, invocation.arguments.head)
        )
      case Tan       => commands.expressions.TanFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Tanh      => commands.expressions.TanhFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToBoolean => commands.expressions.ToBooleanFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToBooleanList =>
        commands.expressions.ToBooleanListFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToBooleanOrNull =>
        commands.expressions.ToBooleanOrNullFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToFloat => commands.expressions.ToFloatFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToFloatList =>
        cypherVersion match {
          case CypherVersion.Cypher5 =>
            commands.expressions.ToFloatListFunctionCypher5(self.toCommandExpression(id, invocation.arguments.head))
          case CypherVersion.Cypher25 =>
            commands.expressions.ToFloatListFunctionCypher25(self.toCommandExpression(id, invocation.arguments.head))
        }
      case ToFloatOrNull =>
        commands.expressions.ToFloatOrNullFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToInteger => commands.expressions.ToIntegerFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToIntegerList =>
        cypherVersion match {
          case CypherVersion.Cypher5 =>
            commands.expressions.ToIntegerListFunctionCypher5(self.toCommandExpression(id, invocation.arguments.head))
          case CypherVersion.Cypher25 =>
            commands.expressions.ToIntegerListFunctionCypher25(self.toCommandExpression(id, invocation.arguments.head))
        }

      case ToIntegerOrNull =>
        commands.expressions.ToIntegerOrNullFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToLower  => commands.expressions.ToLowerFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToString => commands.expressions.ToStringFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToStringList =>
        commands.expressions.ToStringListFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToStringOrNull =>
        commands.expressions.ToStringOrNullFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ToUpper => commands.expressions.ToUpperFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Properties =>
        commands.expressions.PropertiesFunction(self.toCommandExpression(id, invocation.arguments.head))
      case Trim => if (invocation.arguments.size == 2) {
          commands.expressions.TrimFunction(
            self.toCommandExpression(id, invocation.arguments.head),
            self.toCommandExpression(id, invocation.arguments(1)),
            None
          )
        } else {
          commands.expressions.TrimFunction(
            self.toCommandExpression(id, invocation.arguments.head),
            self.toCommandExpression(id, invocation.arguments(2)),
            Some(self.toCommandExpression(id, invocation.arguments(1)))
          )
        }
      case CollFlatten => if (invocation.arguments.size == 1) {
          commands.expressions.CollFlattenFunction(
            self.toCommandExpression(id, invocation.arguments.head),
            None
          )
        } else {
          commands.expressions.CollFlattenFunction(
            self.toCommandExpression(id, invocation.arguments.head),
            Some(self.toCommandExpression(id, invocation.arguments(1)))
          )
        }
      case CollSort =>
        commands.expressions.CollSortFunction(
          self.toCommandExpression(id, invocation.arguments.head)
        )
      case CollIndexOf =>
        commands.expressions.CollIndexOfFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1))
        )
      case CollMax =>
        commands.expressions.CollMaxFunction(
          self.toCommandExpression(id, invocation.arguments.head)
        )
      case CollMin =>
        commands.expressions.CollMinFunction(
          self.toCommandExpression(id, invocation.arguments.head)
        )
      case Type =>
        commands.expressions.RelationshipTypeFunction(self.toCommandExpression(id, invocation.arguments.head))
      case ValueType => commands.expressions.ValueTypeFunction(self.toCommandExpression(id, invocation.arguments.head))
      case VectorDimensionCount =>
        commands.expressions.VectorDimensionCountFunction(self.toCommandExpression(id, invocation.arguments.head))
      case UUIDConstructor => if (invocation.arguments.isEmpty) {
          commands.expressions.UUIDConstructorFunction(
            None,
            None
          )
        } else if (invocation.arguments.size == 1) {
          commands.expressions.UUIDConstructorFunction(
            Some(self.toCommandExpression(id, invocation.arguments.head))
          )
        } else {
          commands.expressions.UUIDConstructorFunction(
            Some(self.toCommandExpression(id, invocation.arguments.head)),
            Some(self.toCommandExpression(id, invocation.arguments(1)))
          )
        }
      case UUIDLeastSignificantBits =>
        commands.expressions.UUIDLeastSignificantBits(
          self.toCommandExpression(id, invocation.arguments.head)
        )
      case UUIDMostSignificantBits =>
        commands.expressions.UUIDMostSignificantBits(
          self.toCommandExpression(id, invocation.arguments.head)
        )
      case VectorDistance =>
        commands.expressions.VectorDistanceFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1)),
          self.toCommandExpression(id, invocation.arguments(2))
        )
      case VectorNorm =>
        commands.expressions.VectorNormFunction(
          self.toCommandExpression(id, invocation.arguments.head),
          self.toCommandExpression(id, invocation.arguments(1))
        )
      case VectorSimilarityEuclidean =>
        val firstArg = self.toCommandExpression(id, invocation.arguments.head)
        val secondArg = self.toCommandExpression(id, invocation.arguments(1))
        commands.expressions.VectorSimilarityFunction(
          VectorSimilarity.EUCLIDEAN,
          firstArg,
          secondArg
        )

      case VectorSimilarityCosine =>
        val firstArg = self.toCommandExpression(id, invocation.arguments.head)
        val secondArg = self.toCommandExpression(id, invocation.arguments(1))
        commands.expressions.VectorSimilarityFunction(
          VectorSimilarity.COSINE,
          firstArg,
          secondArg
        )

      case x =>
        throw InternalException.internalError(getClass.getSimpleName, s"Unexpected function '${x.name}'")
    }

  private def toCommandProperty(
    id: Id,
    e: internal.expressions.LogicalProperty,
    self: ExpressionConverters
  ): commands.expressions.Expression =
    e match {
      case e: CachedHasProperty if e.entityType == NODE_TYPE =>
        commands.expressions.CachedNodeHasProperty(e.entityName, getPropertyKey(e.propertyKey), e.runtimeKey)
      case e: CachedHasProperty if e.entityType == RELATIONSHIP_TYPE =>
        commands.expressions.CachedRelationshipHasProperty(e.entityName, getPropertyKey(e.propertyKey), e.runtimeKey)
      case e: ASTCachedProperty if e.entityType == NODE_TYPE =>
        commands.expressions.CachedNodeProperty(e.entityName, getPropertyKey(e.propertyKey), e.runtimeKey)
      case e: ASTCachedProperty if e.entityType == RELATIONSHIP_TYPE =>
        commands.expressions.CachedRelationshipProperty(e.entityName, getPropertyKey(e.propertyKey), e.runtimeKey)
      case e: LogicalProperty =>
        commands.expressions.Property(self.toCommandExpression(id, e.map), getPropertyKey(e.propertyKey))
    }

  private def toCommandExpression(
    id: Id,
    expression: Option[internal.expressions.Expression],
    self: ExpressionConverters
  ): Option[commands.expressions.Expression] =
    expression.map(self.toCommandExpression(id, _))

  private def toCommandExpression(
    id: Id,
    expressions: Seq[internal.expressions.Expression],
    self: ExpressionConverters
  ): Seq[commands.expressions.Expression] =
    expressions.map(self.toCommandExpression(id, _))

  private def variable(e: internal.expressions.LogicalVariable): VariableCommand =
    e match {
      case e: ExpressionVariable => commands.expressions.ExpressionVariable(e.offset, e.name)
      case x                     => commands.expressions.Variable(x.name)
    }

  private def inequalityExpression(
    id: Id,
    original: internal.expressions.InequalityExpression,
    self: ExpressionConverters
  ): predicates.ComparablePredicate = original match {
    case e: internal.expressions.LessThan =>
      predicates.LessThan(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
    case e: internal.expressions.LessThanOrEqual => predicates
        .LessThanOrEqual(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
    case e: internal.expressions.GreaterThan =>
      predicates.GreaterThan(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
    case e: internal.expressions.GreaterThanOrEqual => predicates
        .GreaterThanOrEqual(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs))
  }

  private def getDegree(id: Id, original: internal.expressions.GetDegree, self: ExpressionConverters) = {
    val typ = original.relType.map(relType => UnresolvedRelType(relType.name))
    commands.expressions.GetDegree(self.toCommandExpression(id, original.node), typ, original.dir)
  }

  private def regexMatch(id: Id, e: internal.expressions.RegexMatch, self: ExpressionConverters) =
    self.toCommandExpression(id, e.rhs) match {
      case literal: commands.expressions.Literal =>
        predicates.LiteralRegularExpression(self.toCommandExpression(id, e.lhs), literal)
      case command =>
        predicates.RegularExpression(self.toCommandExpression(id, e.lhs), command)
    }

  private def in(id: Id, e: internal.expressions.In, self: ExpressionConverters) = e.rhs match {

    case internal.expressions.ListLiteral(expressions) if expressions.isEmpty =>
      predicates.Not(predicates.True())

    case _ =>
      predicates.CachedIn(self.toCommandExpression(id, e.lhs), self.toCommandExpression(id, e.rhs), id)
  }

  private def caseExpression(id: Id, e: internal.expressions.CaseExpression, self: ExpressionConverters) = {
    val predicateAlternatives = e.alternatives
      .map { a => (self.toCommandPredicate(id, a._1), self.toCommandExpression(id, a._2)) }
    commands.expressions.CaseExpression(predicateAlternatives, toCommandExpression(id, e.default, self))
  }

  private def hasALabelOrType(
    id: Id,
    e: internal.expressions.HasALabelOrType,
    self: ExpressionConverters
  ): Predicate = {
    predicates.HasALabelOrType(self.toCommandExpression(id, e.entityExpression))
  }

  private def hasALabel(
    id: Id,
    e: internal.expressions.HasALabel,
    self: ExpressionConverters
  ): Predicate = {
    predicates.HasALabel(self.toCommandExpression(id, e.expression))
  }

  private def hasLabelsOrTypes(
    id: Id,
    e: internal.expressions.HasLabelsOrTypes,
    self: ExpressionConverters
  ): Predicate = {
    val preds = e.labelsOrTypes.map {
      l =>
        predicates.HasLabelOrType(self.toCommandExpression(id, e.entityExpression), l.name): Predicate
    }
    commands.predicates.Ands(preds.toArray)
  }

  private def hasLabels(id: Id, e: internal.expressions.HasLabels, self: ExpressionConverters): Predicate = {
    val preds = e.labels.map {
      l =>
        predicates.HasLabel(
          self.toCommandExpression(id, e.expression),
          LazyLabel(l, tokenContext)
        ): Predicate
    }
    commands.predicates.Ands(preds.toArray)
  }

  private def hasDynamicLabels(
    id: Id,
    e: internal.expressions.HasDynamicLabels,
    self: ExpressionConverters
  ): Predicate =
    predicates.HasDynamicLabels(
      self.toCommandExpression(id, e.expression),
      e.labels.map(self.toCommandExpression(id, _))
    ): Predicate

  private def hasDynamicLabelsOrTypes(
    id: Id,
    e: internal.expressions.HasDynamicLabelsOrTypes,
    self: ExpressionConverters
  ): Predicate =
    predicates.HasDynamicLabelsOrTypes(
      self.toCommandExpression(id, e.entityExpression),
      e.labelsOrTypes.map(self.toCommandExpression(id, _))
    ): Predicate

  private def hasAnyLabel(
    id: Id,
    e: internal.expressions.HasAnyLabel,
    self: ExpressionConverters
  ): Predicate =
    predicates.HasAnyLabel(
      self.toCommandExpression(id, e.expression),
      e.labels.map(l => commands.values.KeyToken.Unresolved(l.name, commands.values.TokenType.Label))
    ): Predicate

  private def hasAnyDynamicLabel(
    id: Id,
    e: internal.expressions.HasAnyDynamicLabel,
    self: ExpressionConverters
  ): Predicate =
    predicates.HasAnyDynamicLabel(
      self.toCommandExpression(id, e.expression),
      e.labels.map(self.toCommandExpression(id, _))
    ): Predicate

  private def hasAnyDynamicLabelsOrTypes(
    id: Id,
    e: internal.expressions.HasAnyDynamicLabelsOrTypes,
    self: ExpressionConverters
  ): Predicate =
    predicates.HasAnyDynamicLabelsOrTypes(
      self.toCommandExpression(id, e.entityExpression),
      e.labelsOrTypes.map(self.toCommandExpression(id, _))
    ): Predicate

  private def hasTypes(id: Id, e: internal.expressions.HasTypes, self: ExpressionConverters): Predicate = {
    val preds = e.types.map {
      l =>
        predicates.HasType(
          self.toCommandExpression(id, e.expression),
          commands.values.KeyToken.Unresolved(l.name, commands.values.TokenType.RelType)
        ): Predicate
    }
    commands.predicates.Ands(preds.toArray)
  }

  private def hasDynamicType(
    id: Id,
    e: internal.expressions.HasDynamicType,
    self: ExpressionConverters
  ): Predicate = {
    predicates.HasDynamicType(
      self.toCommandExpression(id, e.expression),
      e.types.map(self.toCommandExpression(id, _))
    ): Predicate
  }

  private def hasAnyDynamicType(
    id: Id,
    e: internal.expressions.HasAnyDynamicType,
    self: ExpressionConverters
  ): Predicate = {
    predicates.HasAnyDynamicType(
      self.toCommandExpression(id, e.expression),
      e.types.map(self.toCommandExpression(id, _))
    ): Predicate
  }

  private def mapItems(
    id: Id,
    items: Seq[(internal.expressions.PropertyKeyName, internal.expressions.Expression)],
    self: ExpressionConverters
  ): Map[String, commands.expressions.Expression] =
    items.map {
      case (name, ex) => name.name -> self.toCommandExpression(id, ex)
    }.toMap

  private def mapProjectionItems(
    id: Id,
    items: Seq[internal.expressions.LiteralEntry],
    self: ExpressionConverters
  ): Map[String, commands.expressions.Expression] =
    items.map {
      case internal.expressions.LiteralEntry(name, ex) => name.name -> self.toCommandExpression(id, ex)
    }.toMap

  private def listComprehension(
    id: Id,
    e: internal.expressions.ListComprehension,
    self: ExpressionConverters
  ): commands.expressions.Expression = {
    val ev = ExpressionVariable.cast(e.variable)
    val filter = e.innerPredicate match {
      case Some(_: internal.expressions.True) | None =>
        self.toCommandExpression(id, e.expression)
      case Some(inner) =>
        commands.expressions.FilterFunction(
          self.toCommandExpression(id, e.expression),
          ev.name,
          ev.offset,
          self.toCommandPredicate(id, inner)
        )
    }
    e.extractExpression match {
      case Some(extractExpression) =>
        commands.expressions.ExtractFunction(
          filter,
          ev.name,
          ev.offset,
          self.toCommandExpression(id, extractExpression)
        )
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
