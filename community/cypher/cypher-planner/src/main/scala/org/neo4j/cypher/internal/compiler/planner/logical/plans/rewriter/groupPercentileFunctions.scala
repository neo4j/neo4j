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
package org.neo4j.cypher.internal.compiler.planner.logical.plans.rewriter

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.functions.MultiPercentileDisc
import org.neo4j.cypher.internal.expressions.functions.PercentileDisc
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

/**
 * If multiple [[PercentileDisc]] functions take the same input, rewrites them to [[MultiPercentileDisc]].
 *
 * For example,
 * {{{
 *   percentileDisc(a,0.5) AS p1,
 *   percentileDisc(a,0.6) AS p2,
 *   percentileDisc(b,0.7) AS p3
 * }}}
 * Would become,
 * {{{
 *   multiPercentileDisc(a,[0.5,0.6],['p1','p2']) AS map,
 *   percentileDisc(b,0.7) AS p3
 * }}}
 */
case class groupPercentileFunctions(anonymousVariableNameGenerator: AnonymousVariableNameGenerator, idGen: IdGen)
    extends Rewriter {

  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  private val pos: InputPosition = InputPosition.NONE

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case aggregation @ Aggregation(_, _, aggregations: Map[LogicalVariable, Expression]) =>
      val groupedPercentileFunctions = groupPercentileFunctions(aggregations)

      if (groupedPercentileFunctions.isEmpty) {
        aggregation
      } else {
        val functionVariableMappings: Map[FunctionInvocation, Seq[LogicalVariable]] =
          toMultiPercentileFunctions(groupedPercentileFunctions)

        val (multiPercentileExpressions, variableMappings) =
          functionVariableMappings.foldLeft(
            (Map.empty[LogicalVariable, Expression], Map.empty[LogicalVariable, Expression])
          ) {
            case ((accAggregations, accMappings), (multiPercentileFun, variables)) =>
              val mapVar = varFor(anonymousVariableNameGenerator.nextName)
              (
                accAggregations + (mapVar -> multiPercentileFun),
                accMappings ++ variables.map(v => (v, mapVar))
              )
          }

        val aggregationsToRemove = groupedPercentileFunctions.flatMap { case (_, fs) => fs.keys }.toSet
        val newAggregations = (aggregations -- aggregationsToRemove) ++ multiPercentileExpressions
        val newAggregation = aggregation.copy(aggregationExpressions = newAggregations)(SameId(aggregation.id))

        val projectExpressions = variableMappings.map {
          case (projectTo: LogicalVariable, map: LogicalVariable) =>
            projectTo -> Property(map, PropertyKeyName(varToKey(projectTo))(pos))(pos)
        }

        Projection(newAggregation, projectExpressions)(idGen)
      }
  })

  /**
   * Filters out all aggregation expressions other than [[PercentileDisc]] and returns those (if any) expressions
   * grouped on their input variable.
   *
   * For example,
   * {{{
   *   percentileDisc(a,0.5) AS p1,
   *   percentileDisc(a,0.6) AS p2,
   *   percentileDisc(b,0.7) AS p3
   * }}}
   * Would become,
   * {{{
   *   Map(a -> Map(p1 -> 'percentileDisc(a,0.5)', p2 -> 'percentileDisc(a,0.6)'))
   * }}}
   *
   * @return groups that contain at least two expressions.
   */
  private def groupPercentileFunctions(aggregationExpressions: Map[LogicalVariable, Expression])
    : Map[LogicalVariable, Map[LogicalVariable, FunctionInvocation]] = {
    aggregationExpressions.collect {
      case (v, f @ FunctionInvocation(_, FunctionName(name), false, _)) if name == PercentileDisc.name => (v, f)
    }.groupBy { case (_, f: FunctionInvocation) => f.args(0).asInstanceOf[LogicalVariable] }.filter { case (_, fs) =>
      fs.size > 1
    }
  }

  /**
   * Given a map of grouped [[PercentileDisc]] expressions, create a [[MultiPercentileDisc]] expression for each group.
   *
   * For example,
   * {{{
   *   Map(
   *      a -> Map(p1 -> 'percentileDisc(a,0.5)', p2 -> 'percentileDisc(a,0.6)'),
   *      b -> Map(p3 -> 'percentileDisc(b,0.7)', p4 -> 'percentileDisc(b,0.8)')
   *   )
   * }}}
   * Would become,
   * {{{
   *   Map(
   *      multiPercentileDisc(a,[0.5,0.6],['p1','p2']) -> [p1,p2],
   *      multiPercentileDisc(b,[0.7,0.8],['p3','p4']) -> [p3,p4]
   *   )
   * }}}
   *
   * @return mapping of multi percentile functions to the variables which eventually need to be projected.
   */
  private def toMultiPercentileFunctions(groupedFunctions: Map[
    LogicalVariable,
    Map[LogicalVariable, FunctionInvocation]
  ]): Map[FunctionInvocation, Seq[LogicalVariable]] = {
    groupedFunctions.map { case (inputNumberVariable, percentileGroup: Map[LogicalVariable, FunctionInvocation]) =>
      val (variables, percentiles) = toVariablePercentilePairs(percentileGroup)

      val percentilesLiteral = ListLiteral(percentiles)(pos)
      val propertyKeys = ListLiteral(variables.map(v => StringLiteral(varToKey(v))(pos)))(pos)
      val multiPercentileFunctionInvocation = FunctionInvocation(
        FunctionName(MultiPercentileDisc.name)(pos),
        distinct = false,
        IndexedSeq(inputNumberVariable, percentilesLiteral, propertyKeys)
      )(pos)

      (multiPercentileFunctionInvocation, variables)
    }
  }

  /**
   * Given an already-grouped percentile functions map, for a single input,
   * method will returns the variable:percentile pairs.
   *
   * For example,
   * {{{
   *   Map(p1 -> 'percentileDisc(a,0.5)', p2 -> 'percentileDisc(a,0.6)')
   * }}}
   * Would become,
   * {{{
   *   [(p1,p2),(0.5,0.6)]
   * }}}
   *
   * @return variable:percentile pairs
   */
  private def toVariablePercentilePairs(percentileGroup: Map[LogicalVariable, FunctionInvocation])
    : (Seq[LogicalVariable], Seq[Expression]) = {
    percentileGroup.foldLeft((Seq.empty[LogicalVariable], Seq.empty[Expression])) {
      case ((accVars, accPercentiles), (v, FunctionInvocation(_, _, _, args))) =>
        (accVars :+ v, accPercentiles :+ args(1))
    }
  }

  private def varToKey(variable: LogicalVariable): String = variable.name
}
