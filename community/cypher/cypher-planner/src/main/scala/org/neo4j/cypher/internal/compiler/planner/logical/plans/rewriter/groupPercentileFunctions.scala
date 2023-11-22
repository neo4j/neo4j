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
import org.neo4j.cypher.internal.expressions.Namespace
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.expressions.functions.MultiPercentileDisc
import org.neo4j.cypher.internal.expressions.functions.PercentileDisc
import org.neo4j.cypher.internal.logical.plans.Aggregation
import org.neo4j.cypher.internal.logical.plans.Projection
import org.neo4j.cypher.internal.macros.AssertMacros
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.attribution.IdGen
import org.neo4j.cypher.internal.util.attribution.SameId
import org.neo4j.cypher.internal.util.bottomUp

/**
 * TODO
 */
case class groupPercentileFunctions(anonymousVariableNameGenerator: AnonymousVariableNameGenerator, idGen: IdGen)
    extends Rewriter {

  override def apply(input: AnyRef): AnyRef = instance.apply(input)

  private val instance: Rewriter = bottomUp(Rewriter.lift {
    case aggregation @ Aggregation(_, _, aggregationExpressions: Map[LogicalVariable, Expression]) =>
      // TODO document
      // percentile(inputNumberVariable,0.5) AS outputPercentileVariable
      // groupedPercentileFunctions: Map[inputNumberVariable, Map[outputPercentileVariable, percentile(inputNumberVariable,0.5)]]
      val groupedPercentileFunctions: Map[LogicalVariable, Map[LogicalVariable, FunctionInvocation]] =
        aggregationExpressions
          .collect {
            case entry @ (_, FunctionInvocation(_, FunctionName(name), false, _)) if name == PercentileDisc.name =>
              entry.asInstanceOf[(LogicalVariable, FunctionInvocation)] // TODO this cast is annoying
          }
          .groupBy { case (_, f: FunctionInvocation) => f.args(0).asInstanceOf[LogicalVariable] }
          .filter { case (_, fs) => fs.size > 1 }

      if (groupedPercentileFunctions.isEmpty) {
        aggregation
      } else {
        // TODO document
        // percentile(inputNumberVariable,0.5) AS outputPercentileVariable
        // groupedPercentileFunctions: Map[inputNumberVariable, Map[outputPercentileVariable, percentile(inputNumberVariable,0.5)]]
        // ===>
        // Seq[ ( newMapVar, MultiPercentileDisc, Seq[(mapKey,percentile)] ) ]
        val combinedPercentileFunctions
          : Seq[(LogicalVariable, FunctionInvocation, Seq[(PropertyKeyName, LogicalVariable)])] =
          combineGroups(groupedPercentileFunctions)

        val (
          multiPercentileAggregationExpressions: Map[LogicalVariable, Expression],
          mapKeyVariableMappings: Seq[(LogicalVariable, PropertyKeyName, LogicalVariable)]
        ) =
          combinedPercentileFunctions.foldLeft((
            Map.empty[LogicalVariable, Expression],
            Seq.empty[(LogicalVariable, PropertyKeyName, Expression)]
          )) {
            case (
                (accMultiPercentileAggregationExpressions, accMapKeyVariableMappings),
                (mapVar, multiPercentileFun, propertyKeyToVariableMapping: Seq[(PropertyKeyName, LogicalVariable)])
              ) =>
              val newMapKeyVariableMappings = propertyKeyToVariableMapping.map { case (key, percentileExpression) =>
                (mapVar, key, percentileExpression)
              }
              (
                accMultiPercentileAggregationExpressions + (mapVar -> multiPercentileFun),
                accMapKeyVariableMappings ++ newMapKeyVariableMappings
              )
          }

        val groupedPercentilesToRemove = groupedPercentileFunctions.flatMap { case (_, fs) => fs.keys }.toSet
        val newAggregationExpressions =
          (aggregationExpressions -- groupedPercentilesToRemove) ++ multiPercentileAggregationExpressions
        val newAggregation =
          aggregation.copy(aggregationExpressions = newAggregationExpressions)(SameId(aggregation.id))

        val projectExpressions: Map[LogicalVariable, Expression] =
          mapKeyVariableMappings.foldLeft(Map.empty[LogicalVariable, Expression]) {
            case (accProjectionsMap, (map: LogicalVariable, key: PropertyKeyName, projectTo: LogicalVariable)) =>
              accProjectionsMap + (projectTo -> Property(map, key)(InputPosition.NONE))
          }

        Projection(newAggregation, projectExpressions)(idGen)
      }
  })

  // TODO scala doc
  // percentile(inputNumberVariable,0.5) AS outputPercentileVariable
  // groupedPercentileFunctions: Map[inputNumberVariable, Map[outputPercentileVariable, percentile(inputNumberVariable,0.5)]]
  // ===>
  // Seq[ ( newMapVar, MultiPercentileDisc, Seq[(mapKey,variable)] ) ]
  private def combineGroups(groupedFunctions: Map[LogicalVariable, Map[LogicalVariable, FunctionInvocation]])
    : Seq[(LogicalVariable, FunctionInvocation, Seq[(PropertyKeyName, LogicalVariable)])] = {
    groupedFunctions.map { case (inputNumberVariable, percentileGroup: Map[LogicalVariable, FunctionInvocation]) =>
      val (namespace, variables, percentiles) = extractPercentileGroupInfo(percentileGroup)

      val percentilesLiteral = ListLiteral(percentiles)(InputPosition.NONE)
      val propertyKeys = ListLiteral(variables.map(v => StringLiteral(v.name)(InputPosition.NONE)))(InputPosition.NONE)

      /*
      TODO use the object apply method below and then namespace would not need to be tracked at all
      object FunctionInvocation {
        def apply(
          functionName: FunctionName,
          distinct: Boolean,
          args: IndexedSeq[Expression]
        )(position: InputPosition): FunctionInvocation =
          FunctionInvocation(Namespace()(position), functionName, distinct, args)(position)
      }
       */

      val multiPercentileFunctionInvocation = new FunctionInvocation(
        namespace,
        FunctionName(MultiPercentileDisc.name)(InputPosition.NONE),
        distinct = false,
        IndexedSeq(inputNumberVariable, percentilesLiteral, propertyKeys)
      )(InputPosition.NONE)

      val mapVariable = varFor(anonymousVariableNameGenerator.nextName)
      val propertyKeyToVariableMapping: Seq[(PropertyKeyName, LogicalVariable)] =
        variables.map(v => (PropertyKeyName(v.name)(InputPosition.NONE), v))

      (mapVariable, multiPercentileFunctionInvocation, propertyKeyToVariableMapping)
    }.toSeq
  }

  // TODO scala doc
  // percentile(inputNumberVariable,0.5) AS outputPercentileVariable
  // groupedPercentileFunctions: Map[inputNumberVariable, Map[outputPercentileVariable, percentile(inputNumberVariable,0.5)]]
  // percentileGroup: Map[outputPercentileVariable, percentile(inputNumberVariable,0.5)]
  // ===>
  // (namespace, Seq[(mapKey,percentile)])
  private def extractPercentileGroupInfo(percentileGroup: Map[LogicalVariable, FunctionInvocation])
    : (Namespace, Seq[LogicalVariable], Seq[Expression]) = {
    percentileGroup.foldLeft((null.asInstanceOf[Namespace], Seq.empty[LogicalVariable], Seq.empty[Expression])) {
      case ((accNs, accVariables, accPercentiles), (variable, FunctionInvocation(ns, _, _, args))) =>
        AssertMacros.checkOnlyWhenAssertionsAreEnabled(
          accNs == null || accNs.parts == ns.parts,
          s"Expected all percentile functions to be in the same namespace: $accNs, $ns"
        )

        val percentile = args(1)
        (ns, accVariables :+ variable, accPercentiles :+ percentile)
    }
  }
}
