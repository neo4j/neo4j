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
package org.neo4j.cypher.internal.ir.ast

import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.AllIterablePredicate
import org.neo4j.cypher.internal.expressions.BooleanExpression
import org.neo4j.cypher.internal.expressions.ContainerIndex
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LogicalVariable
import org.neo4j.cypher.internal.expressions.Subtract
import org.neo4j.cypher.internal.expressions.UnPositionedVariable
import org.neo4j.cypher.internal.expressions.UnsignedDecimalIntegerLiteral
import org.neo4j.cypher.internal.expressions.VariableGrouping
import org.neo4j.cypher.internal.expressions.functions
import org.neo4j.cypher.internal.ir.QuantifiedPathPattern
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition

/**
 * A QPP predicate that was lifted into outer Selections.
 * Equivalent to:
 *   all(i IN range(0, size([[groupVariableAnchor]]) - 1) WHERE <rewrittenPredicate>)
 * Where <rewrittenPredicate> is [[originalInnerPredicate]] with singleton variables replaced
 * by indexed group variables (i.e. `singletonVar` -> `groupVar[i]`.
 *
 * @param groupVariableAnchor arbitrary group variable used to create a dependency between lifted predicate and the original QPP
 * @param variableGroupings variable groupings used to translate singleton variables into group variables
 * @param originalInnerPredicate original predicate using singleton variables
 */
case class ForAllRepetitions(
  groupVariableAnchor: LogicalVariable,
  variableGroupings: Set[expressions.VariableGrouping],
  originalInnerPredicate: Expression
)(override val position: InputPosition) extends BooleanExpression {

  override def isConstantForQuery: Boolean = originalInnerPredicate.isConstantForQuery

  final override def dependencies: Set[LogicalVariable] = {
    translatedSingletonDependencies + groupVariableAnchor
  }

  def asAllIterablePredicate(anonymousVariableNameGenerator: AnonymousVariableNameGenerator): Expression = {
    val pos = InputPosition.NONE

    val iterVar = UnPositionedVariable.varFor(anonymousVariableNameGenerator.nextName)

    val singletonReplacements: Set[(LogicalVariable, LogicalVariable)] =
      originalInnerPredicate.dependencies.flatMap { v =>
        groupVariableFor(v).map(v -> _)
      }

    val rewrittenPredicate = singletonReplacements.foldLeft(originalInnerPredicate) {
      case (expr, (singletonVar, groupVar)) =>
        def indexedGroupVar: Expression = ContainerIndex(groupVar.copyId, iterVar.copyId)(pos)
        // x -> xGroup[iterVar]
        expr.replaceAllOccurrencesBy(singletonVar, indexedGroupVar)
    }

    AllIterablePredicate(
      iterVar,
      functions.Range.asInvocation(
        UnsignedDecimalIntegerLiteral("0")(pos),
        Subtract(
          functions.Size(groupVariableAnchor.copyId)(pos),
          UnsignedDecimalIntegerLiteral("1")(pos)
        )(pos)
      )(pos),
      Some(rewrittenPredicate)
    )(originalInnerPredicate.position)
  }

  private def groupVariableFor(singleton: LogicalVariable): Option[LogicalVariable] = {
    variableGroupings.collectFirst {
      case expressions.VariableGrouping(`singleton`, group) => group
    }
  }

  private lazy val translatedSingletonDependencies: Set[LogicalVariable] = {
    originalInnerPredicate.dependencies.map { v =>
      groupVariableFor(v).getOrElse(v)
    }
  }
}

object ForAllRepetitions {

  def apply(qpp: QuantifiedPathPattern, predicate: Expression): ForAllRepetitions = {

    val groupVariableAnchor =
      predicate.dependencies.flatMap(v => VariableGrouping.singletonToGroup(qpp.variableGroupings, v))
        .minOption(Ordering.by[LogicalVariable, String](_.name))
        .getOrElse(qpp.variableGroupNames.min(
          Ordering.by[LogicalVariable, String](_.name)
        )) // if the predicate doesn't use any QPP variables, just pick one

    val astVariableGroupings = qpp.variableGroupings.map { irGrouping =>
      expressions.VariableGrouping(
        singleton = irGrouping.singleton,
        group = irGrouping.group
      )(InputPosition.NONE)
    }

    ForAllRepetitions(
      groupVariableAnchor,
      astVariableGroupings,
      predicate
    )(predicate.position)
  }
}
