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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.commands.expressions

import org.neo4j.cypher.internal.compatibility.v3_3.runtime.ExecutionContext
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers.{RuntimeJavaValueConverter, RuntimeScalaValueConverter}
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.QueryState
import org.neo4j.cypher.internal.compatibility.v3_3.runtime.pipes.aggregation.AggregationFunction
import org.neo4j.cypher.internal.compiler.v3_3.spi.UserFunctionSignature

case class AggregationFunctionInvocation(signature: UserFunctionSignature, arguments: IndexedSeq[Expression])
  extends AggregationExpression {

  override def createAggregationFunction: AggregationFunction = new AggregationFunction {
    private var inner: UserDefinedAggregator = null

    override def result(implicit state:QueryState) = {
      val isGraphKernelResultValue = state.query.isGraphKernelResultValue _
      val scalaValues = new RuntimeScalaValueConverter(isGraphKernelResultValue, state.typeConverter.asPrivateType)
      scalaValues.asDeepScalaValue(aggregator.result)
    }

    override def apply(data: ExecutionContext)
                      (implicit state: QueryState) = {
      val converter = new RuntimeJavaValueConverter(state.query.isGraphKernelResultValue, state.typeConverter.asPublicType)
      val argValues = arguments.map(arg => converter.asDeepJavaValue(arg(data)(state)))
      aggregator.update(argValues)
    }

    private def aggregator(implicit state: QueryState) = {
      if (inner == null) {
        inner = state.query.aggregateFunction(signature.name, signature.allowed)
      }
      inner
    }


  }

  override def rewrite(f: (Expression) => Expression): Expression = f(
    AggregationFunctionInvocation(signature, arguments.map(a => a.rewrite(f))))

  override def symbolTableDependencies: Set[String] = arguments.flatMap(_.symbolTableDependencies).toSet
}
