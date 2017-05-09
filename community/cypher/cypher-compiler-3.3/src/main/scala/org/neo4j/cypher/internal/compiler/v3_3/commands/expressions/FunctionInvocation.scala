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
package org.neo4j.cypher.internal.compiler.v3_3.commands.expressions

import org.neo4j.cypher.internal.compiler.v3_3._
import org.neo4j.cypher.internal.compiler.v3_3.helpers.{RuntimeJavaValueConverter, RuntimeScalaValueConverter}
import org.neo4j.cypher.internal.compiler.v3_3.mutation.GraphElementPropertyFunctions
import org.neo4j.cypher.internal.compiler.v3_3.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_3.spi.UserFunctionSignature

case class FunctionInvocation(signature: UserFunctionSignature, arguments: IndexedSeq[Expression])
  extends Expression with GraphElementPropertyFunctions {

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
   val query = state.query

    val isGraphKernelResultValue = query.isGraphKernelResultValue _
    val converter = new RuntimeJavaValueConverter(state.query.isGraphKernelResultValue, state.typeConverter.asPublicType)
    val scalaValues = new RuntimeScalaValueConverter(isGraphKernelResultValue, state.typeConverter.asPrivateType)
    val argValues = arguments.map(arg => converter.asDeepJavaValue(arg(ctx)(state)))

    val result = query.callFunction(signature.name, argValues, signature.allowed)

    scalaValues.asDeepScalaValue(result)
  }

  override def rewrite(f: (Expression) => Expression) =
    f(FunctionInvocation(signature, arguments.map(a => a.rewrite(f))))

  override def symbolTableDependencies = arguments.flatMap(_.symbolTableDependencies).toSet

  override def toString = s"${signature.name}(${arguments.mkString(",")})"
}