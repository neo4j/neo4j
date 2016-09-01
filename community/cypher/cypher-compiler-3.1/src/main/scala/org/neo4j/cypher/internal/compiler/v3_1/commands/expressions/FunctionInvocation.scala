/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.commands.expressions

import org.neo4j.cypher.internal.compiler.v3_1._
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.ProcedureCallMode
import org.neo4j.cypher.internal.compiler.v3_1.helpers.RuntimeScalaValueConverter
import org.neo4j.cypher.internal.compiler.v3_1.mutation.GraphElementPropertyFunctions
import org.neo4j.cypher.internal.compiler.v3_1.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_1.spi.UserDefinedFunctionSignature
import org.neo4j.cypher.internal.compiler.v3_1.symbols.SymbolTable

case class FunctionInvocation(signature: UserDefinedFunctionSignature, arguments: IndexedSeq[Expression])
  extends Expression with GraphElementPropertyFunctions {

  private val callMode = ProcedureCallMode.fromAccessMode(signature.accessMode)

  override def apply(ctx: ExecutionContext)(implicit state: QueryState): Any = {
   val query = state.query

    val result = callMode.callFunction(query, signature.name, arguments)

    val isGraphKernelResultValue = query.isGraphKernelResultValue _
    val scalaValues = new RuntimeScalaValueConverter(isGraphKernelResultValue, state.typeConverter.asPrivateType)
    scalaValues.asDeepScalaValue(result)
  }

  override def rewrite(f: (Expression) => Expression) =
    f(FunctionInvocation(signature, arguments.map(a => a.rewrite(f))))

  override def calculateType(symbols: SymbolTable) = signature.outputField.typ

  override def symbolTableDependencies = arguments.flatMap(_.symbolTableDependencies).toSet

  override def toString = s"${signature.name}(${arguments.mkString(",")})"
}
