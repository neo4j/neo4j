/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.collection.RawIterator
import org.neo4j.cypher._
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.ProcedureSignature.procedureSignature
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature.functionSignature
import org.neo4j.internal.kernel.api.procs.{Neo4jTypes, ProcedureSignature, UserAggregator}
import org.neo4j.kernel.api.ResourceTracker
import org.neo4j.kernel.api.proc.CallableProcedure.BasicProcedure
import org.neo4j.kernel.api.proc.CallableUserAggregationFunction.BasicUserAggregationFunction
import org.neo4j.kernel.api.proc.CallableUserFunction.BasicUserFunction
import org.neo4j.kernel.api.proc.Context
import org.neo4j.values.AnyValue

abstract class ProcedureCallAcceptanceTest extends ExecutionEngineFunSuite {

  protected def registerDummyInOutProcedure(types: Neo4jTypes.AnyType*) =
    registerProcedure("my.first.proc") { builder =>

      for (i <- types.indices) {
        builder
          .in(s"in$i", types(i))
          .out(s"out$i", types(i))
      }

      new BasicProcedure(builder.build) {
        override def apply(ctx: Context, input: Array[AnyRef],
                           resourceTracker: ResourceTracker): RawIterator[Array[AnyRef], ProcedureException] =
          RawIterator.of[Array[AnyRef], ProcedureException](input)
      }
  }

  protected def registerProcedureReturningSingleValue(value: AnyRef) =
    registerProcedure("my.first.value") { builder =>
      val builder = procedureSignature(Array("my", "first"), "value")
      builder.out("out", Neo4jTypes.NTAny)

      new BasicProcedure(builder.build) {
        override def apply(ctx: Context, input: Array[AnyRef],
                           resourceTracker: ResourceTracker): RawIterator[Array[AnyRef], ProcedureException] =
          RawIterator.of[Array[AnyRef], ProcedureException](Array(value))
      }
    }

  protected def registerUserFunction(value: AnyValue, typ: Neo4jTypes.AnyType = Neo4jTypes.NTAny) =
    registerUserDefinedFunction("my.first.value") { builder =>
      val builder = functionSignature(Array("my", "first"), "value")
      builder.out(typ)

      new BasicUserFunction(builder.build) {
        override def apply(ctx: Context, input: Array[AnyValue]): AnyValue = value
      }
    }

  protected def registerUserAggregationFunction(value: AnyRef, typ: Neo4jTypes.AnyType = Neo4jTypes.NTAny) =
    registerUserDefinedAggregationFunction("my.first.value") { builder =>
      val builder = functionSignature(Array("my", "first"), "value")
      builder.out(typ)

      new BasicUserAggregationFunction(builder.build) {

        override def create(ctx: Context): UserAggregator = new UserAggregator {

          override def result() = value

          override def update(input: Array[AnyRef]) = {}
        }
      }
    }


  protected def registerVoidProcedure() =
    registerProcedure("dbms.do_nothing") { builder =>
      builder.out(ProcedureSignature.VOID)

      new BasicProcedure(builder.build) {
        override def apply(ctx: Context, input: Array[AnyRef],
                           resourceTracker: ResourceTracker): RawIterator[Array[AnyRef], ProcedureException] =
          RawIterator.empty()
      }
    }

  protected def registerProcedureReturningNoRowsOrColumns() =
    registerProcedure("dbms.return_nothing") { builder =>
      new BasicProcedure(builder.build) {
        override def apply(ctx: Context, input: Array[AnyRef],
                           resourceTracker: ResourceTracker): RawIterator[Array[AnyRef], ProcedureException] =
          RawIterator.empty()
      }
    }
}
