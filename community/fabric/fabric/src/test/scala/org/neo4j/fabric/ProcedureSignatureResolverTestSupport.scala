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
package org.neo4j.fabric

import org.neo4j.collection.RawIterator
import org.neo4j.cypher.internal.compiler.helpers.SignatureResolver
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignature
import org.neo4j.cypher.internal.frontend.phases.ProcedureSignatureResolver
import org.neo4j.cypher.internal.frontend.phases.QualifiedName
import org.neo4j.cypher.internal.frontend.phases.UserFunctionSignature
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs
import org.neo4j.internal.kernel.api.procs.FieldSignature.inputField
import org.neo4j.internal.kernel.api.procs.FieldSignature.outputField
import org.neo4j.internal.kernel.api.procs.ProcedureSignature.VOID
import org.neo4j.kernel.api.ResourceMonitor
import org.neo4j.kernel.api.procedure
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.procedure.Mode
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters.SeqHasAsJava

trait ProcedureSignatureResolverTestSupport {

  val callableProcedures: Seq[procedure.CallableProcedure] = Seq(
    mkProcedure(Seq("my", "ns", "myProcedure"), Seq(), Seq("a", "b"))(Seq(Array(
      Values.intValue(1),
      Values.intValue(10)
    ))),
    mkProcedure(Seq("my", "ns", "myProcedure2"), Seq("x"), Seq("a", "b"))(Seq(Array(
      Values.intValue(1),
      Values.intValue(10)
    ))),
    mkProcedure(Seq("my", "ns", "read"), Seq(), Seq("a", "b"), Mode.DEFAULT)(Seq(Array(
      Values.intValue(1),
      Values.intValue(10)
    ))),
    mkProcedure(Seq("my", "ns", "write"), Seq(), Seq("a", "b"), Mode.WRITE)(Seq(Array(
      Values.intValue(1),
      Values.intValue(10)
    ))),
    mkProcedure(Seq("my", "ns", "unitProcedure"), Seq(), Seq(), Mode.WRITE)(Seq.empty)
  )

  val callableUseFunctions: Seq[procedure.CallableUserFunction] = Seq(
    mkFunction(Seq("const0"), Seq(), "MyCategory")(Values.intValue(0)),
    mkFunction(Seq("const1"), Seq(), "MyOtherCategory")(Values.intValue(1)),
    mkFunction(Seq("my", "ns", "const0"), Seq("x"), "MyCategory")(Values.intValue(2)),
    mkFunction(Seq("say", "neo4j"), Seq(), "MyCategory")(Values.stringValue("neo4j"))
  )

  val signatures: ProcedureSignatureResolver = TestProcedureSignatureResolver(callableProcedures, callableUseFunctions)

  case class TestProcedureSignatureResolver(
    procedures: Seq[procedure.CallableProcedure],
    functions: Seq[procedure.CallableUserFunction],
    procedureSignatureVersion: Long = -1
  ) extends ProcedureSignatureResolver {

    private val procSignatures = procedures.zipWithIndex.map { case (procedure, i) =>
      val handle = new procs.ProcedureHandle(procedure.signature(), i)
      SignatureResolver.toCypherProcedure(handle)
    }

    private val funcSignatures = functions.zipWithIndex.map { case (function, i) =>
      val handle = new procs.UserFunctionHandle(function.signature(), i)
      SignatureResolver.toCypherFunction(handle)
    }

    override def procedureSignature(name: QualifiedName): ProcedureSignature =
      procSignatures.find(_.name == name).getOrElse(throw new RuntimeException(s"No such procedure $name"))

    override def functionSignature(name: QualifiedName): Option[UserFunctionSignature] =
      funcSignatures.find(_.name == name)
  }

  private def mkFunction(
    name: Seq[String],
    args: Seq[String],
    category: String
  )(
    body: => AnyValue
  ): procedure.CallableUserFunction =
    new procedure.CallableUserFunction.BasicUserFunction(
      new procs.UserFunctionSignature(
        new procs.QualifiedName(name.init.toArray, name.last),
        ListBuffer(args: _*).map(inputField(_, procs.Neo4jTypes.NTAny)).asJava,
        procs.Neo4jTypes.NTAny,
        false,
        null,
        name.last,
        category,
        true,
        false,
        false,
        false
      )
    ) {
      override def apply(ctx: procedure.Context, input: Array[AnyValue]): AnyValue = body
    }

  private def mkProcedure(
    name: Seq[String],
    args: Seq[String],
    out: Seq[String],
    mode: Mode = Mode.DEFAULT
  )(
    values: => Seq[Array[AnyValue]]
  ): procedure.CallableProcedure = {
    val outputSignature =
      if (out.isEmpty) VOID else ListBuffer(out: _*).map(outputField(_, procs.Neo4jTypes.NTAny)).asJava

    new procedure.CallableProcedure.BasicProcedure(new procs.ProcedureSignature(
      new procs.QualifiedName(name.init.toArray, name.last),
      ListBuffer(args: _*).map(inputField(_, procs.Neo4jTypes.NTAny)).asJava,
      outputSignature,
      mode,
      false,
      false,
      null,
      name.last,
      null,
      false,
      false,
      false,
      false,
      false,
      false
    )) {
      override def apply(
        ctx: Context,
        input: Array[AnyValue],
        resourceMonitor: ResourceMonitor
      ): RawIterator[Array[AnyValue], ProcedureException] =
        RawIterator.of(values: _*)
    }
  }

}
