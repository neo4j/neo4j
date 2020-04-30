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
package org.neo4j.fabric

import org.neo4j.collection.RawIterator
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.procs.FieldSignature.inputField
import org.neo4j.internal.kernel.api.procs.Neo4jTypes
import org.neo4j.internal.kernel.api.procs.ProcedureSignature
import org.neo4j.internal.kernel.api.procs.QualifiedName
import org.neo4j.internal.kernel.api.procs.UserFunctionSignature
import org.neo4j.kernel.api.ResourceTracker
import org.neo4j.kernel.api.procedure.CallableProcedure
import org.neo4j.kernel.api.procedure.CallableUserFunction
import org.neo4j.kernel.api.procedure.Context
import org.neo4j.procedure.Mode
import org.neo4j.procedure.impl.GlobalProceduresRegistry
import org.neo4j.values.AnyValue
import org.neo4j.values.storable.Values

import scala.collection.JavaConverters.bufferAsJavaListConverter
import scala.collection.mutable.ListBuffer

trait ProcedureRegistryTestSupport {


  def userFunction(name: Seq[String], args: Seq[String])(body: => AnyValue): CallableUserFunction.BasicUserFunction =
    new CallableUserFunction.BasicUserFunction(
      new UserFunctionSignature(
        new QualifiedName(name.init.toArray, name.last),
        ListBuffer(args: _*).map(inputField(_, Neo4jTypes.NTAny)).asJava,
        Neo4jTypes.NTAny,
        null, Array[String](), name.last, true
      )
    ) {
      override def apply(ctx: Context, input: Array[AnyValue]): AnyValue = body
    }

  private def procedure(name: Seq[String], args: Seq[String], out: Seq[String], mode: Mode = Mode.DEFAULT)(values: => Seq[Array[AnyValue]]): CallableProcedure =
    new CallableProcedure.BasicProcedure(new ProcedureSignature(
      new QualifiedName(name.init.toArray, name.last),
      ListBuffer(args: _*).map(inputField(_, Neo4jTypes.NTAny)).asJava,
      ListBuffer(out: _*).map(inputField(_, Neo4jTypes.NTAny)).asJava,
      mode, false, null, Array[String](), name.last, null, false, false, false
    )) {
      override def apply(ctx: Context, input: Array[AnyValue], resourceTracker: ResourceTracker): RawIterator[Array[AnyValue], ProcedureException] =
        RawIterator.of(values: _*)
    }

  val procedures: GlobalProceduresRegistry = {
    val reg = new GlobalProceduresRegistry()
    reg.register(userFunction(Seq("const0"), Seq())(Values.intValue(0)))
    reg.register(userFunction(Seq("const1"), Seq())(Values.intValue(1)))
    reg.register(userFunction(Seq("my", "ns", "const0"), Seq("x"))(Values.intValue(2)))
    reg.register(procedure(Seq("my", "ns", "myProcedure"), Seq(), Seq("a", "b"))(Seq(Array(Values.intValue(1), Values.intValue(10)))))
    reg.register(procedure(Seq("my", "ns", "myProcedure2"), Seq("x"), Seq("a", "b"))(Seq(Array(Values.intValue(1), Values.intValue(10)))))
    reg.register(procedure(Seq("my", "ns", "read"), Seq(), Seq("a", "b"), Mode.DEFAULT)(Seq(Array(Values.intValue(1), Values.intValue(10)))))
    reg.register(procedure(Seq("my", "ns", "write"), Seq(), Seq("a", "b"), Mode.WRITE)(Seq(Array(Values.intValue(1), Values.intValue(10)))))
    reg
  }
}
