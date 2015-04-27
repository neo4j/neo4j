/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.birk.il

import org.neo4j.collection.primitive.PrimitiveLongIterator
import org.neo4j.cypher.internal.compiler.v2_3.birk.JavaSymbol
import org.neo4j.cypher.internal.compiler.v2_3.birk.profiling.ProfilingTracer
import org.neo4j.cypher.internal.compiler.v2_3.planDescription._
import org.neo4j.cypher.internal.compiler.v2_3.test_helpers.CypherFunSuite
import org.neo4j.function.Supplier
import org.neo4j.kernel.api._

import org.mockito.Mockito._

class CompiledProfilingTest extends CypherFunSuite with CodeGenSugar {

  test("foo") {
    val id1 = new Id()
    val id2 = new Id()

    val compiled = compile(WhileLoop(JavaSymbol("name", "Object"),
        ScanAllNodes("OP1"), AcceptVisitor("OP2", Map.empty, Map.empty, Map.empty)))

    val statement = mock[Statement]
    val readOps = mock[ReadOperations]
    when(statement.readOperations()).thenReturn(readOps)

    when(readOps.nodesGetAll()).thenReturn(new PrimitiveLongIterator {
      private var counter = 0

      override def next(): Long = counter

      override def hasNext: Boolean = {
        counter += 1
        counter < 3
      }
    })

    val supplier = new Supplier[InternalPlanDescription] {
      override def get(): InternalPlanDescription = PlanDescriptionImpl(id2, "accept", SingleChild(PlanDescriptionImpl(id1, "scanallnodes", NoChildren, Seq.empty, Set.empty)), Seq.empty, Set.empty)
    }

    val tracer = new ProfilingTracer()

    val instance = newInstance(compiled, statement = statement, supplier = supplier, queryExecutionTracer = tracer)
    instance.size

    tracer.dbHitsOf(id2) should equal(3)
//
    val list = evaluate(instance)
    println(instance.executionPlanDescription())

//    list.foreach(l => println(l))
  }


}
