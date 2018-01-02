/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.pipes

import org.neo4j.cypher.internal.compiler.v2_3.planDescription._
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

class UnionPipeTest extends CypherFunSuite {
  test("union between three pipes produces expected an expected plan description") {
    val pipeA = fakePipe("A")
    val pipeB = fakePipe("B")
    val pipeC = fakePipe("C")
    val sources = List(pipeA, pipeB, pipeC)

    val pipe = UnionPipe(sources, List("a"))(mock[PipeMonitor])
    val A = PlanDescriptionImpl(pipeA.id, "A", NoChildren, Seq.empty, Set.empty)
    val B = PlanDescriptionImpl(pipeB.id, "B", NoChildren, Seq.empty, Set.empty)
    val C = PlanDescriptionImpl(pipeC.id, "C", NoChildren, Seq.empty, Set.empty)
    val AuB = PlanDescriptionImpl(pipe.id, "Union", TwoChildren(A, B), Seq.empty, Set("a"))
    val AuBuC = PlanDescriptionImpl(pipe.id, "Union", TwoChildren(AuB, C), Seq.empty, Set("a"))

    val description = pipe.planDescription
    description should equal(AuBuC)
  }

  private def fakePipe(name: String): Pipe = new Pipe {
    def monitor = ???

    def sources = ???

    def localEffects = ???

    def planDescription = new PlanDescriptionImpl(id, name, NoChildren, Seq.empty, Set.empty)

    def symbols = ???

    protected def internalCreateResults(state: QueryState) = ???

    def dup(sources: List[Pipe]) = ???

    def exists(pred: (Pipe) => Boolean) = ???
  }
}
