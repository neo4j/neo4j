/**
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
package org.neo4j.cypher.internal.compiler.v2_1.executionplan

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_1.pipes._
import org.neo4j.cypher.internal.compiler.v2_1.planDescription.PlanDescription
import org.neo4j.cypher.internal.compiler.v2_1.symbols.SymbolTable

class AddEagernessIfNecessaryTest extends CypherFunSuite {
  implicit val monitor = mock[PipeMonitor]

  test("NONE -> NONE need no eagerness") {
    testThatGoingFrom(Effects.NONE)
    .to(Effects.NONE)
    .doesNotIntroduceEagerness()
  }

  test("NONE -> READS need no eagerness") {
    testThatGoingFrom(Effects.NONE)
    .to(Effects.READS_ENTITIES)
    .doesNotIntroduceEagerness()
  }

  test("WRITES -> NONE need no eagerness") {
    testThatGoingFrom(Effects.WRITES_NODES)
    .to(Effects.NONE)
    .doesNotIntroduceEagerness()
  }

  test("WRITES -> WRITES need no eagerness") {
    testThatGoingFrom(Effects.WRITES_ENTITIES)
    .to(Effects.WRITES_ENTITIES)
    .doesNotIntroduceEagerness()
  }

  test("READS -> WRITES needs eagerness") {
    testThatGoingFrom(Effects.READS_ENTITIES)
    .to(Effects.WRITES_ENTITIES)
    .doesIntroduceEagerness()
  }

  test("WRITES -> READS needs no eagerness") {
    testThatGoingFrom(Effects.WRITES_ENTITIES)
    .to(Effects.READS_ENTITIES)
    .doesNotIntroduceEagerness()
  }

  test("READS -> READS needs no eagerness") {
    testThatGoingFrom(Effects.READS_ENTITIES)
    .to(Effects.READS_ENTITIES)
    .doesNotIntroduceEagerness()
  }

  test("READS NODES -> WRITE RELS does not need eagerness") {
    testThatGoingFrom(Effects.READS_NODES)
    .to(Effects.WRITES_RELATIONSHIPS)
    .doesNotIntroduceEagerness()
  }

  test("READS ALL -> WRITE RELS needs eagerness") {
    testThatGoingFrom(Effects.READS_ENTITIES)
    .to(Effects.WRITES_RELATIONSHIPS)
    .doesIntroduceEagerness()
  }

  test("READS RELS -> WRITE NODES needs not eagerness") {
    testThatGoingFrom(Effects.READS_RELATIONSHIPS)
    .to(Effects.WRITES_NODES)
    .doesNotIntroduceEagerness()
  }

  test("NONE -> READS RELS -> WRITE NODES -> NONE needs not eagerness") {
    val a = FakePipeWithSources("a", List.empty, Effects.NONE)
    val b = FakePipeWithSources("b", List(a), Effects.READS_RELATIONSHIPS)
    val c = FakePipeWithSources("c", List(b), Effects.WRITES_NODES)
    val d = FakePipeWithSources("d", List(c), Effects.NONE)

    val result = addEagernessIfNecessary.apply(d)

    result should equal(d)
  }

  test("NONE -> READS ALL -> WRITE NODES -> NONE needs eagerness") {
    val a = FakePipeWithSources("a", List.empty, Effects.NONE)
    val b = FakePipeWithSources("b", List(a), Effects.READS_ENTITIES)
    val c = FakePipeWithSources("c", List(b), Effects.WRITES_NODES)
    val d = FakePipeWithSources("d", List(c), Effects.NONE)

    val eagerB = EagerPipe(b)
    val eagerC = FakePipeWithSources("c", List(eagerB), Effects.WRITES_NODES)
    val eagerD = FakePipeWithSources("d", List(eagerC), Effects.NONE)

    val result = addEagernessIfNecessary.apply(d)

    result should equal(eagerD)
  }

  test("mixed bag of pipes that need eager with pipes that do not") {
    val leaf1 = FakePipeWithSources("l1", List.empty, Effects.READS_NODES)
    val leaf2 = FakePipeWithSources("l2", List.empty, Effects.READS_RELATIONSHIPS)
    val parent = FakePipeWithSources("parent", List(leaf1, leaf2), Effects.WRITES_NODES)

    val expected = FakePipeWithSources("parent", List(EagerPipe(leaf1), leaf2), Effects.WRITES_NODES)

    val result = addEagernessIfNecessary.apply(parent)
    result should equal(expected)
  }

  test("pipe with no source is returned as is") {
    val pipe = mock[Pipe]
    when(pipe.sources).thenReturn(Seq.empty)
    addEagernessIfNecessary.apply(pipe)

    verify(pipe).dup(List.empty)
  }

  case class FakePipeWithSources(name:String, sources: List[Pipe], override val localEffects: Effects = Effects.ALL) extends Pipe {
    def monitor: PipeMonitor = mock[PipeMonitor]

    override def effects: Effects = sources.foldLeft(localEffects)(_ | _.effects)

    def planDescription: PlanDescription = ???

    def symbols: SymbolTable = ???

    protected def internalCreateResults(state: QueryState): Iterator[ExecutionContext] = ???

    def dup(sources: List[Pipe]): Pipe = copy(sources = sources)

    def exists(pred: (Pipe) => Boolean): Boolean = ???

    override def toString: String = s"Fake($name - ${sources.map(_.toString).mkString(",")} | $effects)"
  }

  case class EffectsAssertion(from: Effects, to: Effects) {
    def doesNotIntroduceEagerness() {
      constructedPipe.sources.head should not be an[EagerPipe]
    }

    def doesIntroduceEagerness() {
      constructedPipe.sources.head shouldBe an[EagerPipe]
    }

    def constructedPipe: Pipe = {
      val source = mock[Pipe]
      when(source.dup(any())).thenReturn(source)
      when(source.effects).thenReturn(from)
      when(source.sources).thenReturn(Seq.empty)

      val pipe = mock[Pipe]
      when(pipe.effects).thenReturn(to)
      when(pipe.sources).thenReturn(Seq(source))
      when(pipe.dup(any())).thenAnswer(new Answer[Pipe] {
        def answer(invocation: InvocationOnMock): Pipe = {
          val sources = invocation.getArguments.head.asInstanceOf[List[Pipe]]
          FakePipeWithSources("?", sources)
        }
      })

      addEagernessIfNecessary.apply(pipe)
    }

  }

  case class ToObject(fromEffects: Effects) {
    def to(toEffects: Effects): EffectsAssertion = EffectsAssertion(fromEffects, toEffects)
  }

  private def testThatGoingFrom(in: Effects): ToObject = ToObject(in)
}
