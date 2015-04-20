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
package org.neo4j.cypher.internal.compiler.v2_2.executionplan

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.commons.CypherFunSuite
import org.neo4j.cypher.internal.compiler.v2_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_2.pipes._
import org.neo4j.cypher.internal.compiler.v2_2.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v2_2.symbols.SymbolTable

class AddEagernessIfNecessaryTest extends CypherFunSuite {
  implicit val monitor = mock[PipeMonitor]

  test("NONE -> NONE need no eagerness") {
    testThatGoingFrom(Effects())
    .to(Effects())
    .doesNotIntroduceEagerness()
  }

  test("NONE -> READS need no eagerness") {
    testThatGoingFrom(Effects())
    .to(AllReadEffects)
    .doesNotIntroduceEagerness()
  }

  test("WRITES -> NONE need no eagerness") {
    testThatGoingFrom(Effects(WritesNodes))
    .to(Effects())
    .doesNotIntroduceEagerness()
  }

  test("WRITES -> WRITES need no eagerness") {
    testThatGoingFrom(AllWriteEffects)
    .to(AllWriteEffects)
    .doesNotIntroduceEagerness()
  }

  test("READS -> WRITES needs eagerness") {
    testThatGoingFrom(AllReadEffects)
    .to(AllWriteEffects)
    .doesIntroduceEagerness()
  }

  test("WRITES -> READS needs no eagerness") {
    testThatGoingFrom(AllWriteEffects)
    .to(AllReadEffects)
    .doesNotIntroduceEagerness()
  }

  test("READS -> READS needs no eagerness") {
    testThatGoingFrom(AllReadEffects)
    .to(AllReadEffects)
    .doesNotIntroduceEagerness()
  }

  test("READS NODES -> WRITES RELS does not need eagerness") {
    testThatGoingFrom(Effects(ReadsNodes))
    .to(Effects(WritesRelationships))
    .doesNotIntroduceEagerness()
  }

  test("READS ALL -> WRITES RELS needs eagerness") {
    testThatGoingFrom(AllReadEffects)
    .to(Effects(WritesRelationships))
    .doesIntroduceEagerness()
  }

  test("READS RELS -> WRITES NODES needs not eagerness") {
    testThatGoingFrom(Effects(ReadsRelationships))
    .to(Effects(WritesNodes))
    .doesNotIntroduceEagerness()
  }

  test("READS PROP -> WRITES PROP needs eagerness"){
    testThatGoingFrom(Effects(ReadsNodeProperty("foo")))
      .to(Effects(WritesNodeProperty("foo")))
      .doesIntroduceEagerness()
  }

  test("WRITES PROP -> READS PROP does not need eagerness") {
    testThatGoingFrom(Effects(WritesNodeProperty("foo")))
      .to(Effects(ReadsNodeProperty("foo")))
      .doesNotIntroduceEagerness()
  }

  test("READS ALL PROPS -> WRITES PROP needs eagerness") {
    testThatGoingFrom(Effects(ReadsAnyNodeProperty))
      .to(Effects(WritesNodeProperty("bar")))
      .doesIntroduceEagerness()
  }

  test("WRITES ALL PROPS -> READS PROP does not need eagerness") {
    testThatGoingFrom(Effects(WritesAnyNodeProperty))
      .to(Effects(ReadsNodeProperty("foo")))
      .doesNotIntroduceEagerness()
  }

  test("READS ALL PROPS -> WRITES ALL PROPS needs eagerness") {
    testThatGoingFrom(Effects(ReadsAnyNodeProperty))
      .to(Effects(WritesAnyNodeProperty))
      .doesIntroduceEagerness()
  }

  test("WRITES ALL PROPS -> READS ALL PROPS does not need eagerness") {
    testThatGoingFrom(Effects(WritesAnyNodeProperty))
      .to(Effects(ReadsAnyNodeProperty))
      .doesNotIntroduceEagerness()
  }

  test("READS LABEL -> WRITES LABEL needs eagerness") {
    testThatGoingFrom(Effects(ReadsLabel("foo")))
      .to(Effects(WritesLabel("foo")))
      .doesIntroduceEagerness()
  }

  test("WRITES LABEL -> READS LABEL does not need eagerness") {
    testThatGoingFrom(Effects(WritesLabel("foo")))
      .to(Effects(ReadsLabel("foo")))
      .doesNotIntroduceEagerness()
  }

  test("READS ALL LABELS -> WRITES LABEL needs eagerness") {
    testThatGoingFrom(Effects(ReadsAnyLabel))
      .to(Effects(WritesLabel("foo")))
      .doesIntroduceEagerness()
  }

  test("WRITES ALL LABELS -> READS LABEL does not need eagerness") {
    testThatGoingFrom(Effects(WritesAnyLabel))
      .to(Effects(ReadsLabel("foo")))
      .doesNotIntroduceEagerness()
  }

  test("NONE -> READS RELS -> WRITE NODES -> NONE needs not eagerness") {
    val a = FakePipeWithSources("a", List.empty, Effects())
    val b = FakePipeWithSources("b", List(a), Effects(ReadsRelationships))
    val c = FakePipeWithSources("c", List(b), Effects(WritesNodes))
    val d = FakePipeWithSources("d", List(c), Effects())

    val result = addEagernessIfNecessary.apply(d)

    result should equal(d)
  }

  test("ReadsLabel with matching label should introduce eagerness") {
    testThatGoingFrom(Effects(ReadsLabel("Folder"), WritesRelationships, WritesNodes))
      .to(Effects(WritesLabel("Folder"), WritesNodes, WritesRelationships))
      .doesIntroduceEagerness()
  }

  test("NONE -> READS ALL -> WRITE NODES -> NONE needs eagerness") {
    val a = FakePipeWithSources("a", List.empty, Effects())
    val b = FakePipeWithSources("b", List(a), AllReadEffects)
    val c = FakePipeWithSources("c", List(b), Effects(WritesNodes))
    val d = FakePipeWithSources("d", List(c), Effects())

    val eagerB = EagerPipe(b)
    val eagerC = FakePipeWithSources("c", List(eagerB), Effects(WritesNodes))
    val eagerD = FakePipeWithSources("d", List(eagerC), Effects())

    val result = addEagernessIfNecessary.apply(d)

    result should equal(eagerD)
  }

  test("mixed bag of pipes that need eager with pipes that do not") {
    val leaf1 = FakePipeWithSources("l1", List.empty, Effects(ReadsNodes))
    val leaf2 = FakePipeWithSources("l2", List.empty, Effects(ReadsRelationships))
    val parent = FakePipeWithSources("parent", List(leaf1, leaf2), Effects(WritesNodes))

    val expected = FakePipeWithSources("parent", List(EagerPipe(leaf1), leaf2), Effects(WritesNodes))

    val result = addEagernessIfNecessary.apply(parent)
    result should equal(expected)
  }

  test("pipe with no source is returned as is") {
    val pipe = mock[Pipe]
    when(pipe.sources).thenReturn(Seq.empty)
    addEagernessIfNecessary.apply(pipe)

    verify(pipe).dup(List.empty)
  }

  case class FakePipeWithSources(name:String, sources: List[Pipe], override val localEffects: Effects = AllEffects) extends Pipe {
    def monitor: PipeMonitor = mock[PipeMonitor]

    override def effects: Effects = sources.foldLeft(localEffects)(_ | _.effects)

    def planDescription: InternalPlanDescription = ???

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
      when(pipe.localEffects).thenReturn(to)
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
