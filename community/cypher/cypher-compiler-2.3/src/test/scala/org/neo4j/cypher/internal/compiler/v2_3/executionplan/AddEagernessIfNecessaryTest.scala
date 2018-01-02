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
package org.neo4j.cypher.internal.compiler.v2_3.executionplan

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v2_3.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_3.pipes._
import org.neo4j.cypher.internal.compiler.v2_3.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v2_3.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v2_3.test_helpers.CypherFunSuite

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
    testThatGoingFrom(Effects(WritesAnyNode))
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
    testThatGoingFrom(Effects(ReadsAllNodes))
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
    .to(Effects(WritesAnyNode))
    .doesNotIntroduceEagerness()
  }

  test("READS PROP -> WRITES PROP needs eagerness"){
    testThatGoingFrom(Effects(ReadsGivenNodeProperty("foo")))
      .to(Effects(WritesGivenNodeProperty("foo")))
      .doesIntroduceEagerness()
  }

  test("WRITES PROP -> READS PROP does not need eagerness") {
    testThatGoingFrom(Effects(WritesGivenNodeProperty("foo")))
      .to(Effects(ReadsGivenNodeProperty("foo")))
      .doesNotIntroduceEagerness()
  }

  test("READS ALL PROPS -> WRITES PROP needs eagerness") {
    testThatGoingFrom(Effects(ReadsAnyNodeProperty))
      .to(Effects(WritesGivenNodeProperty("bar")))
      .doesIntroduceEagerness()
  }

  test("WRITES ALL PROPS -> READS PROP does not need eagerness") {
    testThatGoingFrom(Effects(WritesAnyNodeProperty))
      .to(Effects(ReadsGivenNodeProperty("foo")))
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
    testThatGoingFrom(Effects(ReadsNodesWithLabels("foo")))
      .to(Effects(WritesNodesWithLabels("foo")))
      .doesIntroduceEagerness()
  }

  test("WRITES LABEL -> READS LABEL does not need eagerness") {
    testThatGoingFrom(Effects(WritesNodesWithLabels("foo")))
      .to(Effects(ReadsNodesWithLabels("foo")))
      .doesNotIntroduceEagerness()
  }

  test("READS ALL NODES -> WRITES LABEL does not need eagerness") {
    testThatGoingFrom(Effects(ReadsAllNodes))
      .to(Effects(WritesNodesWithLabels("foo")))
      .doesNotIntroduceEagerness()
  }

  test("WRITES ANY NODE -> READS LABEL does not need eagerness") {
    testThatGoingFrom(Effects(WritesAnyNode))
      .to(Effects(ReadsNodesWithLabels("foo")))
      .doesNotIntroduceEagerness()
  }

  test("NONE -> READS RELS -> WRITE NODES -> NONE needs not eagerness") {
    val a = FakePipeWithSources("a", List.empty, Effects())
    val b = FakePipeWithSources("b", List(a), Effects(ReadsRelationships))
    val c = FakePipeWithSources("c", List(b), Effects(WritesAnyNode))
    val d = FakePipeWithSources("d", List(c), Effects())

    val result = addEagernessIfNecessary.apply(d)

    result should equal(d)
  }

  test("ReadsLabel with matching label should introduce eagerness") {
    testThatGoingFrom(Effects(ReadsNodesWithLabels("Folder"), WritesRelationships, WritesAnyNode))
      .to(Effects(WritesNodesWithLabels("Folder"), WritesAnyNode, WritesRelationships))
      .doesIntroduceEagerness()
  }

  test("NONE -> READS ALL -> WRITE NODES -> NONE needs eagerness") {
    val a = FakePipeWithSources("a", List.empty, Effects())
    val b = FakePipeWithSources("b", List(a), AllReadEffects)
    val c = FakePipeWithSources("c", List(b), Effects(WritesAnyNode))
    val d = FakePipeWithSources("d", List(c), Effects())

    val eagerB = EagerPipe(b)
    val eagerC = FakePipeWithSources("c", List(eagerB), Effects(WritesAnyNode))
    val eagerD = FakePipeWithSources("d", List(eagerC), Effects())

    val result = addEagernessIfNecessary.apply(d)

    result should equal(eagerD)
  }

  test("mixed bag of pipes that need eager with pipes that do not") {
    val leaf1 = FakePipeWithSources("l1", List.empty, Effects(ReadsAllNodes))
    val leaf2 = FakePipeWithSources("l2", List.empty, Effects(ReadsRelationships))
    val parent = FakePipeWithSources("parent", List(leaf1, leaf2), Effects(WritesAnyNode))

    val expected = FakePipeWithSources("parent", List(EagerPipe(leaf1), leaf2), Effects(WritesAnyNode))

    val result = addEagernessIfNecessary.apply(parent)
    result should equal(expected)
  }

  test("pipe with no source is returned as is") {
    val pipe = mock[Pipe]
    when(pipe.sources).thenReturn(Seq.empty)
    addEagernessIfNecessary.apply(pipe)

    verify(pipe).dup(List.empty)
  }

  test("Reading and writing nodes with different labels does not introduce eagerness") {
    testThatGoingFrom(Effects(ReadsNodesWithLabels("a", "b", "c")))
      .to(Effects(WritesNodesWithLabels("d", "e", "f")))
      .doesNotIntroduceEagerness()
  }

  test("Reading any node and writing to nodes with specific labels does not introduce eagerness") {
    testThatGoingFrom(Effects(ReadsAllNodes))
      .to(Effects(WritesNodesWithLabels("d", "e", "f")))
      .doesNotIntroduceEagerness()
  }

  test("Reading node with specific label and writing to any nodes does introduce eagerness") {
    testThatGoingFrom(Effects(ReadsNodesWithLabels("a", "b")))
      .to(Effects(WritesAnyNode))
      .doesIntroduceEagerness()
  }

  test("Reading and writing nodes to nodes with overlapping labels does introduce eagerness") {
    testThatGoingFrom(Effects(ReadsNodesWithLabels("a", "b", "c")))
      .to(Effects(WritesNodesWithLabels("d", "b", "f")))
      .doesIntroduceEagerness()
  }

  test("Writing nodes with one set of labels + read and write  with different labels does not introduce eagerness") {
    testThatGoingFrom(Effects(WritesNodesWithLabels("a", "b", "c")))
      .to(Effects(WritesNodesWithLabels("d", "e", "f")) | Effects(ReadsNodesWithLabels("f")))
      .doesNotIntroduceEagerness()
  }

  test("Writing nodes with one set of labels + write and read same label does introduce eagerness") {
    testThatGoingFrom(Effects(WritesNodesWithLabels("a", "b", "c")))
      .to(Effects(WritesNodesWithLabels("d", "e", "f")) | Effects(ReadsNodesWithLabels("b")))
      .doesIntroduceEagerness()
  }

  test("Writing to any nodes  + write and read some label does introduce eagerness") {
    testThatGoingFrom(Effects(WritesAnyNode))
      .to(Effects(WritesNodesWithLabels("d", "e", "f")) | Effects(ReadsNodesWithLabels("b")))
      .doesIntroduceEagerness()
  }

  test("Writing to nodes with some labels + write and read to any labeled node") {
    testThatGoingFrom(Effects(WritesNodesWithLabels("a")))
      .to(Effects(WritesNodesWithLabels("d", "e", "f")) | Effects(ReadsAllNodes))
      .doesIntroduceEagerness()
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
