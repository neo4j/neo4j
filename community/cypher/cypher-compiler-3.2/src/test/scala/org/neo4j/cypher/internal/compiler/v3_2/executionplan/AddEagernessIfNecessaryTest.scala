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
package org.neo4j.cypher.internal.compiler.v3_2.executionplan

import org.mockito.Matchers._
import org.mockito.Mockito._
import org.mockito.invocation.InvocationOnMock
import org.mockito.stubbing.Answer
import org.neo4j.cypher.internal.compiler.v3_2.ExecutionContext
import org.neo4j.cypher.internal.compiler.v3_2.pipes._
import org.neo4j.cypher.internal.compiler.v3_2.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.compiler.v3_2.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite

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

  test("CREATES -> NONE need no eagerness") {
    testThatGoingFrom(Effects(CreatesAnyNode))
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

  test("READS -> READS needs no eagerness") {
    testThatGoingFrom(AllReadEffects)
    .to(AllReadEffects)
    .doesNotIntroduceEagerness()
  }

  test("READS NODES -> CREATES RELS does not need eagerness") {
    testThatGoingFrom(Effects(ReadsAllNodes))
    .to(Effects(CreatesRelationship("Foo")))
    .doesNotIntroduceEagerness()
  }

  test("READS RELS -> WRITES NODES needs not eagerness") {
    testThatGoingFrom(Effects(ReadsAllRelationships))
    .to(Effects(CreatesAnyNode))
    .doesNotIntroduceEagerness()
  }

  test("READS LABEL -> WRITES LABEL needs eagerness") {
    testThatGoingFrom(Effects(ReadsNodesWithLabels("foo")))
      .to(Effects(SetLabel("foo")))
      .doesIntroduceEagerness()
  }

  test("READS LABEL in a leaf -> WRITES LABEL does not need eagerness") {
    testThatGoingFrom(Effects(ReadsNodesWithLabels("foo")).asLeafEffects)
      .to(Effects(SetLabel("foo")))
      .doesNotIntroduceEagerness()
  }

  test("WRITES LABEL in leaf -> READS LABEL does not need eagerness") {
    testThatGoingFrom(Effects(CreatesNodesWithLabels("foo")).asLeafEffects)
      .to(Effects(ReadsNodesWithLabels("foo")))
      .doesNotIntroduceEagerness()
  }

  test("WRITES LABEL in non-leaf -> READS LABEL needs eagerness") {
    testThatGoingFrom(Effects(CreatesNodesWithLabels("foo")))
      .to(Effects(ReadsNodesWithLabels("foo")))
      .doesIntroduceEagerness()
  }

  test("READS ALL NODES in leaf -> WRITES LABEL does not need eagerness") {
    testThatGoingFrom(Effects(ReadsAllNodes).asLeafEffects)
      .to(Effects(CreatesNodesWithLabels("foo")))
      .doesNotIntroduceEagerness()
  }

  test("READS ALL NODES in leaf -> WRITES LABEL needs eagerness") {
    testThatGoingFrom(Effects(ReadsAllNodes))
      .to(Effects(CreatesNodesWithLabels("foo")))
      .doesIntroduceEagerness()
  }

  test("ReadsLabel in leaf with matching label should not introduce eagerness") {
    testThatGoingFrom(Effects(ReadsNodesWithLabels("Folder")).asLeafEffects)
      .to(Effects(CreatesNodesWithLabels("Folder")))
      .doesNotIntroduceEagerness()
  }

  test("ReadsLabel in leaf with matching label should introduce eagerness") {
    testThatGoingFrom(Effects(ReadsNodesWithLabels("Folder")))
      .to(Effects(CreatesNodesWithLabels("Folder")))
      .doesIntroduceEagerness()
  }

  test("WRITES ANY NODE -> READS LABEL does not need eagerness") {
    testThatGoingFrom(Effects(CreatesAnyNode))
      .to(Effects(ReadsNodesWithLabels("foo")))
      .doesNotIntroduceEagerness()
  }

  test("NONE -> READS RELS -> WRITE NODES -> NONE needs no eagerness") {
    val a = FakePipeWithSources("a", List.empty, Effects())
    val b = FakePipeWithSources("b", List(a), Effects(ReadsAllRelationships))
    val c = FakePipeWithSources("c", List(b), Effects(CreatesAnyNode))
    val d = FakePipeWithSources("d", List(c), Effects())

    val result = addEagernessIfNecessary.apply(d)

    result should equal(d)
  }

  test("NONE -> READS ALL -> WRITE NODES -> NONE does not need eagerness") {
    val a = FakePipeWithSources("a", List.empty, Effects())
    val b = FakePipeWithSources("b", List(a), AllReadEffects)
    val c = FakePipeWithSources("c", List(b), Effects(CreatesAnyNode))
    val d = FakePipeWithSources("d", List(c), Effects())

    val eagerB = EagerPipe(b)()
    val eagerC = FakePipeWithSources("c", List(eagerB), Effects(CreatesAnyNode))
    val eagerD = FakePipeWithSources("d", List(eagerC), Effects())

    val result = addEagernessIfNecessary.apply(d)

    result should equal(eagerD)
  }

  test("NONE -> READS ALL -> READS ALL -> WRITE NODES -> NONE needs eagerness") {
    val a = FakePipeWithSources("a", List.empty, Effects())
    val b = FakePipeWithSources("b", List(a), AllReadEffects)
    val c = FakePipeWithSources("c", List(b), AllReadEffects)
    val d = FakePipeWithSources("d", List(c), Effects(CreatesAnyNode))
    val e = FakePipeWithSources("e", List(d), Effects())

    val eagerC = EagerPipe(c)()
    val eagerD = FakePipeWithSources("d", List(eagerC), Effects(CreatesAnyNode))
    val eagerE = FakePipeWithSources("e", List(eagerD), Effects())

    val result = addEagernessIfNecessary.apply(e)

    result should equal(eagerE)
  }

  test("pipe with no source is returned as is") {
    val pipe = mock[Pipe]
    when(pipe.sources).thenReturn(Seq.empty)
    addEagernessIfNecessary.apply(pipe)

    verify(pipe).dup(List.empty)
  }

  test("Reading and writing nodes with different labels does not introduce eagerness") {
    testThatGoingFrom(Effects(ReadsNodesWithLabels("a", "b", "c")))
      .to(Effects(CreatesNodesWithLabels("d", "e", "f")))
      .doesNotIntroduceEagerness()
  }

  test("Reading any node from leaf and writing to nodes with specific labels does not introduce eagerness") {
    testThatGoingFrom(Effects(ReadsAllNodes).asLeafEffects)
      .to(Effects(CreatesNodesWithLabels("d", "e", "f")))
      .doesNotIntroduceEagerness()
  }
  test("Reading any node from non-leaf and writing to nodes with specific labels does not introduce eagerness") {
    testThatGoingFrom(Effects(ReadsAllNodes))
      .to(Effects(CreatesNodesWithLabels("d", "e", "f")))
      .doesIntroduceEagerness()
  }

  test("Reading node with specific label and writing to any nodes does not introduce eagerness") {
    testThatGoingFrom(Effects(ReadsNodesWithLabels("a", "b")))
      .to(Effects(CreatesAnyNode))
      .doesNotIntroduceEagerness()
  }

  test("Reading and writing nodes to nodes with overlapping labels does introduce eagerness") {
    testThatGoingFrom(Effects(ReadsNodesWithLabels("a", "b", "c")))
      .to(Effects(CreatesNodesWithLabels("d", "b", "f")))
      .doesIntroduceEagerness()
  }

  test("Writing nodes with one set of labels + read and write  with different labels does not introduce eagerness") {
    testThatGoingFrom(Effects(CreatesNodesWithLabels("a", "b", "c")))
      .to(Effects(CreatesNodesWithLabels("d", "e", "f"), ReadsNodesWithLabels("f")))
      .doesNotIntroduceEagerness()
  }

  test("Writing nodes with one set of labels + write and read same label does introduce eagerness") {
    testThatGoingFrom(Effects(CreatesNodesWithLabels("a", "b", "c")))
      .to(Effects(CreatesNodesWithLabels("d", "e", "f"), ReadsNodesWithLabels("b")))
      .doesIntroduceEagerness()
  }

  test("Writing to any nodes  + write and read some label does not introduce eagerness") {
    testThatGoingFrom(Effects(CreatesAnyNode))
      .to(Effects(CreatesNodesWithLabels("d", "e", "f"), ReadsNodesWithLabels("b")))
      .doesNotIntroduceEagerness()
  }

  test("Writing to nodes with some labels + write and read to any labeled node") {
    testThatGoingFrom(Effects(CreatesNodesWithLabels("a")))
      .to(Effects(CreatesNodesWithLabels("d", "e", "f"), ReadsAllNodes))
      .doesIntroduceEagerness()
  }


  case class FakePipeWithSources(name:String, sources: List[Pipe], override val localEffects: Effects = AllEffects) extends Pipe {
    def monitor: PipeMonitor = mock[PipeMonitor]

    override def effects: Effects = sources.foldLeft(localEffects)(_ ++ _.effects)

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
