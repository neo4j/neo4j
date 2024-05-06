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
package org.neo4j.cypher

import org.neo4j.cypher.NFAToProductGraphCursorIT.NFABuilderWrapper
import org.neo4j.cypher.NFAToProductGraphCursorIT.NFAStateWrapper
import org.neo4j.cypher.internal.ast.semantics.SemanticTable
import org.neo4j.cypher.internal.expressions.False
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.SemanticDirection
import org.neo4j.cypher.internal.expressions.SemanticDirection.BOTH
import org.neo4j.cypher.internal.expressions.SemanticDirection.INCOMING
import org.neo4j.cypher.internal.expressions.SemanticDirection.OUTGOING
import org.neo4j.cypher.internal.expressions.True
import org.neo4j.cypher.internal.expressions.UnPositionedVariable.varFor
import org.neo4j.cypher.internal.kernel.api.helpers.ProductGraph
import org.neo4j.cypher.internal.kernel.api.helpers.ProductGraph.equalProductGraph
import org.neo4j.cypher.internal.logical.plans.Expand.VariablePredicate
import org.neo4j.cypher.internal.logical.plans.NFA
import org.neo4j.cypher.internal.logical.plans.NFABuilder
import org.neo4j.cypher.internal.physicalplanning.SlotConfiguration
import org.neo4j.cypher.internal.planner.spi.ReadTokenContext
import org.neo4j.cypher.internal.runtime.CypherRuntimeConfiguration
import org.neo4j.cypher.internal.runtime.SelectivityTrackerRegistrator
import org.neo4j.cypher.internal.runtime.ast.TemporaryExpressionVariable
import org.neo4j.cypher.internal.runtime.interpreted.QueryStateHelper
import org.neo4j.cypher.internal.runtime.interpreted.commands.CommandNFA
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.CommunityExpressionConverter
import org.neo4j.cypher.internal.runtime.interpreted.commands.convert.ExpressionConverters
import org.neo4j.cypher.internal.runtime.slotted.SlottedRow
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.attribution.Id
import org.neo4j.cypher.internal.util.symbols
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Relationship
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.ProductGraphTraversalCursor
import org.neo4j.memory.EmptyMemoryTracker
import org.neo4j.values.AnyValue

import scala.language.implicitConversions

class NFAToProductGraphCursorIT extends ExecutionEngineFunSuite {

  test("should traverse two hops") {
    // (start)-[r1:R1]->(a1)-[r2:R2]->(f1)
    val start = createNode()
    val a1 = createNode()
    val a2 = createNode()
    val r1 = relate(start, a1, "R1")
    val r2 = relate(a1, a2, "R2")

    val nfa = new NFABuilderWrapper()
    val s1 = nfa.newState()
    val s2 = nfa.newState(isFinalState = true)
    val s0 = nfa.startState
    s0.relExpansion(s1, types = Seq("R1"))
    s1.relExpansion(s2, types = Seq("R2"))

    val expected = new ProductGraph()
      .addNode(start, s0)
      .addNode(a1, s1)
      .addNode(a2, s2)
      .addRelationship(start, s0, r1, a1, s1)
      .addRelationship(a1, s1, r2, a2, s2)

    run(start, nfa) should equalProductGraph(expected)
  }

  test("should filter on type") {
    // (start)-[r1:R1]->(a1)-[r2:R2]->(f1)
    val start = createNode()
    val a1 = createNode()
    val a2 = createNode()
    val r1 = relate(start, a1, "R1")
    relate(a1, a2, "R2")

    val nfa = new NFABuilderWrapper()
    val s1 = nfa.newState()
    val s2 = nfa.newState(isFinalState = true)
    val s0 = nfa.startState

    s0.relExpansion(s1, types = Seq("R1"))
    s1.relExpansion(s2, types = Seq("R1"))

    val expected = new ProductGraph()
      .addNode(start, s0)
      .addNode(a1, s1)
      .addRelationship(start, s0, r1, a1, s1)

    run(start, nfa) should equalProductGraph(expected)
  }

  test("should filter on direction") {
    // (start)-[r1:R1]->(a1)-[r2:R2]->(f1)
    val start = createNode()
    val a1 = createNode()
    val a2 = createNode()
    val r1 = relate(start, a1, "R1")
    relate(a1, a2, "R2")

    val nfa = new NFABuilderWrapper
    val s1 = nfa.newState()
    val s2 = nfa.newState(isFinalState = true)
    val s0 = nfa.startState

    s0.relExpansion(s1, types = Seq("R1"))
    s1.relExpansion(s2, types = Seq("R2"), dir = SemanticDirection.INCOMING)

    val expected = new ProductGraph()
      .addNode(start, s0)
      .addNode(a1, s1)
      .addRelationship(start, s0, r1, a1, s1)

    run(start, nfa) should equalProductGraph(expected)
  }

  test("should filter on rel predicate") {
    // (start)-[r1:R1]->(a1)-[r2:R2]->(f1)
    val start = createNode()
    val a1 = createNode()
    val a2 = createNode()
    val r1 = relate(start, a1, "R1")
    relate(a1, a2, "R2")

    val nfa = new NFABuilderWrapper()
    val s1 = nfa.newState()
    val s2 = nfa.newState(isFinalState = true)
    val s0 = nfa.startState

    s0.relExpansion(s1, types = Seq("R1"))
    s1.relExpansion(s2, predicate = alwaysFalse, types = Seq("R2"))

    val expected = new ProductGraph()
      .addNode(start, s0)
      .addNode(a1, s1)
      .addRelationship(start, s0, r1, a1, s1)

    run(start, nfa) should equalProductGraph(expected)
  }

  test("should filter on node predicate") {
    // (start)-[r1:R1]->(a1)-[r2:R2]->(f1)
    val start = createNode()
    val a1 = createNode()
    val a2 = createNode()
    val r1 = relate(start, a1, "R1")
    relate(a1, a2, "R2")

    val nfa = new NFABuilderWrapper()
    val s1 = nfa.newState()
    val s2 = nfa.newState(predicate = alwaysFalse, isFinalState = true)
    val s0 = nfa.startState

    s0.relExpansion(s1, types = Seq("R1"))
    s1.relExpansion(s2, predicate = alwaysTrue, types = Seq("R2"))

    val expected = new ProductGraph()
      .addNode(start, s0)
      .addNode(a1, s1)
      .addRelationship(start, s0, r1, a1, s1)

    run(start, nfa) should equalProductGraph(expected)
  }

  test("should handle multiple types in multiple directions") {
    //                              -[o1:O1]->
    //       _________<________     -[o2:O3]->
    //     /                   \    -[o3:O2]->
    // [l{1,2,3}:L{1,2,3}]   (start)            (a1)
    //     \_______>___________/    <-[i1:I1]-
    //                              <-[i2:I3]-
    //                              <-[i3:I2]-

    val start = createNode()
    val a1 = createNode()

    // Outgoing
    val o1 = relate(start, a1, "O1")
    val _ = relate(start, a1, "O2")
    val o3 = relate(start, a1, "O3")

    // Incoming
    val i1 = relate(a1, start, "I1")
    val i2 = relate(a1, start, "I2")
    val i3 = relate(a1, start, "I3")

    // Loop
    val l1 = relate(start, start, "L1")
    val l2 = relate(start, start, "L2")
    val l3 = relate(start, start, "L3")

    val nfa = new NFABuilderWrapper()
    val s1 = nfa.newState()
    val s2 = nfa.newState()
    val s3 = nfa.newState()
    val s4 = nfa.newState(isFinalState = true)
    val s0 = nfa.startState

    s0.relExpansion(s1, predicate = alwaysTrue, types = Seq("O1"), dir = OUTGOING)
    s0.relExpansion(s2, predicate = alwaysTrue, types = Seq("I1", "I2"), dir = INCOMING)
    s0.relExpansion(s3, predicate = alwaysTrue, types = Seq("L1", "I3"), dir = BOTH)

    s3.relExpansion(s4, predicate = alwaysTrue, types = Seq("O3", "L2"), dir = INCOMING)
    s4.relExpansion(s0, predicate = alwaysTrue, types = Seq("L3"), dir = INCOMING)

    // then
    val expected = new ProductGraph()
      .addNode(start, s0)
      .addNode(a1, s1)
      .addNode(a1, s2)
      .addNode(a1, s3)
      .addNode(start, s3)
      .addNode(start, s4)
      .addRelationship(start, s0, o1, a1, s1)
      .addRelationship(start, s0, i1, a1, s2)
      .addRelationship(start, s0, i2, a1, s2)
      .addRelationship(start, s0, i3, a1, s3)
      .addRelationship(start, s0, l1, start, s3)
      .addRelationship(a1, s3, o3, start, s4)
      .addRelationship(start, s3, l2, start, s4)
      .addRelationship(start, s4, l3, start, s0)

    run(start, nfa) should equalProductGraph(expected)
  }

  test("node juxtaposition should filter on node predicate") {
    val start = createNode()

    val nfa = new NFABuilderWrapper()
    val s1 = nfa.newState(predicate = alwaysTrue)
    val s2 = nfa.newState(predicate = alwaysFalse, isFinalState = true)
    val s0 = nfa.startState

    s0.nodeJuxtaposition(s1)
    s0.nodeJuxtaposition(s2)
    s1.nodeJuxtaposition(s2)

    val expected =
      new ProductGraph()
        .addNode(start, s0)
        .addNode(start, s1)
        .addJuxtaposition(start, s0, s1)

    run(start, nfa) should equalProductGraph(expected)
  }

  implicit private def nodeToId(node: Node): Long = node.getId
  implicit private def relToId(rel: Relationship): Long = rel.getId
  implicit private def stateToId(state: NFAStateWrapper): Int = state.state.id

  private def alwaysTrue = Some(VariablePredicate(TemporaryExpressionVariable(0, "tmp"), True()(InputPosition.NONE)))
  private def alwaysFalse = Some(VariablePredicate(TemporaryExpressionVariable(0, "tmp"), False()(InputPosition.NONE)))

  private def run(start: Node, nfa: NFABuilderWrapper): ProductGraph = {
    withTx { tx =>
      val cursorFactory = tx.kernelTransaction().cursors()
      val cursorContext = tx.kernelTransaction().cursorContext()
      val read = tx.kernelTransaction().dataRead()
      val nodeCursor = cursorFactory.allocateNodeCursor(cursorContext)
      val relCursor = cursorFactory.allocateRelationshipTraversalCursor(cursorContext)
      val pgCursor = new ProductGraphTraversalCursor(read, nodeCursor, relCursor, EmptyMemoryTracker.INSTANCE)

      try {
        QueryStateHelper.withQueryState(
          graph,
          tx,
          f = queryState => {
            val slots = SlotConfiguration.empty
              .newLong("start", nullable = true, symbols.CTNode)
            val context = SlottedRow(slots)
            context.setLongAt(slots.getLongOffsetFor("start"), start.getId)

            val converters =
              new ExpressionConverters(
                None,
                CommunityExpressionConverter(
                  ReadTokenContext.EMPTY,
                  new AnonymousVariableNameGenerator(),
                  new SelectivityTrackerRegistrator,
                  CypherRuntimeConfiguration.defaultConfiguration,
                  SemanticTable()
                )
              )

            implicit val semanticTable: SemanticTable = SemanticTable()

            val (startState, _) = CommandNFA
              .fromLogicalNFA(nfa.builder.build(), x => converters.toCommandPredicate(Id.INVALID_ID, x.predicate))
              .compile(context, queryState)

            ProductGraph.fromCursor(start.getId, startState, pgCursor)
          },
          expressionVariables = Array.fill[AnyValue](1)(null)
        )
      } finally {
        nodeCursor.close()
        relCursor.close()
        pgCursor.close()
      }
    }
  }
}

object NFAToProductGraphCursorIT {

  private class NFABuilderWrapper() {
    private var states = 0

    def nextName(name: String) = {
      states += 1
      varFor(name + states)
    }
    val builder = new NFABuilder(varFor("start"))

    def startState = wrapState(builder.getStartState)

    def newState(predicate: Option[VariablePredicate] = None, isFinalState: Boolean = false) = {
      states += 1
      val state = builder.addAndGetState(nextName("s"), predicate)
      if (isFinalState) {
        builder.setFinalState(state)
      }
      wrapState(state)
    }

    private def wrapState(state: NFA.State): NFAStateWrapper =
      NFAStateWrapper(state, this)
  }

  private case class NFAStateWrapper(state: NFA.State, parent: NFABuilderWrapper) {

    def nodeJuxtaposition(other: NFAStateWrapper): Unit = {
      parent.builder.addTransition(state, NFA.NodeJuxtapositionTransition(other.state.id))
    }

    def relExpansion(
      other: NFAStateWrapper,
      predicate: Option[VariablePredicate] = None,
      types: Seq[String] = Seq.empty,
      dir: SemanticDirection = SemanticDirection.OUTGOING
    ): Unit = {
      parent.builder.addTransition(
        state,
        NFA.RelationshipExpansionTransition(
          NFA.RelationshipExpansionPredicate(
            parent.nextName("r"),
            predicate,
            types.map(RelTypeName(_)(InputPosition.NONE)),
            dir
          ),
          other.state.id
        )
      )
    }
  }
}
