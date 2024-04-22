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
package org.neo4j.cypher.internal.kernel.api.helpers

import org.neo4j.cypher.GraphDatabaseTestSupport
import org.neo4j.cypher.internal.kernel.api.helpers.ProductGraph.PGNode
import org.neo4j.cypher.internal.kernel.api.helpers.ProductGraph.PGRelationship
import org.neo4j.cypher.internal.kernel.api.helpers.ProductGraph.equalProductGraph
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.function.Predicates
import org.neo4j.graphdb.Direction
import org.neo4j.internal.kernel.api.NodeCursor
import org.neo4j.internal.kernel.api.Read
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor
import org.neo4j.internal.kernel.api.Write
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.PGStateBuilder
import org.neo4j.internal.kernel.api.helpers.traversal.productgraph.ProductGraphTraversalCursor
import org.neo4j.io.pagecache.context.CursorContext
import org.neo4j.kernel.api.StatementConstants
import org.neo4j.kernel.impl.coreapi.InternalTransaction
import org.neo4j.memory.EmptyMemoryTracker

import scala.jdk.CollectionConverters.SeqHasAsJava
import scala.language.implicitConversions

class ProductGraphTraversalCursorTest extends CypherFunSuite with GraphDatabaseTestSupport {

  test("should traverse two hops") {
    runTest { fx =>
      val R1 = fx.relType("R1")
      val R2 = fx.relType("R2")

      val start = fx.write.nodeCreate()
      val a1 = fx.write.nodeCreate()
      val a2 = fx.write.nodeCreate()

      val r1 = fx.write.relationshipCreate(start, R1, a1)
      val r2 = fx.write.relationshipCreate(a1, R2, a2)

      val builder = new PGStateBuilder
      val s0 = builder.newState(isStartState = true)
      val s1 = builder.newState()
      val s2 = builder.newState(isFinalState = true)
      s0.addRelationshipExpansion(s1, types = Array(R1))
      s1.addRelationshipExpansion(s2, types = Array(R2))
      val expected = new ProductGraph()
        .addNode(start, s0)
        .addNode(a1, s1)
        .addNode(a2, s2)
        .addRelationship(start, s0, r1, a1, s1)
        .addRelationship(a1, s1, r2, a2, s2)

      (expected, start, builder)
    }
  }

  test("should filter on type") {
    runTest { fx =>
      val R1 = fx.relType("R1")
      val R2 = fx.relType("R2")

      val start = fx.write.nodeCreate()
      val a1 = fx.write.nodeCreate()
      val a2 = fx.write.nodeCreate()

      val r1 = fx.write.relationshipCreate(start, R1, a1)
      fx.write.relationshipCreate(a1, R2, a2)

      val builder = new PGStateBuilder
      val s0 = builder.newState(isStartState = true)
      val s1 = builder.newState()
      val s2 = builder.newState(isFinalState = true)
      s0.addRelationshipExpansion(s1, types = Array(R1), direction = Direction.OUTGOING)
      // can't be traversed from s1 in this direction
      s1.addRelationshipExpansion(s2, types = Array(R1), direction = Direction.OUTGOING)

      val expected = new ProductGraph()
        .addNode(start, s0)
        .addNode(a1, s1)
        .addRelationship(start, s0, r1, a1, s1)

      (expected, start, builder)
    }
  }

  test("should filter on direction") {
    runTest { fx =>
      val R1 = fx.relType("R1")
      val R2 = fx.relType("R2")

      val start = fx.write.nodeCreate()
      val a1 = fx.write.nodeCreate()
      val a2 = fx.write.nodeCreate()

      val r1 = fx.write.relationshipCreate(start, R1, a1)
      fx.write.relationshipCreate(a1, R2, a2)

      val builder = new PGStateBuilder
      val s0 = builder.newState(isStartState = true)
      val s1 = builder.newState()
      val s2 = builder.newState(isFinalState = true)

      s0.addRelationshipExpansion(s1, direction = Direction.OUTGOING)
      // can't be traversed from s1 in this direction
      s1.addRelationshipExpansion(s2, types = Array(R2), direction = Direction.INCOMING)

      val expected = new ProductGraph()
        .addNode(start, s0)
        .addNode(a1, s1)
        .addRelationship(start, s0, r1, a1, s1)

      (expected, start, builder)
    }
  }

  test("should filter on rel predicate") {
    runTest { fx =>
      val R1 = fx.relType("R1")
      val R2 = fx.relType("R2")

      val start = fx.write.nodeCreate()
      val a1 = fx.write.nodeCreate()
      val a2 = fx.write.nodeCreate()

      val r1 = fx.write.relationshipCreate(start, R1, a1)
      fx.write.relationshipCreate(a1, R2, a2)

      val builder = new PGStateBuilder
      val s0 = builder.newState(isStartState = true)
      val s1 = builder.newState()
      val s2 = builder.newState(isFinalState = true)
      s0.addRelationshipExpansion(s1, direction = Direction.OUTGOING)
      s1.addRelationshipExpansion(
        s2,
        relPredicate = Predicates.alwaysFalse,
        types = Array(R2),
        direction = Direction.INCOMING
      )
      val expected = new ProductGraph()
        .addNode(start, s0)
        .addNode(a1, s1)
        .addRelationship(start, s0, r1, a1, s1)

      (expected, start, builder)
    }
  }

  test("should filter on node predicate") {
    runTest { fx =>
      val R1 = fx.relType("R1")
      val R2 = fx.relType("R2")

      val start = fx.write.nodeCreate()
      val a1 = fx.write.nodeCreate()
      val a2 = fx.write.nodeCreate()

      val r1 = fx.write.relationshipCreate(start, R1, a1)
      fx.write.relationshipCreate(a1, R2, a2)

      val builder = new PGStateBuilder
      val s0 = builder.newState(isStartState = true)
      val s1 = builder.newState()
      val s2 = builder.newState(isFinalState = true, predicate = Predicates.ALWAYS_FALSE_LONG)
      s0.addRelationshipExpansion(s1, direction = Direction.OUTGOING)
      s1.addRelationshipExpansion(
        s2,
        types = Array(R2),
        direction = Direction.INCOMING
      )
      val expected = new ProductGraph()
        .addNode(start, s0)
        .addNode(a1, s1)
        .addRelationship(start, s0, r1, a1, s1)

      (expected, start, builder)
    }
  }

  test("should handle multiple types in multiple directions") {
    runTest { fx =>
      val O1 = fx.relType("O1")
      val O2 = fx.relType("O2")
      val O3 = fx.relType("O3")

      val I1 = fx.relType("I1")
      val I2 = fx.relType("I2")
      val I3 = fx.relType("I3")

      val L1 = fx.relType("L1")
      val L2 = fx.relType("L2")
      val L3 = fx.relType("L3")
      val start = fx.write.nodeCreate()
      val a1 = fx.write.nodeCreate()

      // Outgoing
      val o1 = fx.write.relationshipCreate(start, O1, a1)
      fx.write.relationshipCreate(start, O2, a1)
      val o3 = fx.write.relationshipCreate(start, O3, a1)

      // Incoming
      val i1 = fx.write.relationshipCreate(a1, I1, start)
      val i2 = fx.write.relationshipCreate(a1, I2, start)
      val i3 = fx.write.relationshipCreate(a1, I3, start)

      // Loops
      val l1 = fx.write.relationshipCreate(start, L1, start)
      val l2 = fx.write.relationshipCreate(start, L2, start)
      val l3 = fx.write.relationshipCreate(start, L3, start)

      val builder = new PGStateBuilder
      val s0 = builder.newState(isStartState = true)
      val s1 = builder.newState()
      val s2 = builder.newState()
      val s3 = builder.newState()
      val s4 = builder.newState(isFinalState = true)

      s0.addRelationshipExpansion(s1, types = Array(O1), direction = Direction.OUTGOING)
      s0.addRelationshipExpansion(s2, types = Array(I1, I2), direction = Direction.INCOMING)
      s0.addRelationshipExpansion(s3, types = Array(L1, I3))
      s3.addRelationshipExpansion(s4, types = Array(O3, L2), direction = Direction.INCOMING)
      s4.addRelationshipExpansion(s0, types = Array(L3), direction = Direction.INCOMING)
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

      (expected, start, builder)
    }
  }

  test("node juxtaposition should filter on node predicate") {
    runTest { fx =>
      val start = fx.write.nodeCreate

      val builder = new PGStateBuilder
      val s0 = builder.newState(isStartState = true)
      val s1 = builder.newState()
      val s2 = builder.newState(isFinalState = true, predicate = Predicates.ALWAYS_FALSE_LONG)

      s0.addNodeJuxtaposition(s1)
      s0.addNodeJuxtaposition(s2)
      s1.addNodeJuxtaposition(s2)

      val expected = new ProductGraph()
        .addNode(start, s0)
        .addNode(start, s1)
        .addJuxtaposition(start, s0, s1)

      (expected, start, builder)
    }
  }

  private def runTest(configure: Fixture => (ProductGraph, Long, PGStateBuilder)): Unit = {
    val (expected, start, builder) = withFixture { fx =>
      val (expected, start, builder) = configure(fx)

      val actual = ProductGraph.fromCursor(start, builder.getStart.state, fx.pgCursor)
      actual should equalProductGraph(expected)
      fx.verifyMultiStateExpansions(builder, expected)

      (expected, start, builder)
    }

    // test in separate transaction
    withFixture { fx =>
      val actual = ProductGraph.fromCursor(start, builder.getStart.state, fx.pgCursor)
      actual should equalProductGraph(expected)
      fx.verifyMultiStateExpansions(builder, expected)
    }
  }

  private def withFixture[A](f: Fixture => A): A = {
    withTx { tx =>
      val nodeCursor = tx.kernelTransaction().cursors().allocateNodeCursor(CursorContext.NULL_CONTEXT)
      val relCursor = tx.kernelTransaction().cursors().allocateRelationshipTraversalCursor(CursorContext.NULL_CONTEXT)
      try {
        f(new Fixture(tx, nodeCursor, relCursor))
      } finally {
        nodeCursor.close()
        relCursor.close()
        tx.commit()
      }
    }
  }

  implicit private def builderStateToStateId(state: PGStateBuilder.BuilderState): Int = state.state.id()

  class Fixture(
    val tx: InternalTransaction,
    val nodeCursor: NodeCursor,
    val relCursor: RelationshipTraversalCursor
  ) {
    val write: Write = tx.kernelTransaction().dataWrite()
    val read: Read = tx.kernelTransaction().dataRead()
    val pgCursor = new ProductGraphTraversalCursor(read, nodeCursor, relCursor, EmptyMemoryTracker.INSTANCE)

    def relType(name: String): Int =
      tx.kernelTransaction().tokenWrite().relationshipTypeGetOrCreateForName(name)

    def nodeCreate(): Long =
      write.nodeCreate()

    /**
     * Since {{ProductGraph.fromCursor}} does a simple DFS, we use this method to assert that calling
     * {{ProductGraphTraversalCursor.setNodeAndStates}} with multiple states yields the same set of expanded relationships
     * as doing so with each state in turn.
     * */
    def verifyMultiStateExpansions(builder: PGStateBuilder, graph: ProductGraph): Unit = {
      val nodeStates = graph.adjacencyLists.keys
        .map(node => node -> builder.getState(node.stateId).state)
        .filter(_._2.getRelationshipExpansions.nonEmpty)
        .groupMap(_._1.id)(_._2)

      for {
        (nodeId, states) <- nodeStates
        statesSubset <- states.toSet.subsets()
      } {
        val expectedRelationships =
          statesSubset
            .flatMap(state => graph.adjacencyLists(PGNode(nodeId, state.id)))
            .filter(_.id != StatementConstants.NO_SUCH_RELATIONSHIP)

        pgCursor.setNodeAndStates(nodeId, statesSubset.toList.asJava)
        val foundRelationships = new Iterator[PGRelationship] {
          def hasNext: Boolean = pgCursor.next()

          def next(): PGRelationship = PGRelationship(
            pgCursor.relationshipReference,
            PGNode(pgCursor.otherNodeReference, pgCursor.targetState().id)
          )
        }.toSet

        foundRelationships shouldEqual expectedRelationships
      }
    }
  }
}
