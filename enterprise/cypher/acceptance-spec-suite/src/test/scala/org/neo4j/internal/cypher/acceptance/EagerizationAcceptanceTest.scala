/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.collection.RawIterator
import org.neo4j.cypher.internal.runtime.planDescription.InternalPlanDescription
import org.neo4j.cypher.internal.runtime.{Counter, CreateTempFileTestSupport, InternalExecutionResult}
import org.neo4j.cypher.{ExecutionEngineFunSuite, QueryStatisticsTestSupport}
import org.neo4j.graphdb.Node
import org.neo4j.internal.cypher.acceptance.CypherComparisonSupport._
import org.neo4j.internal.kernel.api.exceptions.ProcedureException
import org.neo4j.internal.kernel.api.helpers.{RelationshipSelectionCursor, RelationshipSelections}
import org.neo4j.internal.kernel.api.procs
import org.neo4j.internal.kernel.api.procs.{Neo4jTypes, ProcedureSignature}
import org.neo4j.kernel.api.proc.CallableProcedure.BasicProcedure
import org.neo4j.kernel.api.proc.Context
import org.neo4j.kernel.api.{ResourceTracker, proc}
import org.neo4j.procedure.Mode
import org.neo4j.values.storable.Values
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.util.matching.Regex

class EagerizationAcceptanceTest
  extends ExecutionEngineFunSuite
    with TableDrivenPropertyChecks
    with QueryStatisticsTestSupport
    with CypherComparisonSupport
    with CreateTempFileTestSupport {

  val VERBOSE = false
  val VERBOSE_INCLUDE_PLAN_DESCRIPTION = true

  val EagerRegEx: Regex = "Eager(?!(Aggregation))".r

  test("should be eager between node property writes in QG head and reads in horizon") {
    val n1 = createNode("val" -> 1)
    val n2 = createNode("val" -> 1)
    relate(n1, n2)

    val query =
      """MATCH (n)--(m)
        |SET n.val = n.val + 1
        |WITH *
        |UNWIND [1] as i WITH *
        |RETURN n.val AS nv, m.val AS mv
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, Configs.Cost2_3 + Configs.Rule2_3))

    result.toList should equal(List(Map("nv" -> 2, "mv" -> 2),
      Map("nv" -> 2, "mv" -> 2)))
    assertStats(result, propertiesWritten = 2)
  }

  test("should be eager between node property writes in QG tail and reads in horizon") {
    val n1 = createNode("val" -> 1)
    val n2 = createNode("val" -> 1)
    relate(n1, n2)

    val query =
      """UNWIND [1] as i WITH *
        |MATCH (n)--(m)
        |SET n.val = n.val + 1
        |RETURN n.val AS nv, m.val AS mv
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query, expectedDifferentResults = Configs.Rule2_3,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))

    result.toList should equal(List(Map("nv" -> 2, "mv" -> 2),
      Map("nv" -> 2, "mv" -> 2)))
    assertStats(result, propertiesWritten = 2)
  }

  test("should be eager between node property writes in QG tail and reads in horizon of another tail") {
    val n1 = createNode("val" -> 1)
    val n2 = createNode("val" -> 1)
    relate(n1, n2)

    val query =
      """UNWIND [1] as i WITH *
        |MATCH (n)--(m)
        |SET n.val = n.val + 1
        |WITH *
        |UNWIND [1] as j WITH *
        |RETURN n.val AS nv, m.val AS mv
      """.stripMargin

    val result = executeWith(
      Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))

    result.toList should equal(List(Map("nv" -> 2, "mv" -> 2),
      Map("nv" -> 2, "mv" -> 2)))
    assertStats(result, propertiesWritten = 2)
  }

  test("should be eager between relationship property writes in QG head and reads in horizon") {
    val n1 = createNode()
    val n2 = createNode()
    relate(n1, n2, ("val" -> 1))

    val query =
      """MATCH (n)-[r]-(m)
        |SET r.val = r.val + 1
        |RETURN r.val AS rv
      """.stripMargin

    val result = executeWith(
      Configs.UpdateConf,
      expectedDifferentResults = Configs.Rule2_3,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, Configs.Rule2_3),
      query = query)

    result.toList should equal(List(Map("rv" -> 3), Map("rv" -> 3)))
    assertStats(result, propertiesWritten = 2)
  }

  test("should be eager between relationship property writes in QG tail and reads in horizon") {
    val n1 = createNode()
    val n2 = createNode()
    relate(n1, n2, ("val" -> 1))

    val query =
      """UNWIND [1] as i WITH *
        |MATCH (n)-[r]-(m)
        |SET r.val = r.val + 1
        |RETURN r.val AS rv
      """.stripMargin

    val result = executeWith(
      Configs.UpdateConf,
      expectedDifferentResults = Configs.Rule2_3,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1),
      query = query)

    result.toList should equal(List(Map("rv" -> 3), Map("rv" -> 3)))
    assertStats(result, propertiesWritten = 2)
  }

  test("should handle detach deleting the same node twice") {
    val a = createLabeledNode("A")
    relate(a, createNode())

    val query =
      """MATCH (a:A)
        |UNWIND [0, 1] AS i
        |MATCH (a)-->()
        |DETACH DELETE a
        |RETURN count(*)
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query = query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))

    result.columnAs[Long]("count(*)").next should equal(2)
    assertStats(result, nodesDeleted = 1, relationshipsDeleted = 1)
  }

  test("should introduce eagerness between MATCH and DELETE relationships") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")
    val query = "MATCH (a)-[t:T]-(b) DELETE t RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query = query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, relationshipsDeleted = 1)
  }

  test("should not introduce extra eagerness after CALL of writing procedure") {
    // Given
    var counter = Counter()

    registerProcedure("user.mkRel") { builder =>
      builder.in("x", Neo4jTypes.NTNode)
      builder.in("y", Neo4jTypes.NTNode)
      builder.out("relId", Neo4jTypes.NTInteger)
      builder.mode(Mode.WRITE)
      new BasicProcedure(builder.build) {
        override def apply(ctx: Context, input: Array[AnyRef],
                           resourceTracker: ResourceTracker): RawIterator[Array[AnyRef], ProcedureException] = {
          val transaction = ctx.get(proc.Context.KERNEL_TRANSACTION)
          val statement = transaction.acquireStatement()
          try {
            val relType = transaction.tokenWrite().relationshipTypeGetOrCreateForName("KNOWS")
            val nodeX = input(0).asInstanceOf[Node]
            val nodeY = input(1).asInstanceOf[Node]
            val rel = transaction.dataWrite().relationshipCreate( nodeX.getId, relType, nodeY.getId)
            val prop = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" )
            transaction.dataWrite().relationshipSetProperty(rel, prop, Values.of(counter.counted))
            counter += 1
            RawIterator.of(Array(new java.lang.Long(rel)))
          } finally {
            statement.close()
          }
        }
      }
    }

    createNode()
    createNode()
    val query = "MATCH (a), (b) CALL user.mkRel(a, b) YIELD relId WITH * MATCH ()-[rel]->() WHERE id(rel) = relId RETURN rel.foo"

    // Correct! Eagerization happens as part of query context operation
    val result = executeWith(Configs.UpdateConf - Configs.AllRulePlanners, query,
      executeBefore = () => counter.reset(),
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))

    result.toSet should equal(Set(Map("rel.foo" -> 0), Map("rel.foo" -> 1), Map("rel.foo" -> 2), Map("rel.foo" -> 3)))
  }

  test("should not introduce extra eagerness after CALL of writing void procedure") {
    // Given
    var counter = Counter()

    registerProcedure("user.mkRel") { builder =>
      builder.in("x", Neo4jTypes.NTNode)
      builder.in("y", Neo4jTypes.NTNode)
      builder.out(ProcedureSignature.VOID)
      builder.mode(Mode.WRITE)
      new BasicProcedure(builder.build) {
        override def apply(ctx: Context, input: Array[AnyRef],
                           resourceTracker: ResourceTracker): RawIterator[Array[AnyRef], ProcedureException] = {
          val transaction = ctx.get(proc.Context.KERNEL_TRANSACTION)
          val statement = transaction.acquireStatement()
          try {
            val relType = transaction.tokenWrite().relationshipTypeGetOrCreateForName("KNOWS")
            val nodeX = input(0).asInstanceOf[Node]
            val nodeY = input(1).asInstanceOf[Node]
            val rel = transaction.dataWrite().relationshipCreate( nodeX.getId, relType, nodeY.getId)
            val prop = transaction.tokenWrite().propertyKeyGetOrCreateForName( "foo" )
            transaction.dataWrite().relationshipSetProperty(rel, prop, Values.of(counter.counted))
            counter += 1
            RawIterator.empty()
          } finally {
            statement.close()
          }
        }
      }
    }

    createNode()
    createNode()
    val query = "MATCH (a), (b) CALL user.mkRel(a, b) MATCH (a)-[rel]->(b) RETURN rel.foo"

    // Correct! Eagerization happens as part of query context operation
    val result = executeWith(Configs.UpdateConf - Configs.AllRulePlanners, query,
      executeBefore = () => counter.reset(),
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))

    counter.counted should equal(4)
    result.toSet should equal(Set(Map("rel.foo" -> 0), Map("rel.foo" -> 1), Map("rel.foo" -> 2), Map("rel.foo" -> 3)))
  }

  test("should not introduce extra eagerness after CALL of reading procedure") {
    // Given
    registerProcedure("user.expand") { builder =>
      builder.in("x", Neo4jTypes.NTNode)
      builder.in("y", Neo4jTypes.NTNode)
      builder.out("relId", Neo4jTypes.NTInteger)
      new BasicProcedure(builder.build) {
        override def apply(ctx: Context, input: Array[AnyRef],
                           resourceTracker: ResourceTracker): RawIterator[Array[AnyRef], ProcedureException] = {
          val transaction = ctx.get(proc.Context.KERNEL_TRANSACTION)
          val cursors = transaction.cursors()
          val nodeCursor = cursors.allocateNodeCursor()
          var relCursor: RelationshipSelectionCursor = null
          try {
            val idX = input(0).asInstanceOf[Node].getId
            val idY = input(1).asInstanceOf[Node].getId
            transaction.dataRead().singleNode(idX, nodeCursor)
            val result = Array.newBuilder[Array[AnyRef]]
            if (nodeCursor.next()) {
              relCursor = RelationshipSelections.outgoingCursor(cursors, nodeCursor, null)
              while (relCursor.next()) {
                if (relCursor.targetNodeReference() == idY) {
                  result += Array(new java.lang.Long(relCursor.relationshipReference()))
                }
              }
            }
            RawIterator.of(result.result(): _*)
          } finally {
            nodeCursor.close()
            if (relCursor != null) {
              relCursor.close()
            }
          }
        }
      }
    }

    val nodeA = createNode()
    val nodeB = createNode()
    val nodeC = createNode()
    relate(nodeA, nodeB)
    relate(nodeA, nodeC)

    val query = "MATCH (x), (y) CALL user.expand(x, y) YIELD relId RETURN x, y, relId"

    // Correct! No eagerization necessary
    val result = executeWith(Configs.UpdateConf - Configs.AllRulePlanners, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))

    result.size should equal(2)
  }

  test("should not introduce extra eagerness after CALL of reading VOID procedure") {
    // Given
    var counter = Counter()

    registerProcedure("user.expand") { builder =>
      builder.in("x", Neo4jTypes.NTNode)
      builder.in("y", Neo4jTypes.NTNode)
      builder.out(procs.ProcedureSignature.VOID)
      new BasicProcedure(builder.build) {
        override def apply(ctx: Context, input: Array[AnyRef],
                           resourceTracker: ResourceTracker): RawIterator[Array[AnyRef], ProcedureException] = {
          val transaction = ctx.get(proc.Context.KERNEL_TRANSACTION)
          val cursors = transaction.cursors()
          val nodeCursor = cursors.allocateNodeCursor()
          var relCursor: RelationshipSelectionCursor = null

          try {
            val idX = input(0).asInstanceOf[Node].getId
            val idY = input(1).asInstanceOf[Node].getId
            transaction.dataRead().singleNode(idX, nodeCursor)
            if (nodeCursor.next()) {
              relCursor = RelationshipSelections.outgoingCursor(cursors, nodeCursor, null)
              while (relCursor.next()) if (relCursor.targetNodeReference() == idY) counter += 1
            }
            RawIterator.empty()
          } finally {
            nodeCursor.close()
            if (relCursor != null) {
              relCursor.close()
            }
          }
        }
      }
    }

    val nodeA = createNode()
    val nodeB = createNode()
    val nodeC = createNode()
    relate(nodeA, nodeB)
    relate(nodeA, nodeC)

    val query = "MATCH (x), (y) CALL user.expand(x, y) WITH * MATCH (x)-[rel]->(y) RETURN *"

    // Correct! No eagerization necessary
    val result = executeWith(Configs.UpdateConf - Configs.AllRulePlanners, query,
      executeBefore = () => counter.reset(),
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))

    result.size should equal(2)
    counter.counted should equal(2)
  }

  test("should introduce eagerness between MATCH and CREATE relationships with overlapping relationship types") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")
    val query = "MATCH (a)-[t:T]-(b) CREATE (a)-[:T]->(b) RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, relationshipsCreated = 2)
  }

  test("should not introduce eagerness between MATCH and CREATE relationships when properties don't overlap") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T", Map("prop1" -> "foo"))
    val query = "MATCH (a)-[t:T {prop1: 'foo'}]-(b) CREATE (a)-[:T {prop2: 'bar'}]->(b) RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.AllRulePlanners))
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, relationshipsCreated = 2, propertiesWritten = 2)
  }

  test("should introduce eagerness between MATCH and CREATE relationships when properties overlap") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T", Map("prop1" -> "foo"))
    val query = "MATCH (a)-[t:T {prop1: 'foo'}]-(b) CREATE (a)-[:T {prop1: 'foo'}]->(b) RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, relationshipsCreated = 2, propertiesWritten = 2)
  }

  test("should plan eagerness for delete on paths") {
    val node0 = createLabeledNode("L")
    val node1 = createLabeledNode("L")
    relate(node0, node1)

    val query = "MATCH p=(:L)-[*]-() DELETE p RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, relationshipsDeleted = 1, nodesDeleted = 2)
  }

  test("should plan eagerness for detach delete on paths") {
    val node0 = createLabeledNode("L")
    val node1 = createLabeledNode("L")
    relate(node0, node1)

    val query = "MATCH p=(:L)-[*]-() DETACH DELETE p RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, relationshipsDeleted = 1, nodesDeleted = 2)
  }

  test("github issue #5653") {
    graph.execute("CREATE (a:Person {id: 42})-[:FRIEND_OF]->(b:Person {id:42}), (b)-[:FRIEND_OF]->(a), (:Person)-[:FRIEND_OF]->(b)")

    val query = "MATCH (p1:Person {id: 42})-[r:FRIEND_OF]->(p2:Person {id:42}) DETACH DELETE r, p1, p2 RETURN count(*) AS count"
    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, relationshipsDeleted = 3, nodesDeleted = 2)
  }

  test("github issue #5653 with path instead") {
    graph.execute("CREATE (a:Person {id: 42})-[:FRIEND_OF]->(b:Person {id:42}), (b)-[:FRIEND_OF]->(a), (:Person)-[:FRIEND_OF]->(b)")

    val query = "MATCH p = (p1:Person {id: 42})-[r:FRIEND_OF]->(p2:Person {id:42}) DETACH DELETE p RETURN count(*) AS count"
    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, relationshipsDeleted = 3, nodesDeleted = 2)
  }

  test("should not introduce eagerness between MATCH and CREATE relationships with overlapping relationship types when directed") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")
    val query = "MATCH (a)-[t:T]->(b) CREATE (a)-[:T]->(b) RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, optimalEagerCount = 0))
    result.columnAs[Int]("count").next should equal(1)
    assertStats(result, relationshipsCreated = 1)
  }

  test("should not introduce eagerness between MATCH and CREATE relationships with unrelated relationship types") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")
    val query = "MATCH (a)-[t:T]-(b) CREATE (a)-[:T2]->(b) RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, relationshipsCreated = 2)
  }

  // TESTS FOR DELETE AND MERGE

  test("should delete all nodes before merge tries to read") {
    createNode("p" -> 0)
    createNode("p" -> 0)

    val query = "MATCH (n) DELETE n MERGE (m {p: 0}) ON CREATE SET m.p = 1 RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, Configs.Rule2_3))

    assertStats(result, nodesCreated = 2, propertiesWritten = 4, nodesDeleted = 2)
    result.columnAs[Long]("count(*)").next shouldBe 2
  }

  test("should introduce eagerness between DELETE and MERGE for node also with UNWIND") {
    createLabeledNode(Map("value" -> 0, "deleted" -> true), "B")
    createLabeledNode(Map("value" -> 1, "deleted" -> true), "B")

    val query =
      """
        |MATCH (b:B)
        |DELETE b
        |WITH *
        |UNWIND [0] AS i
        |MERGE (b2:B { value: 1 })
        |RETURN count(*)
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, Configs.Rule2_3))
    assertStats(result, nodesCreated = 1, nodesDeleted = 2, propertiesWritten = 1, labelsAdded = 1)
    result.columnAs[Long]("count(*)").next shouldBe 2
  }

  test("should introduce eagerness between DELETE and MERGE for node") {
    createLabeledNode(Map("value" -> 0, "deleted" -> true), "B")
    createLabeledNode(Map("value" -> 1, "deleted" -> true), "B")
    createLabeledNode(Map("value" -> 2, "deleted" -> true), "B")

    val query =
      """
        |MATCH (b:B)
        |DELETE b
        |MERGE (b2:B { value: 1 }) // this is supposed to not be the first node matched by the label scan
        |RETURN b2.deleted
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, Configs.Rule2_3))
    assertStats(result, nodesCreated = 1, nodesDeleted = 3, propertiesWritten = 1, labelsAdded = 1)
    result.columnAs[Node]("b2.deleted").toList should equal(List(null, null, null))
  }

  test("should introduce eagerness between DELETE and MERGE for node with projection") {
    createLabeledNode(Map("value" -> 0, "deleted" -> true), "B")
    createLabeledNode(Map("value" -> 1, "deleted" -> true), "B")

    val query =
      """
        |MATCH (b:B)
        |WITH b.value AS v, b
        |DELETE b
        |WITH *
        |MERGE (b2:B { value: (v + 1) % 2 }) // the other node
        |RETURN b2.deleted
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2, optimalEagerCount = 1, expectPlansToFailPredicate = Configs.AllRulePlanners - Configs.Rule2_3))
    assertStats(result, nodesCreated = 2, nodesDeleted = 2, propertiesWritten = 2, labelsAdded = 2)
    result.columnAs[Node]("b2.deleted").toList should equal(List(null, null))
  }

  test("should introduce eagerness between DELETE and MERGE for nodes when there merge matches all labels") {
    createLabeledNode("B")
    createLabeledNode("B")

    val query =
      """
        |MATCH (b:B)
        |DELETE b
        |MERGE ()
        |RETURN count(*)
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query)
    result.toList should equal(List(Map("count(*)" -> 2)))
    assertStats(result, nodesCreated = 1, nodesDeleted = 2)
  }

  ignore("should not introduce eagerness between DELETE and MERGE for nodes when deleting variable not bound for same label") {
    // TODO: Delete must know what label(s) on nodes it deletes to be able to solve this

    createLabeledNode("B")
    createLabeledNode("B")
    createLabeledNode("C")
    createLabeledNode("C")
    val query =
      """
        |MATCH (b:B)
        |MATCH (c:C)
        |DELETE b
        |MERGE (:C)
        |RETURN count(*)
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    assertStats(result, nodesCreated = 0, nodesDeleted = 2)
    result.columnAs[Long]("count(*)").next shouldBe 8
  }

  test("should introduce eagerness between MATCH and DELETE + DELETE and MERGE for relationship") {
    val a = createNode()
    val b = createNode()
    val rel1 = relate(a, b, "T", Map("id" -> 1))
    val rel2 = relate(a, b, "T", Map("id" -> 2))
    val query =
      """
        |MATCH (a)-[t:T]->(b)
        |DELETE t
        |MERGE (a)-[t2:T]->(b)
        |RETURN exists(t2.id)
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2, optimalEagerCount = 1))
    assertStats(result, relationshipsDeleted = 2, relationshipsCreated = 1)

    // Merge should not be able to match on deleted relationship
    result.toList should equal(List(Map("exists(t2.id)" -> false), Map("exists(t2.id)" -> false)))
  }

  test("should introduce eagerness between MATCH and DELETE + DELETE and MERGE for relationship, direction reversed") {
    val a = createNode()
    val b = createNode()
    val rel1 = relate(a, b, "T", Map("id" -> 1))
    val query =
      """
        |MATCH (a)-[t:T]->(b)
        |DELETE t
        |MERGE (a)<-[t2:T]-(b)
        |RETURN count(*)
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2))
    assertStats(result, relationshipsDeleted = 1, relationshipsCreated = 1)
    result.columnAs[Long]("count(*)").next shouldBe 1
  }

  test("should introduce eagerness between DELETE and MERGE for relationships when there is no read matching the merge") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T", Map("id" -> 1))
    relate(a, b, "T", Map("id" -> 1))
    val query =
      """
        |MATCH (a)-[t:T]->(b)
        |DELETE t
        |MERGE (a)-[t2:T2]->(b)
        |RETURN exists(t2.id)
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2, optimalEagerCount = 1))
    assertStats(result, relationshipsDeleted = 2, relationshipsCreated = 1)
    result.toList should equal(List(Map("exists(t2.id)" -> false), Map("exists(t2.id)" -> false)))
  }

  test("should introduce eagerness between DELETE and MERGE for relationships when there is a read matching the merge") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T", Map("id" -> 1))
    relate(a, b, "T", Map("id" -> 1))
    val query =
      """
        |MATCH (a)-[t]->(b)
        |DELETE t
        |MERGE (a)-[t2:T2]->(b)
        |RETURN exists(t2.id)
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2, optimalEagerCount = 1))
    assertStats(result, relationshipsDeleted = 2, relationshipsCreated = 1)
    result.toList should equal(List(Map("exists(t2.id)" -> false), Map("exists(t2.id)" -> false)))
  }

  test("should introduce eagerness between DELETE and MERGE for relationships when there is a read matching the merge, direction reversed") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T", Map("id" -> 1))
    relate(a, b, "T", Map("id" -> 1))
    val query =
      """
        |MATCH (a)-[t]->(b)
        |DELETE t
        |MERGE (a)<-[t2:T2]-(b)
        |RETURN count(*)
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2))
    assertStats(result, relationshipsDeleted = 2, relationshipsCreated = 1)
    result.columnAs[Long]("count(*)").next shouldBe 2
  }

  // TESTS FOR MATCH AND CREATE

  test("should not introduce eagerness for MATCH nodes and CREATE relationships") {
    createNode()
    createNode()
    val query = "MATCH (a), (b) CREATE (a)-[:KNOWS]->(b) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    assertStats(result, relationshipsCreated = 4)
    result.columnAs[Long]("count(*)").next shouldBe 4
  }

  test("should introduce eagerness for match create match") {
    createNode()
    createNode()

    val query = "MATCH () CREATE () WITH * MATCH (n) RETURN count(*) AS count"

    val result = executeWith(Configs.UpdateConf, query, expectedDifferentResults = Configs.Rule2_3,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))

    assertStats(result, nodesCreated = 2)
    result.columnAs[Int]("count").next should equal(8)
  }

  test("should introduce eagerness for match create match create") {
    createNode("prop" -> 42)
    createNode("prop" -> 43)

    val query = "MATCH (k) CREATE (l {prop: 44}) WITH * MATCH (m) CREATE (n {prop:45}) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      expectedDifferentResults = Configs.Rule2_3,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2))

    result.columnAs[Long]("count(*)").next shouldBe 8
    assertStats(result, nodesCreated = 10, propertiesWritten = 10)
  }

  test("should not introduce eagerness for simple match create with nodes created in same tx") {
    createNode()
    createNode()
    createNode()

    val executeBefore: () => Unit = () => {
      createNode()
      createNode()
      createNode()
    }

    val query = "MATCH () CREATE () RETURN count(*)"

    val result = executeWith(
      Configs.UpdateConf,
      query = query,
      executeBefore = executeBefore,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 6
    assertStats(result, nodesCreated = 6)
  }

  test("should not introduce eagerness for leaf create match") {
    val query = "CREATE () WITH * MATCH () RETURN count(*)"
    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = ComparePlansWithAssertion((plan) => {
        plan should not(useOperators("ReadOnly"))
        assertNumberOfEagerness(plan, 0) should be(true)
      }))
    assertStats(result, nodesCreated = 1)
    result.columnAs[Long]("count(*)").next shouldBe 1
  }

  test("should not need eagerness for match create with labels") {
    createLabeledNode("L")
    val query = "MATCH (:L) CREATE (:L) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, nodesCreated = 1, labelsAdded = 1)
  }

  test("should not need eagerness for match create with labels and property with index") {
    createLabeledNode(Map("id" -> 0), "L")
    graph.createIndex("L", "id")

    val query = "MATCH (n:L {id: 0}) USING INDEX n:L(id) CREATE (:L {id:0}) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 1)
  }

  test("should need eagerness for double match and then create") {
    createNode()
    createNode()
    val query = "MATCH (), () CREATE () RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result, nodesCreated = 4)
  }

  test("should not need eagerness for double match and then create when non-overlapping properties") {
    createNode("prop1" -> 42, "prop2" -> 42)
    createNode("prop1" -> 42, "prop2" -> 42)
    val query = "MATCH (a {prop1: 42}), (n {prop2: 42}) CREATE ({prop3: 42}) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.AllRulePlanners))
    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result, nodesCreated = 4, propertiesWritten = 4)
  }

  test("should need eagerness for double match and then create when overlapping properties") {
    createNode("prop1" -> 42, "prop2" -> 42)
    createNode("prop1" -> 42, "prop2" -> 42)
    val query = "MATCH (a {prop1: 42}), (n {prop2: 42}) CREATE ({prop1: 42, prop2: 42}) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result, nodesCreated = 4, propertiesWritten = 8)
  }

  test("should not add eagerness when not writing to nodes") {
    createNode()
    createNode()
    val query = "MATCH (a), (b) CREATE (a)-[r:KNOWS]->(b) SET r = { key: 42 } RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result, relationshipsCreated = 4, propertiesWritten = 4)
  }

  test("matching using a pattern predicate and creating relationship should not be eager") {
    relate(createNode(), createNode())
    val query = "MATCH (n) WHERE (n)-->() CREATE (n)-[:T]->() RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, nodesCreated = 1, relationshipsCreated = 1)
  }

  test("should not be eager when creating single node after matching on pattern with relationship") {
    relate(createNode(), createNode())
    relate(createNode(), createNode())
    val query = "MATCH ()--() CREATE () RETURN count(*) AS count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Long]("count").next shouldBe 4
    assertStats(result, nodesCreated = 4)
  }

  ignore("should not be eager when creating single node after matching on pattern with relationship and also matching on label") {
    // TODO: Implement RelationShipBoundNodeEffect. Then figure out with which Configuration to call executeWith
    val query = "MATCH (:L) MATCH ()--() CREATE ()"
    executeWith(Configs.All, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
  }

  test("should not be eager when creating single node after matching on empty node") {
    createNode()
    val query = "MATCH () CREATE () RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, nodesCreated = 1)
  }

  test("should not introduce an eager pipe between two node reads and a relationships create") {
    createNode()
    createNode()
    val query = "MATCH (a), (b) CREATE (a)-[:TYPE]->(b) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result, relationshipsCreated = 4)
  }

  test("should not introduce an eager pipe between two node reads and a relationships create when there is sorting between the two") {
    createNode()
    createNode()
    val query = "MATCH (a), (b) WITH a, b ORDER BY id(a) CREATE (a)-[:TYPE]->(b) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result, relationshipsCreated = 4)
  }

  test("should not introduce an eager pipe between a leaf node read and a relationship + node create") {
    createNode()
    createNode()
    val query = "MATCH (a) CREATE (a)-[:TYPE]->() RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 2
    assertStats(result, nodesCreated = 2, relationshipsCreated = 2)
  }

  test("should introduce an eager pipe between a non-leaf node read and a relationship + node create") {
    createNode()
    createNode()
    val query = "MATCH (), (a) CREATE (a)-[:TYPE]->() RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result, nodesCreated = 4, relationshipsCreated = 4)
  }

  test("should not introduce an eager pipe between a leaf relationship read and a relationship create") {
    relate(createNode(), createNode(), "TYPE")
    relate(createNode(), createNode(), "TYPE")
    val query = "MATCH (a)-[:TYPE]->(b) CREATE (a)-[:TYPE]->(b) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, optimalEagerCount = 0))
    assertStats(result, relationshipsCreated = 2)
    result.columnAs[Int]("count(*)").next should equal(2)
  }

  test("should introduce an eager pipe between a leaf relationship read and a relationship create if directions reversed 1") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "TYPE") // NOTE: The order the nodes are related should not affect the result (opposite from the test below)
    val query = "MATCH (a)-[:TYPE]->(b) CREATE (a)<-[:TYPE]-(b) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, relationshipsCreated = 1)
    result.columnAs[Int]("count(*)").next should equal(1)
  }

  test("should introduce an eager pipe between a leaf relationship read and a relationship create if directions reversed 2") {
    val a = createNode()
    val b = createNode()
    relate(b, a, "TYPE") // NOTE: The order the nodes are related should not affect the result (opposite from the test above)
    val query = "MATCH (a)-[:TYPE]->(b) CREATE (a)<-[:TYPE]-(b) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, relationshipsCreated = 1)
    result.columnAs[Int]("count(*)").next should equal(1)
  }

  test("should introduce an eager pipe between a non-directional leaf relationship read and a relationship create") {
    relate(createNode(), createNode(), "TYPE")
    relate(createNode(), createNode(), "TYPE")
    val query = "MATCH (a)-[:TYPE]-(b) CREATE (a)-[:TYPE]->(b) RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, relationshipsCreated = 4)
    result.columnAs[Int]("count").next should equal(4)
  }

  test("should introduce an eager pipe between a non-directional read and a relationship merge") {
    relate(createNode(), createNode(), "TYPE")
    relate(createNode(), createNode(), "TYPE")
    val query = "MATCH (a)-[:TYPE]-(b) MERGE (a)-[:TYPE]->(b) RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, relationshipsCreated = 2)
    result.columnAs[Int]("count").next should equal(4)

  }

  test("should introduce an eager pipe between a non-leaf relationship read, rel uniqueness, and a relationship create, with comma") {
    relate(createNode(), createNode(), "TYPE")
    relate(createNode(), createNode(), "TYPE")
    val query = "MATCH ()-[:TYPE]->(), (a)-[:TYPE]->(b) CREATE (a)-[:TYPE]->(b) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next shouldBe 2
    assertStats(result, relationshipsCreated = 2)
  }

  test("should introduce an eager pipe between a non-leaf relationship read, rel uniqueness, and a relationship create, with double match") {
    relate(createNode(), createNode(), "TYPE")
    relate(createNode(), createNode(), "TYPE")
    val query = "MATCH ()-[:TYPE]->() MATCH (a)-[:TYPE]->(b) CREATE (a)-[:TYPE]->(b) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result, relationshipsCreated = 4)
  }

  test("should handle conflicts with create after WITH") {
    relate(createNode(), createNode(), "TYPE")
    relate(createNode(), createNode(), "TYPE")
    val query = "MATCH ()-[:TYPE]->() CREATE (a)-[:TYPE]->(b) WITH * MATCH ()-[:TYPE]->() CREATE (c)-[:TYPE]->(d) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query, expectedDifferentResults = Configs.Rule2_3,
      planComparisonStrategy = testEagerPlanComparisonStrategy(3, optimalEagerCount = 2, expectPlansToFailPredicate = Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 8
    assertStats(result, nodesCreated = 20, relationshipsCreated = 10)
  }

  test("should introduce an eager pipe between a non-leaf relationship read and a relationship create") {
    relate(createLabeledNode("LabelOne"), createLabeledNode("LabelTwo"), "TYPE")
    relate(createLabeledNode("LabelOne"), createLabeledNode("LabelTwo"), "TYPE")
    val query = "MATCH ()-[:TYPE]->() MATCH (a:LabelOne)-[:TYPE]->(b:LabelTwo) CREATE (a)-[:TYPE]->(b) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result, relationshipsCreated = 4)
  }

  test("should not introduce eagerness for match - create on different relationship types") {
    relate(createNode(), createNode(), "T1")
    relate(createNode(), createNode(), "T1")
    val query = "MATCH ()-[:T1]->() CREATE ()-[:T2]->() RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 2
    assertStats(result, nodesCreated = 4, relationshipsCreated = 2)
  }

  // TESTS FOR MATCH AND DELETE

  test("should introduce eagerness when deleting nodes on normal matches") {
    createLabeledNode("Person")
    createLabeledNode("Person")
    createLabeledNode("Movie")
    createLabeledNode("Movie")
    createNode()

    val query = "MATCH (a:Person), (m:Movie) DELETE a, m RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result, nodesDeleted = 4)
  }

  test("should not introduce eagerness when deleting nodes on single leaf") {
    createLabeledNode("Person")
    createLabeledNode("Person")
    createLabeledNode("Movie")

    val query = "MATCH (a:Person) DELETE a RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 2
    assertStats(result, nodesDeleted = 2)
  }

  test("should include eagerness when reading and deleting") {
    relate(createNode(), createNode())

    val query = "MATCH (a)-[r]-(b) DELETE r,a,b RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next shouldBe 2
    assertStats(result, nodesDeleted = 2, relationshipsDeleted = 1)
  }

  test("matching relationship, deleting relationship and nodes should be eager") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")

    val query = "MATCH (a)-[r]-(b) DELETE r, a, b RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.toList should equal(List(Map("count" -> 2)))
    assertStats(result, nodesDeleted = 2, relationshipsDeleted = 1)
  }

  test("matching relationship with property, deleting relationship and nodes should be eager") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T", Map("prop" -> 3))

    val query = "MATCH (a)-[r {prop : 3}]-(b) DELETE r, a, b RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next shouldBe 2
    assertStats(result, nodesDeleted = 2, relationshipsDeleted = 1)
  }

  test("matching directional relationship, and detach deleting both nodes should be eager") {
    val a = createNode()
    val b = createNode()
    relate(a, b)
    relate(a, b)

    val query = "MATCH (a)-[r]->(b) DETACH DELETE a, b RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))

    result.toList should equal(List(Map("count(*)" -> 2)))
    assertStats(result, nodesDeleted = 2, relationshipsDeleted = 2)
  }

  test("matching directional relationship, and deleting it should not be eager") {
    val a = createNode()
    val b = createNode()
    relate(a, b)
    relate(a, b)

    val query = "MATCH (a)-[r]->(b) DELETE r RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, optimalEagerCount = 0))

    result.toList should equal(List(Map("count(*)" -> 2)))
    assertStats(result, relationshipsDeleted = 2)
  }

  test("matching directional relationship, deleting relationship and labeled nodes should not be eager") {
    relate(createLabeledNode("A"), createLabeledNode("B"), "T")
    relate(createLabeledNode("A"), createLabeledNode("B"), "T")

    val query = "MATCH (a:A)-[r]->(b:B) DELETE r, a, b RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, optimalEagerCount = 0))
    result.toList should equal(List(Map("count(*)" -> 2)))
    assertStats(result, nodesDeleted = 4, relationshipsDeleted = 2)
  }

  test("matching reversed directional relationship, deleting relationship and labeled nodes should not be eager") {
    relate(createLabeledNode("A"), createLabeledNode("B"), "T")
    relate(createLabeledNode("A"), createLabeledNode("B"), "T")

    val query = "MATCH (b:B)<-[r]-(a:A) DELETE r, a, b RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, optimalEagerCount = 0))
    result.toList should equal(List(Map("count(*)" -> 2)))
    assertStats(result, nodesDeleted = 4, relationshipsDeleted = 2)
  }

  test("matching directional relationship with property, deleting relationship and nodes should not be eager") {
    relate(createNode(), createNode(), "T", Map("prop" -> 3))
    relate(createNode(), createNode(), "T", Map("prop" -> 3))
    relate(createNode(), createNode(), "T", Map("prop" -> 3))
    relate(createNode(), createNode(), "T", Map("prop" -> 3))

    val query = "MATCH (a)-[r {prop : 3}]->(b) DELETE r, a, b RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, optimalEagerCount = 0))
    result.toList should equal(List(Map("count(*)" -> 4)))
    assertStats(result, nodesDeleted = 8, relationshipsDeleted = 4)
  }

  test("matching undirected relationship, deleting relationship and nodes should be eager") {
    relate(createNode(), createNode(), "T1")
    relate(createNode(), createNode(), "T2")
    relate(createNode(), createNode())
    relate(createNode(), createNode(), "T1")
    relate(createNode(), createNode(), "T2")
    relate(createNode(), createNode())

    val query = "MATCH (a)-[r]-(b) DELETE r, a, b RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.toList should equal(List(Map("count(*)" -> 12)))
    assertStats(result, nodesDeleted = 12, relationshipsDeleted = 6)
  }

  test("matching undirected relationship, deleting relationship and labeled nodes should be eager") {
    relate(createLabeledNode("A"), createLabeledNode("B"), "T1")
    relate(createLabeledNode("A"), createLabeledNode("A"), "T2")
    relate(createLabeledNode("B"), createLabeledNode("A"), "T3")

    val query = "MATCH (a:A)-[r]-(b) DELETE r, a, b RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.toList should equal(List(Map("count(*)" -> 4)))
    assertStats(result, nodesDeleted = 6, relationshipsDeleted = 3)
  }

  test("matching undirected relationship with property, deleting relationship and nodes should be eager") {
    relate(createNode(), createNode(), "T", Map("prop" -> 3))
    relate(createNode(), createNode(), "T", Map("prop" -> 3))
    relate(createNode(), createNode(), "T", Map("prop" -> 3))
    relate(createNode(), createNode(), "T", Map("prop" -> 3))

    val query = "MATCH (a)-[r {prop : 3}]-(b) DELETE r, a, b RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.toList should equal(List(Map("count(*)" -> 8)))
    assertStats(result, nodesDeleted = 8, relationshipsDeleted = 4)
  }

  test("matching directional multi-step relationship, deleting relationship and nodes should be eager") {
    val b = createNode()
    relate(createNode(), b)
    relate(createNode(), b)
    relate(b, createNode())
    relate(b, createNode())

    val query = "MATCH (a)-[r1]->(b)-[r2]->(c) DELETE r1, r2, a, b, c RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.toList should equal(List(Map("count(*)" -> 4)))
    assertStats(result, nodesDeleted = 5, relationshipsDeleted = 4)
  }

  test("matching directional varlength relationship, deleting relationship and nodes should be eager") {
    val b = createNode()
    relate(createNode(), b)
    relate(createNode(), b)
    relate(b, createNode())
    relate(b, createNode())

    val query = "MATCH (a)-[r*]->(b) DETACH DELETE a, b RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.toList should equal(List(Map("count(*)" -> 8)))
    assertStats(result, nodesDeleted = 5, relationshipsDeleted = 4)
  }

  // ANDRES CLAIMS THAT THIS TEST IS DUBIOUS
  test("create directional relationship with property, match and delete relationship and nodes within same query should be eager and work") {
    val query = "CREATE ()-[:T {prop: 3}]->() WITH * MATCH (a)-[r {prop : 3}]->(b) DELETE r, a, b RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, nodesCreated = 2, relationshipsCreated = 1, propertiesWritten = 1, nodesDeleted = 2, relationshipsDeleted = 1)
  }

  // TESTS USING OPTIONAL MATCHES

  test("should need eagerness for match optional match create") {
    createLabeledNode("A", "B")
    createLabeledNode("A", "B")
    createLabeledNode("A")
    val query = "MATCH (a:A) OPTIONAL MATCH (b:B) CREATE (:B) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next shouldBe 6
    assertStats(result, nodesCreated = 6, labelsAdded = 6)
  }

  test("should not need eagerness for match optional match create where labels do not interfere") {
    createLabeledNode("A", "B")
    createLabeledNode("A", "B")
    createLabeledNode("A")
    val query = "MATCH (a:A) OPTIONAL MATCH (b:B) CREATE (:A) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 6
    assertStats(result, nodesCreated = 6, labelsAdded = 6)
  }

  test("should introduce eagerness when deleting things on optional matches") {
    val node0 = createLabeledNode("Person")
    val node1 = createLabeledNode("Person")
    val node2 = createNode()
    relate(node0, node0)
    relate(node0, node2)
    relate(node0, node2)
    relate(node0, node2)
    relate(node1, node2)
    relate(node1, node2)
    relate(node1, node2)
    relate(node2, node2)

    val query = "MATCH (a:Person) OPTIONAL MATCH (a)-[r1]-() DELETE a, r1 RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next shouldBe 7
    assertStats(result, nodesDeleted = 2, relationshipsDeleted = 7)
  }

  test("should introduce eagerness when deleting relationship on optional expand into") {
    val node0 = createLabeledNode("Person")
    val node1 = createNode()
    relate(node0, node1)
    relate(node0, node1)

    val query = "MATCH (a:Person) MERGE (b) WITH * OPTIONAL MATCH (a)-[r1]-(b) DELETE r1 RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2, optimalEagerCount = 1, expectPlansToFailPredicate = Configs.AllRulePlanners - Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 3
    assertStats(result, relationshipsDeleted = 2)
  }

  test("should introduce eagerness when deleting things from an optional match which is a cartesian product") {
    val node0 = createLabeledNode("Person")
    val node1 = createLabeledNode("Person")
    val node2 = createLabeledNode("Movie")
    val node3 = createLabeledNode("Movie")
    val node4 = createNode()
    relate(node0, node1)
    relate(node0, node1)
    relate(node0, node1)
    relate(node2, node4)

    val query = "MATCH (a:Person) OPTIONAL MATCH (a)-[r1]-(), (m:Movie)-[r2]-() DELETE a, r1, m, r2 RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next shouldBe 6
    assertStats(result, nodesDeleted = 3, relationshipsDeleted = 4)
  }

  test("MATCH (a:Person),(m:Movie) CREATE (a)-[:T]->(m) WITH a OPTIONAL MATCH (a) RETURN *") {
    createLabeledNode("Person")
    createLabeledNode("Movie")
    val query = "MATCH (a:Person), (m:Movie) CREATE (a)-[:T]->(m) WITH a OPTIONAL MATCH (a) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, relationshipsCreated = 1)
  }

  // TESTS FOR MATCH AND MERGE

  test("on match set label on unstable iterator should be eager") {
    createLabeledNode("Two")
    createLabeledNode("Two")
    createNode()
    val query = "MATCH (m1:Two), (m2:Two), (n) MERGE (q) ON MATCH SET q:Two RETURN count(*) AS c"

    val result: InternalExecutionResult = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, labelsAdded = 1)
    result.toList should equal(List(Map("c" -> 36)))
  }

  test("on match set label on unstable iterator should be eager and work when creating new nodes") {
    createLabeledNode("Two")
    createLabeledNode("Two")
    createNode()
    val query = "MATCH (m1:Two), (m2:Two), (n) MERGE (q:Three) ON MATCH SET q:Two RETURN count(*) AS c"

    val result: InternalExecutionResult = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, labelsAdded = 2, nodesCreated = 1)
    result.toList should equal(List(Map("c" -> 12)))
  }

  test("should be eager if merging node with properties after matching all nodes") {
    createLabeledNode("Two")
    createLabeledNode("Two")
    createNode()
    val query = "MATCH (a:Two), (b) MERGE (q {p: 1}) RETURN count(*) AS c"

    val result: InternalExecutionResult = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, nodesCreated = 1, propertiesWritten = 1)
    result.toList should equal(List(Map("c" -> 6)))
  }

  test("on match set label on unstable iterator should not be eager if no overlap") {
    createLabeledNode("Two")
    createLabeledNode("Two")
    createNode()
    val query = "MATCH (m1:Two), (m2:Two) MERGE (q) ON MATCH SET q:One RETURN count(*) AS c"

    val result: InternalExecutionResult = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    assertStats(result, labelsAdded = 3)
    result.toList should equal(List(Map("c" -> 12)))
  }

  test("on create set label on unstable iterator should be eager") {
    createLabeledNode("Two")
    createLabeledNode("Two")
    createNode()
    val query = "MATCH (m1:Two), (m2:Two), (n) MERGE (q) ON CREATE SET q:Two RETURN count(*) AS c"

    val result: InternalExecutionResult = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, labelsAdded = 0)
    result.toList should equal(List(Map("c" -> 36)))
  }

  test("on match set property on unstable iterator should be eager") {
    createNode()
    createNode(Map("id" -> 0))
    createNode(Map("id" -> 0))
    val query = "MATCH (b {id: 0}), (c {id: 0}), (a) MERGE () ON MATCH SET a.id = 0 RETURN count(*) AS c"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.toList should equal(List(Map("c" -> 36)))
    assertStats(result, propertiesWritten = 36)
  }

  test("on match set property on unstable iterator should not be eager if no overlap") {
    createNode()
    createNode(Map("id" -> 0))
    createNode(Map("id" -> 0))
    val query = "MATCH (b {id: 0}), (c {id: 0}) MERGE (a) ON MATCH SET a.id2 = 0 RETURN count(*) AS c"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.AllRulePlanners))
    result.toList should equal(List(Map("c" -> 12)))
    assertStats(result, propertiesWritten = 12)
  }

  test("on create set overwrite property with literal map on unstable iterator should be eager") {
    createNode()
    createNode(Map("id" -> 0))
    createNode(Map("id" -> 0))
    val query = "MATCH (b {id: 0}), (c {id: 0}), (a) MERGE () ON CREATE SET a = {id: 0} RETURN count(*) AS c"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.toList should equal(List(Map("c" -> 36)))
    assertStats(result, propertiesWritten = 0)
  }

  test("on create set append property with literal map on unstable iterator should be eager") {
    createNode()
    createNode(Map("id" -> 0))
    createNode(Map("id" -> 0))
    val query = "MATCH (b {id: 0}), (c {id: 0}), (a) MERGE () ON CREATE SET a += {id: 0} RETURN count(*) AS c"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.toList should equal(List(Map("c" -> 36)))
    assertStats(result, propertiesWritten = 0)
  }

  test("on match set property with parameter map on unstable iterator should be eager") {
    createNode()
    createNode(Map("id" -> 0))
    createNode(Map("id" -> 0))
    val query = "MATCH (b {id: 0}), (c {id: 0}), (a) MERGE () ON MATCH SET a = {map} RETURN count(*) AS c"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1),
      params = Map("map" -> Map("id" -> 0)))
    result.toList should equal(List(Map("c" -> 36)))
    assertStats(result, propertiesWritten = 36)
  }

  test("should introduce eagerness between MATCH and DELETE path") {
    val a = createNode()
    val b = createNode()
    relate(a, b)
    relate(a, b)

    val query =
      """MATCH p=()-->()
        |DELETE p
        |RETURN count(*)
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, nodesDeleted = 2, relationshipsDeleted = 2)
    result.columnAs[Long]("count(*)").next shouldBe 2
  }

  // TODO: This does not work on 2.3 either
  ignore("should introduce eagerness between MATCH, UNWIND, DELETE nodes in path") {
    val a = createNode()
    val b = createNode()
    relate(a, b)
    relate(a, b)

    val query =
      """MATCH p=()-->()
        |UNWIND nodes(p) as n
        |DETACH DELETE n
        |RETURN count(*)
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, nodesDeleted = 2, relationshipsDeleted = 2)
    result.columnAs[Long]("count(*)").next shouldBe 2
  }

  test("should not introduce eagerness for MATCH nodes and MERGE relationships") {
    createNode()
    createNode()
    val query = "MATCH (a), (b) MERGE (a)-[r:KNOWS]->(b) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result, relationshipsCreated = 4)
  }

  test("should not introduce eagerness when the ON MATCH includes writing to a non-matched property") {
    val a = createLabeledNode("Foo")
    val b = createLabeledNode("Bar")
    relate(a, b, "KNOWS")

    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON MATCH SET a.prop = 42 RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, propertiesWritten = 1)
  }

  test("should not introduce eagerness when the ON MATCH includes writing to a left-side matched label") {
    val node0 = createLabeledNode("Foo")
    val node1 = createLabeledNode("Foo")
    val node2 = createLabeledNode("Bar")
    val node3 = createLabeledNode("Bar")
    relate(node0, node3, "KNOWS")

    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON MATCH SET b:Foo RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, optimalEagerCount = 0, expectPlansToFailPredicate = Configs.AllRulePlanners - Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result, relationshipsCreated = 3, labelsAdded = 1)

    //TODO this we need to consider not only overlap but also what known labels the node we set has
  }

  test("should introduce eagerness when the ON MATCH includes writing to a right-side matched label") {
    val node0 = createLabeledNode("Foo")
    val node1 = createLabeledNode("Foo")
    val node2 = createLabeledNode("Bar")
    val node3 = createLabeledNode("Bar")
    relate(node0, node3, "KNOWS")

    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON MATCH SET a:Bar RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result, relationshipsCreated = 3, labelsAdded = 1)
  }

  test("should not introduce eagerness when the ON CREATE includes writing to a left-side matched label") {
    val node0 = createLabeledNode("Foo")
    val node1 = createLabeledNode("Foo")
    val node2 = createLabeledNode("Bar")
    val node3 = createLabeledNode("Bar")
    relate(node0, node3, "KNOWS")

    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON CREATE SET b:Foo RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, optimalEagerCount = 0, expectPlansToFailPredicate = Configs.AllRulePlanners - Configs.Rule2_3))

    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result, relationshipsCreated = 3, labelsAdded = 2)
    //TODO this we need to consider not only overlap but also what known labels the node we set has
  }

  test("should introduce eagerness when the ON CREATE includes writing to a right-side matched label") {
    val node0 = createLabeledNode("Foo")
    val node1 = createLabeledNode("Foo")
    val node2 = createLabeledNode("Bar")
    val node3 = createLabeledNode("Bar")
    relate(node0, node3, "KNOWS")

    val query = "MATCH (a:Foo), (b:Bar) MERGE (a)-[r:KNOWS]->(b) ON CREATE SET a:Bar RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))

    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result, relationshipsCreated = 3, labelsAdded = 2)
  }

  test("should not add eagerness when reading and merging nodes and relationships when matching different label") {
    createLabeledNode("A")
    val query = "MATCH (a:A) MERGE (a)-[:BAR]->(b:B) WITH a MATCH (a) WHERE (a)-[:FOO]->() RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 0
    assertStats(result, relationshipsCreated = 1, nodesCreated = 1, labelsAdded = 1)
  }

  test("should add eagerness when reading and merging nodes and relationships on matching same label") {
    val node0 = createLabeledNode("A")
    val node1 = createLabeledNode("A")
    val node2 = createLabeledNode("A")
    relate(node1, node2, "BAR")

    val query = "MATCH (a:A) MERGE (a)-[:BAR]->(b:A) WITH a MATCH (a2) RETURN count (a2) AS nodes"

    val result = executeWith(Configs.UpdateConf, query, expectedDifferentResults = Configs.Rule2_3,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))

    assertStats(result, nodesCreated = 2, relationshipsCreated = 2, labelsAdded = 2)
    result.toList should equal(List(Map("nodes" -> 15)))
  }

  test("should not add eagerness when reading nodes and merging relationships") {
    createLabeledNode("A")
    createLabeledNode("B")
    val query = "MATCH (a:A), (b:B) MERGE (a)-[:BAR]->(b) WITH a MATCH (a) WHERE (a)-[:FOO]->() RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 0
    assertStats(result, relationshipsCreated = 1)
  }

  test("never ending query should end - this is the query that prompted Eagerness in the first place") {
    createNode()
    val query = "MATCH (a) CREATE ()"
    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    assertStats(result, nodesCreated = 1)
  }

  test("should be eager when combining MATCH, MERGE, CREATE with UNWIND") {
    createNode()
    createNode()

    val query = "UNWIND range(0, 9) AS i MATCH (x) MERGE (m {v: i % 2}) ON CREATE SET m:Merged CREATE ({v: (i + 1) % 2}) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2))

    result.columnAs[Long]("count(*)").next shouldBe 20
    assertStats(result, nodesCreated = 22, propertiesWritten = 22, labelsAdded = 2)
  }

  test("should be eager when combining MATCH, MATCH, CREATE with UNWIND") {
    createNode("v" -> 1)
    createNode()

    val query = "UNWIND range(0, 9) AS i MATCH (x) MATCH (m {v: i % 2}) CREATE ({v: (i + 1) % 2}) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))

    result.columnAs[Long]("count(*)").next shouldBe 10
    assertStats(result, nodesCreated = 10, propertiesWritten = 10)
  }

  test("should be eager when combining MATCH, CREATE, MERGE with UNWIND") {
    createNode()
    createNode()

    val query = "UNWIND range(0, 9) AS i MATCH (x) WITH * CREATE ({v: i % 2}) MERGE (m {v: (i + 1) % 2}) ON CREATE SET m:Merged RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2))

    result.columnAs[Long]("count(*)").next shouldBe 200
    assertStats(result, nodesCreated = 20, propertiesWritten = 20, labelsAdded = 0)
  }

  // TESTS WITH MULTIPLE MERGES

  test("should not be eager when merging on two different labels") {
    val query = "MERGE(:L1) MERGE(p:L2) ON CREATE SET p.name = 'Blaine' RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, nodesCreated = 2, propertiesWritten = 1, labelsAdded = 2)
  }

  ignore("does not need to be eager when merging on the same label, merges match") {
    createLabeledNode("L1")
    createLabeledNode("L1")
    val query = "MERGE(:L1) MERGE(p:L1) ON CREATE SET p.name = 'Blaine' RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result)
  }

  ignore("does not need to be eager when merging on the same label, merges create") {
    createNode()
    val query = "MERGE(:L1) MERGE(p:L1) ON CREATE SET p.name = 'Blaine' RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, nodesCreated = 1, labelsAdded = 1)
  }

  ignore("does not need to be eager when right side creates nodes for left side, merges match") {
    createNode()
    createLabeledNode("Person")
    val query = "MERGE() MERGE(p: Person) ON CREATE SET p.name = 'Blaine' RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 2
    assertStats(result)
  }

  ignore("does not need to be eager when right side creates nodes for left side, 2nd merge create") {
    createNode()
    createNode()
    val query = "MERGE() MERGE(p: Person) ON CREATE SET p.name = 'Blaine' RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 2
    assertStats(result, nodesCreated = 1, labelsAdded = 1, propertiesWritten = 1)
  }

  ignore("does not need to be eager when no merge has labels, merges match") {
    createNode()
    val query = "MERGE() MERGE(p) ON CREATE SET p.name = 'Blaine' RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result)
  }

  ignore("does not need to be eager when no merge has labels, merges create") {
    val query = "MERGE() MERGE(p) ON CREATE SET p.name = 'Blaine' RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, nodesCreated = 1)
  }

  test("Multiple single node merges building on each other through property values should be eager") {
    val query = "MERGE(a {p: 1}) MERGE(b {p: a.p}) MERGE(c {p: b.p}) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2, Configs.AllRulePlanners - Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, nodesCreated = 1, propertiesWritten = 1)
  }

  test("Multiple single node merges should be eager") {
    val query = "UNWIND [0, 1] AS i MERGE (a {p: i % 2}) MERGE (b {p: (i + 1) % 2}) ON CREATE SET b:ShouldNotBeSet RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))

    result.columnAs[Long]("count(*)").next shouldBe 2
    assertStats(result, nodesCreated = 2, propertiesWritten = 2, labelsAdded = 0)
  }

  test("should not be eager when merging on already bound variables") {
    val query = "MERGE (city:City) MERGE (country:Country) MERGE (city)-[:IN]->(country) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))

    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, nodesCreated = 2, labelsAdded = 2, relationshipsCreated = 1)
  }

  test("should not use eager if on create modifies relationships which don't affect the match clauses") {
    createLabeledNode("LeftLabel")
    createLabeledNode("RightLabel")
    val query =
      """MATCH (src:LeftLabel), (dst:RightLabel)
        |MERGE (src)-[r:IS_RELATED_TO ]->(dst)
        |ON CREATE SET r.p3 = 42""".stripMargin
    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))

    assertStats(result, relationshipsCreated = 1, propertiesWritten = 1)
  }

  // TESTS FOR SET

  test("matching property and writing different property should not be eager") {
    createLabeledNode(Map("prop" -> 5), "Node")
    val query = "MATCH (n:Node {prop:5}) SET n.value = 10 RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, propertiesWritten = 1)
  }

  test("matching label and setting different label should not be eager") {
    createLabeledNode(Map("prop" -> 5), "Node")
    val query = "MATCH (n:Node) SET n:Lol RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, labelsAdded = 1)
  }

  test("matching label and setting same label should not be eager") {
    createLabeledNode(Map("prop" -> 5), "Lol")
    val query = "MATCH (n:Lol) SET n:Lol RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, labelsAdded = 0)
  }

  test("matching label on right-hand side and setting same label should be eager and get the count right") {
    createLabeledNode("Two")
    createLabeledNode("Two")
    createNode()
    val query = "MATCH (m1:Two), (m2:Two), (n) SET n:Two RETURN count(*) AS c"

    val result: InternalExecutionResult = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, labelsAdded = 1)
    result.toList should equal(List(Map("c" -> 12)))
  }

  test("matching label on right-hand side and setting different label should not be eager") {
    createLabeledNode("Lol")
    createNode()
    val query = "MATCH (n), (m1:Lol), (m2:Lol) SET n:Rofl RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 2
    assertStats(result, labelsAdded = 2)
  }

  test("setting label in tail should be eager if overlap") {
    createNode()
    createNode()
    createLabeledNode("Foo")
    createLabeledNode("Foo")
    val query = "MATCH (n) CREATE (m) WITH * MATCH (o:Foo) SET n:Foo RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 8
    assertStats(result, labelsAdded = 2, nodesCreated = 4)
  }

  test("setting label in tail should be eager if overlap within tail") {
    createLabeledNode("A")
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("B")
    val query = "MATCH (n:A) CREATE (m:C) WITH * MATCH (o:B), (p:C) SET p:B RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query, expectedDifferentResults = Configs.Rule2_3,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2, Configs.AllRulePlanners))
    result.columnAs[Long]("count(*)").next shouldBe 8
    assertStats(result, labelsAdded = 4, nodesCreated = 2)
  }

  test("setting label in tail should not be eager if no overlap") {
    createLabeledNode("Foo")
    createLabeledNode("Foo")
    createLabeledNode("Bar")
    createLabeledNode("Bar")
    val query = "MATCH (n) CREATE (m) WITH * MATCH (o:Bar) SET n:Foo RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 8
    assertStats(result, labelsAdded = 2, nodesCreated = 4)
  }

  test("matching property and setting label should not be eager") {
    createNode(Map("name" -> "thing"))
    val query = "MATCH (n {name : 'thing'}) SET n:Lol RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, labelsAdded = 1)
  }

  test("single simple match followed by set property should not be eager") {
    createNode()
    val query = "MATCH (n) SET n.prop = 5 RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, propertiesWritten = 1)
  }

  test("single property match followed by set property should not be eager") {
    createNode(Map("prop" -> 20))
    val query = "MATCH (n { prop: 20 }) SET n.prop = 10 RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, propertiesWritten = 1)
  }

  test("single label match followed by set property should not be eager") {
    createLabeledNode("Node")
    val query = "MATCH (n:Node) SET n.prop = 10 RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, propertiesWritten = 1)
  }

  test("single label+property match followed by set property should not be eager") {
    val query = "MATCH (n:Node {prop:5}) SET n.prop = 10 RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 0
  }

  test("matching property via index and writing same property should not be eager") {
    graph.createConstraint("Book", "isbn")
    createLabeledNode(Map("isbn" -> "123"), "Book")

    val query = "MATCH (b :Book {isbn : '123'}) SET b.isbn = '456' RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))

    result.columnAs[Long]("count(*)").next shouldBe 1
  }

  test("matching property via index and writing same property should be eager") {
    graph.createConstraint("Book", "isbn")
    createLabeledNode(Map("isbn" -> "123"), "Book")

    val query = "MATCH (a), (b :Book {isbn : '123'}) SET a.isbn = '456' RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))

    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, propertiesWritten = 1)
  }

  test("match property on right-side followed by property write on left-side match needs eager") {
    createNode()
    createNode(Map("id" -> 0))
    val query = "MATCH (a),(b {id: 0}),(c {id: 0}) SET a.id = 0 RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next shouldBe 2
    assertStats(result, propertiesWritten = 2)
  }

  test("match property on right-side followed by property write on right-side match needs eager") {
    createNode()
    createNode(Map("id" -> 0))
    val query = "MATCH (a),(b {id: 0}),(c {id: 0}) SET c.id = 1 RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next shouldBe 2
    assertStats(result, propertiesWritten = 2)
  }

  test("match property on left-side followed by property write does not need eager") {
    createNode()
    createNode(Map("id" -> 0))

    val query = "MATCH (b {id: 0}) SET b.id = 1 RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, propertiesWritten = 1)
  }

  test("matching node property, writing relationship property should not be eager") {
    relate(createNode(Map("prop" -> 5)), createNode())

    val query = "MATCH (n {prop : 5})-[r]-() SET r.prop = 6 RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))

    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, propertiesWritten = 1)
  }

  test("matching node property, writing same node property should be eager") {
    relate(createNode(Map("prop" -> 5)), createNode())

    val query = "MATCH (n {prop : 5})-[r]-(m) SET m.prop = 5 RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching relationship property, writing same relationship property should be eager") {
    relate(createNode(), createNode(), "prop" -> 3)
    val query = "MATCH ()-[r {prop : 3}]-() SET r.prop = 6 RETURN count(*) AS c"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))

    assertStats(result, propertiesWritten = 2)
    result.toList should equal(List(Map("c" -> 2)))

  }

  test("matching relationship property, writing different relationship property should not be eager") {
    relate(createNode(), createNode(), "prop1" -> 3)

    val query = "MATCH ()-[r {prop1 : 3}]-() SET r.prop2 = 6 RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))

    result.columnAs[Long]("count(*)").next shouldBe 2
    assertStats(result, propertiesWritten = 2)
  }

  test("matching relationship property, writing node property should not be eager") {
    relate(createNode(), createNode(), "prop" -> 3)
    val query = "MATCH (n)-[r {prop : 3}]-() SET n.prop = 6 RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 2
    assertStats(result, propertiesWritten = 2)
  }

  test("matching on relationship property existence, writing same property doesn't have to be eager but still is") {
    relate(createNode(), createNode(), "prop" -> 42)
    relate(createNode(), createNode())

    val query = "MATCH ()-[r]-() WHERE exists(r.prop) SET r.prop = 'foo' RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query)

    result.columnAs[Long]("count(*)").next shouldBe 2
    assertStats(result, propertiesWritten = 2)
  }

  test("matching on relationship property existence, writing same property to null should be eager") {
    relate(createNode(), createNode(), "prop" -> 42)
    relate(createNode(), createNode())

    val query = "MATCH ()-[r]-() WHERE exists(r.prop) SET r.prop = {null} RETURN count(*)"

    val buggyVersions =  Configs.Cost3_3 + Configs.Cost3_1 + Configs.DefaultRule + Configs.Rule3_1
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query, params = Map("null" -> null),
                             expectedDifferentResults = buggyVersions,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1, buggyVersions))

    result.columnAs[Long]("count(*)").next shouldBe 2
    //we are "smart" enough to only remove property once
    assertStats(result, propertiesWritten = 1)
  }

  test("matching on relationship property existence, removing same property should be eager") {
    relate(createNode(), createNode(), "prop" -> 42)
    relate(createNode(), createNode())

    val query = "MATCH ()-[r]-() WHERE exists(r.prop) REMOVE r.prop  RETURN count(*)"
    val buggyVersions =  Configs.Cost3_3 + Configs.Cost3_1 + Configs.DefaultRule + Configs.Rule3_1
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query, params = Map("null" -> null),
                            expectedDifferentResults = buggyVersions,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1, buggyVersions))

    result.columnAs[Long]("count(*)").next shouldBe 2
    //we are "smart" enough to only remove property once
    assertStats(result, propertiesWritten = 1)
  }

  test("matching on relationship property with property predicate, writing same property should be eager") {
    relate(createNode(), createNode(), "prop" -> 42)
    relate(createNode(), createNode())

    val query = "MATCH ()-[r]-() WHERE r.prop = 42 SET r.prop = 'foo' RETURN count(*)"

    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1))

    result.columnAs[Long]("count(*)").next shouldBe 2
    assertStats(result, propertiesWritten = 2)
  }

  test("matching on relationship property existence, writing different property should not be eager") {
    relate(createNode(), createNode(), "prop1" -> 42)
    relate(createNode(), createNode())

    val query = "MATCH ()-[r]-() WHERE exists(r.prop1) SET r.prop2 = 'foo'"

    assertStats(executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0)), propertiesWritten = 2)
  }

  test("matching two relationships, writing one property should be eager") {
    val l = createLabeledNode("L")
    val a = createNode()
    relate(a, l, "prop" -> 42)
    relate(l, a)

    val query = "MATCH ()-[r {prop: 42}]-(), (:L)-[r2]-() SET r2.prop = 42"

    assertStats(executeWith(Configs.UpdateConf, query, planComparisonStrategy = testEagerPlanComparisonStrategy(1)), propertiesWritten = 2)

  }

  test("setting property in tail should be eager if overlap") {
    createNode()
    createNode()
    createNode("prop" -> 42)
    createNode("prop" -> 42)
    val query = "MATCH (n) CREATE (m) WITH * MATCH (o {prop:42}) SET n.prop = 42 RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, Configs.AllRulePlanners))
    result.columnAs[Int]("count").next should equal(8)
    assertStats(result, propertiesWritten = 8, nodesCreated = 4)
  }

  test("setting property in tail should be eager if overlap head") {
    createNode()
    createNode()
    createNode("prop" -> 42)
    createNode("prop" -> 42)
    val query = "MATCH (n {prop: 42}) CREATE (m) WITH * MATCH (o) SET n.prop = 42 RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query, expectedDifferentResults = Configs.Rule2_3,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Int]("count").next should equal(12)
    assertStats(result, propertiesWritten = 12, nodesCreated = 2)
  }

  test("setting property in tail should be eager if overlap first tail") {
    createNode()
    createNode()
    createNode("prop" -> 42)
    createNode("prop" -> 42)
    val query =
      """CREATE ()
        |WITH *
        |MATCH (n {prop: 42})
        |CREATE (m)
        |WITH *
        |MATCH (o)
        |SET m.prop = 42
        |RETURN count(*) as count""".stripMargin

    val result = executeWith(Configs.UpdateConf, query, expectedDifferentResults = Configs.Rule2_3,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, Configs.AllRulePlanners - Configs.Rule2_3))
    result.columnAs[Int]("count").next should equal(14)
    assertStats(result, propertiesWritten = 14, nodesCreated = 3)
  }

  test("setting property in tail should not be eager if no overlap") {
    createNode()
    createNode()
    createNode("prop" -> 42)
    createNode("prop" -> 42)
    val query = "MATCH (n) CREATE (m) WITH * MATCH (o {prop:42}) SET n.prop2 = 42 RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.AllRulePlanners))
    result.columnAs[Int]("count").next should equal(8)
    assertStats(result, propertiesWritten = 8, nodesCreated = 4)
  }

  test("matching node property, writing with += should be eager") {
    relate(createNode(Map("prop" -> 5)), createNode())
    val query = "MATCH (n {prop : 5})-[r]-(m) SET m += {prop: 5} RETURN count(*)"
    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching node property, writing with += should not be eager when we can avoid it") {
    relate(createNode(Map("prop" -> 5)), createNode())
    val query = "MATCH (n {prop : 5})-[r]-(m) SET m += {prop2: 5} RETURN count(*)"
    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.AllRulePlanners))
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching node property, writing with += should be eager when using parameters") {
    val s = createNode(Map("prop" -> 5))
    val e = createNode()
    relate(s, e)
    relate(e, s)

    val query = "MATCH (n {prop : 5})-[r]->(m) SET m += {props} RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1), params = Map("props" -> Map("prop" -> 5)))
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching rel property, writing with += should not be eager when we can avoid it") {
    relate(createNode(Map("prop" -> 5)), createNode())
    val query = "MATCH (n {prop : 5})-[r]-(m) SET m += {prop2: 5} RETURN count(*)"
    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.AllRulePlanners))
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  //REMOVE LABEL
  test("matching label and removing different label should not be eager") {
    createLabeledNode(Map("prop" -> 5), "Node", "Lol")
    val query = "MATCH (n:Node) REMOVE n:Lol"

    assertStats(executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0)), labelsRemoved = 1)
  }

  test("matching label and removing same label should not be eager") {
    createLabeledNode(Map("prop" -> 5), "Node")
    val query = "MATCH (n:Node) REMOVE n:Node"

    assertStats(executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3)), labelsRemoved = 1)
  }

  test("should not be eager if removing label from left-most node") {
    createLabeledNode("Lol")
    createNode()
    val query = "MATCH (m:Lol), (n) REMOVE n:Lol RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))

    assertStats(result, labelsRemoved = 1)
    result.columnAs[Long]("count(*)").next shouldBe 2
  }

  test("should be eager if removing a label that's matched by an unstable iterator") {
    createLabeledNode("Lol")
    createLabeledNode("Lol")
    val query = "MATCH (m:Lol), (n:Lol) REMOVE m:Lol RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))

    assertStats(result, labelsRemoved = 2)
    result.columnAs[Long]("count(*)").next shouldBe 4
  }

  test("remove same label without anything else reading it should not be eager") {
    createLabeledNode("A")
    createLabeledNode("B")
    createLabeledNode("C")
    val query = "MATCH  (m1:A), (m2:B), (n:C) REMOVE n:C RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.AllRulePlanners))
    result.columnAs[Long]("count(*)").next shouldBe 1
    assertStats(result, labelsRemoved = 1)
  }

  test("matching label on right-hand side and removing same label should be eager and get the count right") {
    createLabeledNode("Two")
    createLabeledNode("Two")
    createNode()
    val query = "MATCH (m1:Two), (m2:Two), (n) REMOVE n:Two RETURN count(*) AS c"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, labelsRemoved = 2)
    result.toList should equal(List(Map("c" -> 12)))
  }

  test("matching label on right-hand side and removing different label should not be eager") {
    createLabeledNode("A", "B")
    createLabeledNode("A", "B")
    createLabeledNode("B")
    val query = "MATCH (n), (m1:A), (m2:A) REMOVE n:B RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 12
    assertStats(result, labelsRemoved = 3)
  }

  test("removing label in tail should be eager if overlap") {
    createLabeledNode("Foo")
    createLabeledNode("Foo")
    val query = "MATCH (n) CREATE (m) WITH * MATCH (o:Foo) REMOVE n:Foo RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result, labelsRemoved = 2, nodesCreated = 2)
  }

  test("removing label in tail should be eager if overlap, creating matching nodes in between") {
    createLabeledNode("Foo")
    createLabeledNode("Foo")
    val query = "MATCH (n) CREATE (m:Foo) WITH * MATCH (o:Foo) REMOVE n:Foo RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query, expectedDifferentResults = Configs.Rule2_3,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 8
    assertStats(result, labelsAdded = 2, labelsRemoved = 2, nodesCreated = 2)
  }

  test("removing label in tail should not be eager if no overlap") {
    createLabeledNode("Foo")
    createLabeledNode("Foo")
    createLabeledNode("Bar")
    createLabeledNode("Bar")
    val query = "MATCH (n) CREATE (m) WITH * MATCH (o:Bar) REMOVE n:Foo RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next shouldBe 8
    assertStats(result, labelsRemoved = 2, nodesCreated = 4)
  }

  test("undirected expand followed by remove label needn't to be eager") {
    relate(createLabeledNode("Foo"), createLabeledNode("Foo"))
    relate(createLabeledNode("Foo"), createLabeledNode("Foo"))

    val query = "MATCH (n:Foo)--(m) REMOVE m:Foo RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next shouldBe 4
    assertStats(result, labelsRemoved = 4)
  }

  // UNWIND TESTS

  test("eagerness should work with match - unwind - delete") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")

    // Relationship match is non-directional, so should give 2 rows
    val query = "MATCH (a)-[t:T]-(b) UNWIND [1] as i DELETE t RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, relationshipsDeleted = 1)
    // this assertion depends on unnestApply and cleanUpEager
  }

  test("eagerness should work with match - unwind - delete with preceding projection") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")

    // Relationship match is non-directional, so should give 2 rows
    val query = "CREATE () WITH * MATCH (a)-[t:T]-(b) UNWIND [1] as i DELETE t RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, nodesCreated = 1, relationshipsDeleted = 1)
    // this assertion depends on unnestApply and cleanUpEager
  }

  test("eagerness should work with match - unwind - delete with multiple preceding projections") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")

    // Relationship match is non-directional, so should give 2 rows
    val query = "CREATE () WITH * CREATE () WITH * MATCH (a)-[t:T]-(b) UNWIND [1] as i DELETE t RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2, optimalEagerCount = 1, expectPlansToFailPredicate = Configs.Rule2_3))
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, nodesCreated = 2, relationshipsDeleted = 1)
    // this assertion depends on unnestApply and cleanUpEager
  }

  test("should be eager between conflicting read/write separated by empty UNWIND") {
    createNode()
    createNode()

    val query = "MATCH (), () UNWIND [] AS i CREATE () RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next() should equal(0)
    assertStats(result, nodesCreated = 0)
  }

  test("should be eager between conflicting read/write separated by UNWIND of one") {
    createNode()
    createNode()

    val query = "MATCH (), () UNWIND [0] AS i CREATE () RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next() should equal(4)
    assertStats(result, nodesCreated = 4)
  }

  test("should be eager between conflicting read/write separated by UNWIND of two") {
    createNode()
    createNode()

    val query = "MATCH (), () UNWIND [0, 0] AS i CREATE () RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next() should equal(8)
    assertStats(result, nodesCreated = 8)
  }

  test("should be eager between conflicting read/write separated by UNWIND between reads") {
    createNode()
    createNode()

    val query = "MATCH () UNWIND [0] as i MATCH () CREATE () RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next() should equal(4)
    assertStats(result, nodesCreated = 4)
  }

  test("should be eager between conflicting read/write separated by UNWIND between reads and writes -- MATCH") {
    createNode()
    createNode()

    val query = "MATCH () UNWIND [0] as i MATCH () UNWIND [0] as j CREATE () RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next() should equal(4)
    assertStats(result, nodesCreated = 4)
  }

  test("should be eager between conflicting read/write separated by UNWIND between reads and writes -- MERGE") {
    createNode()
    createNode()

    val query = "MERGE () WITH * UNWIND [0] as i MATCH () UNWIND [0] as j CREATE () RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2, optimalEagerCount = 1, expectPlansToFailPredicate = Configs.AllRulePlanners))
    result.columnAs[Long]("count(*)").next() should equal(4)
    assertStats(result, nodesCreated = 4)
  }

  // FOREACH TESTS
  test("should be eager between conflicting read and write inside FOREACH") {
    createNode()
    createNode()

    //val query = "UNWIND [0] as u MATCH (a), (b) FOREACH(i in range(0, 1) | DELETE a) RETURN count(*)"
    val query = "MATCH (a), (b) FOREACH(i in range(0, 1) | DELETE a) RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next() should equal(4)
    assertStats(result, nodesDeleted = 2)
  }

  test("should be eager between conflicting CREATE and MERGE node with FOREACH") {
    createLabeledNode("A")
    createLabeledNode("A")

    val query =
      """MATCH (a:A)
        |CREATE (b:B)
        |FOREACH(i in range(0, 1) |
        |  MERGE (b2:B)
        |  ON MATCH SET b2.matched = true
        |)
        |RETURN count(*)""".stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next() should equal(2)
    assertStats(result, nodesCreated = 2, labelsAdded = 2, propertiesWritten = 8)
  }

  test("should be eager between DELETE and MERGE node with FOREACH") {
    createLabeledNode(Map("value" -> 0, "deleted" -> true), "B")
    createLabeledNode(Map("value" -> 1, "deleted" -> true), "B")

    val query =
      """
        |MATCH (b:B)
        |DELETE b
        |FOREACH (i in [0] |
        |  MERGE (b2:B { value: 1, deleted: true })
        |  ON MATCH
        |    SET b2.matched = true
        |  ON CREATE
        |    SET b2.deleted = false
        |)
        |RETURN count(*)
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, Configs.Rule2_3))
    assertStats(result, nodesCreated = 2, nodesDeleted = 2, propertiesWritten = 6, labelsAdded = 2)
    result.columnAs[Long]("count(*)").next shouldBe 2
  }

  test("should not be eager between conflicting CREATE and MERGE node with nested FOREACH") {
    createLabeledNode("A")
    createLabeledNode("A")

    val query =
      """MATCH (a:A)
        |FOREACH (i in [0] |
        |  CREATE (b:B)
        |  FOREACH(j in range(0, 1) |
        |    MERGE (b2:B)
        |    ON MATCH SET b2.matched = true
        |  )
        |)
        |RETURN count(*)""".stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0, Configs.Rule2_3))
    result.columnAs[Long]("count(*)").next() should equal(2)
    assertStats(result, nodesCreated = 2, labelsAdded = 2, propertiesWritten = 6)
  }

  test("should be eager between conflicting MERGEs in nested FOREACH") {
    createLabeledNode("A")
    createLabeledNode("A")

    val query =
      """MATCH (a:A)
        |FOREACH (i in [0] |
        |  MERGE (:A)
        |  CREATE (:B)
        |  FOREACH(j in range(0, 1) |
        |    MERGE (b:B)
        |    ON MATCH SET b.matched = true
        |  )
        |)
        |RETURN count(*)""".stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2, Configs.AllRulePlanners))
    result.columnAs[Long]("count(*)").next() should equal(2)
    assertStats(result, nodesCreated = 4, labelsAdded = 4, propertiesWritten = 24)
  }

  test("should not be eager between non-conflicting updates in nested FOREACH") {
    createLabeledNode("A")

    val query =
      """MATCH (a:A)
        |FOREACH (i in [0] |
        |  CREATE (:B)
        |  FOREACH(j in range(0, 1) |
        |    CREATE (:B)
        |  )
        |  CREATE (:B)
        |)
        |RETURN count(*)""".stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(0))
    result.columnAs[Long]("count(*)").next() should equal(1)
    assertStats(result, nodesCreated = 4, labelsAdded = 4)
  }

  // LOAD CSV
  test("should not be eager for LOAD CSV followed by MERGE") {
    val query = "LOAD CSV FROM 'file:///something' AS line MERGE (b:B {p:line[0]}) RETURN b"
    executeWith(Configs.UpdateConf, "EXPLAIN " + query, planComparisonStrategy = testEagerPlanComparisonStrategy(0))
  }

  test("eagerness should work with match - load csv - delete") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")

    val url = createCSVTempFileURL {
      writer =>
        writer.println("something")
    }

    val query = s"MATCH (a)-[t:T]-(b) LOAD CSV FROM '$url' AS line DELETE t RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, relationshipsDeleted = 1)
    // this assertion depends on unnestApply and cleanUpEager
  }

  test("eagerness should work with match - load csv - delete with preceding projection") {
    val a = createNode()
    val b = createNode()
    relate(a, b, "T")

    val url = createCSVTempFileURL {
      writer =>
        writer.println("something")
    }

    val query = s"CREATE () WITH * MATCH (a)-[t:T]-(b) LOAD CSV FROM '$url' AS line DELETE t RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Int]("count").next should equal(2)
    assertStats(result, nodesCreated = 1, relationshipsDeleted = 1)
    // this assertion depends on unnestApply and cleanUpEager
  }

  test("should be eager between conflicting read/write separated by LOAD CSV between reads and writes -- MATCH") {
    createNode()
    createNode()

    val url = createCSVTempFileURL {
      writer =>
        writer.println("something")
    }

    val query = s"MATCH () LOAD CSV FROM '$url' AS i MATCH () UNWIND [0] as j CREATE () RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.columnAs[Long]("count(*)").next() should equal(4)
    assertStats(result, nodesCreated = 4)
  }

  test("should be eager between conflicting read/write separated by LOAD CSV between reads and writes -- MERGE") {
    createNode()
    createNode()

    val url = createCSVTempFileURL {
      writer =>
        writer.println("something")
    }

    val query = s"MERGE () WITH * LOAD CSV FROM '$url' AS line MATCH () LOAD CSV FROM '$url' AS line2 CREATE () RETURN count(*)"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2, optimalEagerCount = 1, expectPlansToFailPredicate = Configs.AllRulePlanners))
    result.columnAs[Long]("count(*)").next() should equal(4)
    assertStats(result, nodesCreated = 4)
  }

  test("should always be eager after deleted relationships if there are any subsequent expands that might load them") {
    val device = createLabeledNode("Device")
    val cookies = (0 until 2).foldLeft(Map.empty[String, Node]) { (nodes, index) =>
      val name = s"c$index"
      val cookie = createLabeledNode(Map("name" -> name), "Cookie")
      relate(device, cookie)
      relate(cookie, createNode())
      nodes + (name -> cookie)
    }

    val query =
      """
        |MATCH (c:Cookie {name: {cookie}})<-[r2]-(d:Device)
        |WITH c, d
        |MATCH (c)-[r]-()
        |DELETE c, r
        |WITH d
        |MATCH (d)-->(c2:Cookie)
        |RETURN d, c2""".stripMargin

    cookies.foreach { case (name, node) =>
      val result = executeWith(Configs.UpdateConf, query,
        planComparisonStrategy = testEagerPlanComparisonStrategy(2),
        params = Map("cookie" -> name))
      assertStats(result, nodesDeleted = 1, relationshipsDeleted = 2)
    }
  }

  test("should always be eager after deleted nodes if there are any subsequent matches that might load them") {
    val cookies = (0 until 2).foldLeft(Map.empty[String, Node]) { (nodes, index) =>
      val name = s"c$index"
      val cookie = createLabeledNode(Map("name" -> name), "Cookie")
      nodes + (name -> cookie)
    }

    val query = "MATCH (c:Cookie) DELETE c WITH 1 as t MATCH (x:Cookie) RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, Configs.Rule2_3))

    result.columnAs[Int]("count").next should equal(0)
    assertStats(result, nodesDeleted = 2)
  }

  test("should always be eager after deleted paths if there are any subsequent matches that might load them") {
    val cookies = (0 until 2).foldLeft(Map.empty[String, Node]) { (nodes, index) =>
      val name = s"c$index"
      val cookie = createLabeledNode(Map("name" -> name), "Cookie")
      nodes + (name -> cookie)
    }

    val query = "MATCH p=(:Cookie) DELETE p WITH 1 as t MATCH (x:Cookie) RETURN count(*) as count"

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, Configs.Rule2_3))

    result.columnAs[Int]("count").next should equal(0)
    assertStats(result, nodesDeleted = 2)
  }

  test("should be eager with labels function before set label") {
    val n1 = createNode()
    val n2 = createNode()
    relate(n1, n2)

    val query =
      """MATCH (n)--(m)
        |WITH labels(n) AS labels, m
        |SET m:Foo
        |RETURN labels
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query, expectedDifferentResults = Configs.Rule2_3,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, Configs.Rule2_3))
    result.toList should equal(List(Map("labels" -> List()), Map("labels" -> List())))
    assertStats(result, labelsAdded = 2)
  }

  test("should be eager with labels function before remove label") {
    val n1 = createLabeledNode("Foo")
    val n2 = createLabeledNode("Foo")
    relate(n1, n2)

    val query =
      """MATCH (n)--(m)
        |WITH labels(n) AS labels, m
        |REMOVE m:Foo
        |RETURN labels
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query, expectedDifferentResults = Configs.Rule2_3,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, Configs.Rule2_3))
    result.toList should equal(List(Map("labels" -> List("Foo")), Map("labels" -> List("Foo"))))
    assertStats(result, labelsRemoved = 2)
  }

  test("should be eager with labels function after set label") {
    val n1 = createNode()
    val n2 = createNode()
    relate(n1, n2)

    val query =
      """MATCH (n)--(m)
        |SET n:Foo
        |RETURN labels(n), labels(m)
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query, expectedDifferentResults = Configs.Rule2_3,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, Configs.Rule2_3))
    result.toList should equal(List(Map("labels(n)" -> List("Foo"), "labels(m)" -> List("Foo")),
      Map("labels(n)" -> List("Foo"), "labels(m)" -> List("Foo"))))
    assertStats(result, labelsAdded = 2)
  }

  test("should be eager with labels function after merge on match set label") {
    val n1 = createNode()
    val n2 = createNode()
    relate(n1, n2, "T")
    relate(n2, n1, "T")

    val query =
      """MERGE (n)-[:T]->(m)
        |ON MATCH SET n:Foo
        |RETURN labels(n), labels(m)
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, Configs.Rule2_3))
    result.toList should equal(List(Map("labels(n)" -> List("Foo"), "labels(m)" -> List("Foo")),
      Map("labels(n)" -> List("Foo"), "labels(m)" -> List("Foo"))))
    assertStats(result, labelsAdded = 2)
  }

  test("should be eager with labels function after remove label") {
    val n1 = createLabeledNode("Foo")
    val n2 = createLabeledNode("Foo")
    relate(n1, n2)

    val query =
      """MATCH (n)--(m)
        |REMOVE n:Foo
        |RETURN labels(n), labels(m)
      """.stripMargin

    val result = executeWith(Configs.UpdateConf, expectedDifferentResults = Configs.Rule2_3, query = query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, Configs.Rule2_3))
    result.toList should equal(List(Map("labels(n)" -> List(), "labels(m)" -> List()),
      Map("labels(n)" -> List(), "labels(m)" -> List())))
    assertStats(result, labelsRemoved = 2)
  }

  test("merge and pattern comprehension readwrite conflict requires eager") {
    val query =
      """
        |CREATE (node)
        |WITH node
        |UNWIND [{ value: 'apples' }, { value: 'oranges' }] as tags
        |MERGE (tag { value: tags.value})
        |MERGE (node)-[:HAS_TAG {relProp1: 'relProp1', relProp2: 'relProp2'}]->(tag)
        |WITH DISTINCT node
        |WITH [(node)-[:HAS_TAG {relProp1: 'relProp1', relProp2: 'relProp2'}]->(t) | t.value] as tags
        |RETURN size(tags) as nbrTags
      """.stripMargin

    val nonBugFixedConfig = Configs.Cost3_1

    val result = executeWith(Configs.UpdateConf - Configs.AllRulePlanners, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(1, nonBugFixedConfig),
      expectedDifferentResults = nonBugFixedConfig)

    result.toList should equal(List(Map("nbrTags" -> 2)))
  }

  test("merge and optional match readwrite conflict requires eager") {
    val query =
      """
        |CREATE (node:Node {id: 'xyzzy'})
        |WITH node
        |UNWIND [{ value: 'apples' }, { value: 'oranges' }] as tags
        |MERGE (tag:Tag { value: tags.value})
        |MERGE (node)-[:HAS_TAG]->(tag)
        |WITH DISTINCT node
        |OPTIONAL MATCH (node)-[:HAS_TAG]->(t:Tag)
        |RETURN COUNT(t.value) as nbrTags
      """.stripMargin

    val nonBugFixedConfig = Configs.Cost3_1 + Configs.AllRulePlanners

    val result = executeWith(Configs.UpdateConf, query,
      planComparisonStrategy = testEagerPlanComparisonStrategy(2, nonBugFixedConfig),
      expectedDifferentResults = nonBugFixedConfig)

    result.toList should equal(List(Map("nbrTags" -> 2)))
  }

  test("matching node property using integer equality and writing other node should be eager") {
    relate(createNode(Map("prop" -> 5)), createNode())
    val query = "MATCH (n)-[r]-(m) WHERE n.prop = 5 SET m.prop = 5 RETURN count(*)"
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching node property using string equality and writing other node should be eager") {
    relate(createNode(Map("prop" -> "5")), createNode())
    val query = "MATCH (n)-[r]-(m) WHERE n.prop = '5' SET m.prop = '5' RETURN count(*)"
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching node property using or'd string equality and writing other node should be eager 2") {
    relate(createNode(Map("prop" -> "5")), createNode())
    val query = "MATCH (n)-[r]-(m) WHERE n.prop = '5' OR n.prop = '6' SET m.prop = '5' RETURN count(*)"
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching node property using string equality anded with other predicate and writing other node should be eager 2") {
    relate(createNode(Map("prop" -> "5")), createNode())
    val query = "MATCH (n)-[r]-(m) WHERE n.prop = '5' AND n.prop <> '6' SET m.prop = '5' RETURN count(*)"
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching node property using string unequality and writing other node should be eager") {
    relate(createNode(Map("prop" -> "5")), createNode())
    val query = "MATCH (n)-[r]-(m) WHERE n.prop <> '6' SET m.prop = '5' RETURN count(*)"
    val buggyVersions = Configs.Cost3_3 + Configs.Cost3_1
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             expectedDifferentResults = buggyVersions,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1,
                                                                                      expectPlansToFailPredicate = buggyVersions))
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching node property using string prefix and writing other node should be eager") {
    relate(createNode(Map("prop" -> "5")), createNode())
    val query = "MATCH (n)-[r]-(m) WHERE n.prop STARTS WITH '5' SET m.prop = '5' RETURN count(*)"

    val buggyVersions = Configs.Cost3_3 + Configs.Cost3_1
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                              expectedDifferentResults = buggyVersions,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1,
                                                                                      expectPlansToFailPredicate = buggyVersions))
    assertNumberOfEagerness(result.executionPlanDescription(), 1)
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching node property using string postfix and writing other node should be eager") {
    relate(createNode(Map("prop" -> "5")), createNode())
    val query = "MATCH (n)-[r]-(m) WHERE n.prop ENDS WITH '5' SET m.prop = '5' RETURN count(*)"

    val buggyVersions = Configs.Cost3_3 + Configs.Cost3_1
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             expectedDifferentResults = buggyVersions,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1,
                                                                                      expectPlansToFailPredicate = buggyVersions))
    assertNumberOfEagerness(result.executionPlanDescription(), 1)
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching node property using string contains and writing other node should be eager") {
    relate(createNode(Map("prop" -> "5")), createNode())
    val query = "MATCH (n)-[r]-(m) WHERE n.prop CONTAINS '5' SET m.prop = '5' RETURN count(*)"

    val buggyVersions = Configs.Cost3_3 + Configs.Cost3_1
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             expectedDifferentResults = buggyVersions,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1,
                                                                                      expectPlansToFailPredicate = buggyVersions))
    assertNumberOfEagerness(result.executionPlanDescription(), 1)
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching node property using IS NULL and writing other node should be eager") {
    relate(createNode(), createNode(Map("prop" -> "5")))
    val query = "MATCH (n)-[r]-(m) WHERE n.prop IS NULL REMOVE m.prop RETURN count(*)"

    val buggyVersions = Configs.Cost3_3 + Configs.Cost3_1
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             expectedDifferentResults = buggyVersions,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1,
                                                                                      expectPlansToFailPredicate = buggyVersions))
    assertNumberOfEagerness(result.executionPlanDescription(), 1)
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching node property using IS NOT NULL and writing other node should be eager") {
    relate(createNode(Map("prop" -> "5")), createNode())
    val query = "MATCH (n)-[r]-(m) WHERE n.prop IS NOT NULL SET m.prop='5' RETURN count(*)"

    val buggyVersions = Configs.Cost3_3 + Configs.Cost3_1 + Configs.DefaultRule + Configs.Rule3_1
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             expectedDifferentResults = buggyVersions,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1,
                                                                                      expectPlansToFailPredicate = buggyVersions))
    assertNumberOfEagerness(result.executionPlanDescription(), 1)
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching node property using EXISTS and writing other node should be eager") {
    relate(createNode(Map("prop" -> "5")), createNode())
    val query = "MATCH (n)-[r]-(m) WHERE exists(n.prop) SET m.prop='5' RETURN count(*)"

    val buggyVersions = Configs.Cost3_3 + Configs.Cost3_1 + Configs.DefaultRule + Configs.Rule3_1
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             expectedDifferentResults = buggyVersions,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1,
                                                                                      expectPlansToFailPredicate = buggyVersions))
    assertNumberOfEagerness(result.executionPlanDescription(), 1)
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching node property using less than operator and writing other node should be eager") {
    relate(createNode(Map("prop" -> 5)), createNode())
    val query = "MATCH (n)-[r]-(m) WHERE n.prop < 6 SET m.prop = 5 RETURN count(*)"
    val buggyVersions = Configs.Cost3_3 + Configs.Cost3_1
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             expectedDifferentResults = buggyVersions,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1,
                                                                                      expectPlansToFailPredicate = buggyVersions))
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching node property using less than or equal operator and writing other node should be eager") {
    relate(createNode(Map("prop" -> 5)), createNode())
    val query = "MATCH (n)-[r]-(m) WHERE n.prop <= 6 SET m.prop = 5 RETURN count(*)"
    val buggyVersions = Configs.Cost3_3 + Configs.Cost3_1
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             expectedDifferentResults = buggyVersions,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1,
                                                                                      expectPlansToFailPredicate = buggyVersions))
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching node property using greater than operator and writing other node should be eager") {
    relate(createNode(Map("prop" -> 5)), createNode())
    val query = "MATCH (n)-[r]-(m) WHERE n.prop > 4 SET m.prop = 5 RETURN count(*)"
    val buggyVersions = Configs.Cost3_3 + Configs.Cost3_1
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             expectedDifferentResults = buggyVersions,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1,
                                                                                      expectPlansToFailPredicate = buggyVersions))
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching node property using greater than or equal operator and writing other node should be eager") {
    relate(createNode(Map("prop" -> 5)), createNode())
    val query = "MATCH (n)-[r]-(m) WHERE n.prop >= 4 SET m.prop = 5 RETURN count(*)"
    val buggyVersions = Configs.Cost3_3 + Configs.Cost3_1
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             expectedDifferentResults = buggyVersions,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1,
                                                                                      expectPlansToFailPredicate = buggyVersions))
    assertStats(result, propertiesWritten = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("matching node property using greater than or equal operator and creating other node should be eager") {
    relate(createNode(Map("prop" -> 5)), createNode())
    val query = "MATCH (n)-[r]-(m) WHERE n.prop >= 4 CREATE ({prop:5})-[:R]->(m) RETURN count(*)"
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, propertiesWritten = 1, nodesCreated = 1, relationshipsCreated = 1)
    result.toList should equal(List(Map("count(*)" -> 1)))
  }

  test("unstable iterator and property predicates followed by set must be eager") {
    createLabeledNode(Map("prop" -> 42), "L")
    createLabeledNode("L")
    createNode()
    val query = "MATCH (m1:L), (m2:L) WHERE m1.prop < 43 SET m2.prop = 42 RETURN count(*)"
    val buggyVersions = Configs.Cost3_3 + Configs.Cost3_1 + Configs.Rule3_1 + Configs.DefaultRule
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             expectedDifferentResults = buggyVersions,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1,
                                                                                      expectPlansToFailPredicate = buggyVersions))
    assertStats(result, propertiesWritten = 2)
    result.toList should equal(List(Map("count(*)" -> 2)))
  }

  test("unstable iterator and property predicates followed by CREATE must be eager") {
    createLabeledNode(Map("prop" -> 42), "L")
    createLabeledNode("L")
    createNode()
    val query = "MATCH (m1:L), (m2:L) WHERE m1.prop < 43 CREATE (:L {prop: 42}) RETURN count(*)"
    val result = executeWith(Configs.Interpreted - Configs.Cost2_3, query,
                             planComparisonStrategy = testEagerPlanComparisonStrategy(1))
    assertStats(result, propertiesWritten = 2, labelsAdded = 2, nodesCreated = 2)
    result.toList should equal(List(Map("count(*)" -> 2)))
  }

  private def testEagerPlanComparisonStrategy(expectedEagerCount: Int,
                                              expectPlansToFailPredicate: TestConfiguration = TestConfiguration.empty,
                                              optimalEagerCount: Int = -1) = {
    val failureMessage = s"Unexpected number of eagers. Expected $expectedEagerCount" + (if(optimalEagerCount != -1) s", optimal $optimalEagerCount" else "")
    ComparePlansWithPredicate(plan => assertNumberOfEagerness(plan, expectedEagerCount), expectPlansToFailPredicate, failureMessage)
  }

  private def assertNumberOfEagerness(planDescription: InternalPlanDescription, expectedEagerCount: Int): Boolean = {
    val plan = planDescription.toString
    val eagers = EagerRegEx.findAllIn(plan).length
    if (VERBOSE && expectedEagerCount > 0) {
      println(s"Expected eagerness $expectedEagerCount\n  Eager: $eagers\n")
      if (VERBOSE_INCLUDE_PLAN_DESCRIPTION)
        println(plan)
    }

    eagers == expectedEagerCount
  }
}
