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
package org.neo4j.cypher.internal.runtime.spec.tests

import org.neo4j.cypher.internal.CypherRuntime
import org.neo4j.cypher.internal.RuntimeContext
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNode
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createNodeWithProperties
import org.neo4j.cypher.internal.logical.builder.AbstractLogicalPlanBuilder.createRelationship
import org.neo4j.cypher.internal.runtime.spec.Edition
import org.neo4j.cypher.internal.runtime.spec.LogicalQueryBuilder
import org.neo4j.cypher.internal.runtime.spec.RecordingRuntimeResult
import org.neo4j.cypher.internal.runtime.spec.RuntimeTestSuite
import org.neo4j.cypher.internal.runtime.spec.rewriters.TestPlanCombinationRewriter.TestPlanCombinationRewriterHint

import scala.util.Random

abstract class MergeStressTestBase[CONTEXT <: RuntimeContext](
  edition: Edition[CONTEXT],
  runtime: CypherRuntime[CONTEXT],
  testPlanCombinationRewriterHints: Set[TestPlanCombinationRewriterHint] = Set.empty[TestPlanCombinationRewriterHint]
) extends RuntimeTestSuite[CONTEXT](
      edition,
      runtime,
      testPlanCombinationRewriterHints = testPlanCombinationRewriterHints
    ) {
  private val ITERATIONS = 20
  private val PROP_KEY = "id"
  private val VAR_TO_FIND = "idA"
  private val LABEL = "L"
  private val TYPE = "R"
  private val NODE = "n"
  private val REL = "r"

  test("allNodeScan + filter") {
    testNode(builder =>
      builder
        .|.filter(s"$NODE.$PROP_KEY = $VAR_TO_FIND", s"$NODE:$LABEL")
        .|.allNodeScan(NODE)
    )
  }

  test("labelscan + filter") {
    testNode(builder =>
      builder
        .|.filter(s"$NODE.$PROP_KEY = $VAR_TO_FIND")
        .|.nodeByLabelScan(NODE, LABEL)
    )
  }

  test("node index seek") {
    givenGraph {
      nodeIndex(LABEL, PROP_KEY)
    }

    testNode(builder =>
      builder
        .|.nodeIndexOperator(s"$NODE:$LABEL($PROP_KEY = ???)", paramExpr = Some(varFor(VAR_TO_FIND)))
    )
  }

  test("typescan + filter") {
    testRelationship(builder =>
      builder
        .|.filter(s"$REL.$PROP_KEY = $VAR_TO_FIND")
        .|.relationshipTypeScan(s"($NODE)-[$REL:$TYPE]->($NODE)")
    )
  }

  test("relationship index seek") {
    givenGraph {
      relationshipIndex(TYPE, PROP_KEY)
    }

    testRelationship(builder =>
      builder
        .|.relationshipIndexOperator(
          s"($NODE)-[$REL:$TYPE($PROP_KEY = ???)]->($NODE)",
          paramExpr = Some(varFor(VAR_TO_FIND))
        )
    )
  }

  private def testNode(operator: LogicalQueryBuilder => LogicalQueryBuilder): Unit = {

    (1 to ITERATIONS).foreach { _ =>
      val unwindA = Random.nextInt(ITERATIONS)
      val unwindB = Random.nextInt(ITERATIONS)
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("idB")
        .nonFuseable() // force pipeline break so we get continuations
        .unwind(s"${(1 to unwindB).mkString("[", ",", "]")} AS idB")
        .apply()
        .|.merge(nodes = Seq(createNodeWithProperties(NODE, Seq(LABEL), s"{$PROP_KEY: $VAR_TO_FIND}")))
        .theOperator(operator)
        .unwind(s"${(1 to unwindA).mkString("[", ",", "]")} AS $VAR_TO_FIND")
        .argument()
        .build(readOnly = false)

      // then
      rollback {
        val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
        consume(runtimeResult)
        val inner = (1 to unwindB).map(i => Array[Any](i))
        val expected = (1 to unwindA).flatMap(_ => inner)
        runtimeResult should beColumns("idB").withRows(expected).withStatistics(
          nodesCreated = unwindA,
          labelsAdded = unwindA,
          propertiesSet = unwindA
        )
      }
    }
  }

  private def testRelationship(operator: LogicalQueryBuilder => LogicalQueryBuilder): Unit = {

    (1 to ITERATIONS).foreach { _ =>
      val unwindA = Random.nextInt(ITERATIONS)
      val unwindB = Random.nextInt(ITERATIONS)
      val logicalQuery = new LogicalQueryBuilder(this)
        .produceResults("idB")
        .nonFuseable() // force pipeline break so we get continuations
        .unwind(s"${(1 to unwindB).mkString("[", ",", "]")} AS idB")
        .apply()
        .|.merge(
          nodes = Seq(createNode(NODE)),
          relationships =
            Seq(createRelationship(REL, NODE, TYPE, NODE, properties = Some(s"{$PROP_KEY: $VAR_TO_FIND}")))
        )
        .theOperator(operator)
        .unwind(s"${(1 to unwindA).mkString("[", ",", "]")} AS $VAR_TO_FIND")
        .argument()
        .build(readOnly = false)

      // then
      rollback {
        val runtimeResult: RecordingRuntimeResult = execute(logicalQuery, runtime)
        consume(runtimeResult)
        val inner = (1 to unwindB).map(i => Array[Any](i))
        val expected = (1 to unwindA).flatMap(_ => inner)
        runtimeResult should beColumns("idB").withRows(expected).withStatistics(
          nodesCreated = unwindA,
          relationshipsCreated = unwindA,
          propertiesSet = unwindA
        )
      }
    }
  }

  implicit class RichLogicalQueryBuilder(inner: LogicalQueryBuilder) {

    def theOperator(op: LogicalQueryBuilder => LogicalQueryBuilder): LogicalQueryBuilder = {
      op(inner)
    }
  }

}
