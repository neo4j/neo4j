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

import org.neo4j.configuration.GraphDatabaseSettings
import org.neo4j.graphdb.InputPosition
import org.neo4j.graphdb.config.Setting
import org.neo4j.notifications.NotificationCodeWithDescription.runtimeUnsupported

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.nio.file.Path
import java.time.Duration

/**
 * Community `runtime=pipelined` has no pipelined executor and falls back to slotted
 * execution. Planning must therefore use Volcano (row-at-a-time) cost assumptions, not
 * batched ones: a Cartesian product costed with `ceil(lhsCardinality / batchSize)` RHS
 * executions but executed with `lhsCardinality` executions can select an orientation that
 * is catastrophically expensive under the actual slotted execution
 * (see https://github.com/neo4j/neo4j/issues/13937).
 *
 * This test runs the exact #13937 setup and query and asserts that the pipelined-hinted
 * Community plan is identical to the slotted plan, that both complete with the same
 * result, and that the unsupported-runtime fallback notification is preserved.
 */
class CommunityPipelinedFallbackPlanningIT extends ExecutionEngineFunSuite {

  private lazy val importDir: Path = Files.createTempDirectory("pipelinedFallbackPlanning")

  override def databaseConfig(): Map[Setting[?], Object] = {
    super.databaseConfig() ++ Map(
      GraphDatabaseSettings.transaction_timeout -> Duration.ofSeconds(30),
      GraphDatabaseSettings.load_csv_file_url_root -> importDir
    )
  }

  private def writeFuzzCsv(): Unit = {
    Files.write(
      importDir.resolve("fuzz.csv"),
      "a,b\none,two\nthree,four\n".getBytes(StandardCharsets.UTF_8)
    )
  }

  private def setUpIssueGraph(): Unit = {
    execute("DROP INDEX fuzz_node_vector_index IF EXISTS")
    execute("DROP INDEX fuzz_relationship_vector_index IF EXISTS")
    execute(
      """CREATE VECTOR INDEX fuzz_node_vector_index FOR (n:FuzzVector) ON n.embedding
        |OPTIONS { indexConfig: { `vector.dimensions`: 3, `vector.similarity_function`: 'cosine' } }""".stripMargin
    )
    execute(
      """CREATE VECTOR INDEX fuzz_relationship_vector_index FOR ()-[r:FuzzVectorRel]-() ON r.embedding
        |OPTIONS { indexConfig: { `vector.dimensions`: 3, `vector.similarity_function`: 'cosine' } }""".stripMargin
    )
    execute("CALL db.awaitIndexes()")
    execute(
      """CREATE (:FuzzVector {embedding: [1.0, 0.0, 0.0], rank: 1, active: true}),
        |       (:FuzzVector {embedding: [0.0, 1.0, 0.0], rank: 2, active: false})""".stripMargin
    )
    execute(
      """MATCH (a:FuzzVector), (b:FuzzVector)
        |WHERE a.rank = 1 AND b.rank = 2
        |CREATE (a)-[:FuzzVectorRel {embedding: [1.0, 0.0, 0.0], rank: 1}]->(b)""".stripMargin
    )
    execute(
      """UNWIND range(0, 63) AS i
        |CREATE (:l0:l1:l2:l3:l4:l5:l6:l7:l8:l9:l10:l11 {
        |  id: i,
        |  k0: true,
        |  k8: false,
        |  k9: i = 63
        |})""".stripMargin
    )
    execute(
      """UNWIND range(0, 63) AS i
        |MATCH (a {id: i}),
        |      (b {id: CASE WHEN i = 63 THEN 0 ELSE i + 1 END})
        |CREATE (a)-[:rt9 {k8: false}]->(b)""".stripMargin
    )
  }

  private def issueQuery(runtime: String): String =
    s"""CYPHER 25 runtime=$runtime
       |CREATE ()-[:rt9 {
       |  k1: COUNT {
       |    MATCH (), ({k0: true})-[r3 {k8: false}]-()
       |    WHERE EXISTS {
       |      CALL () {
       |        LOAD CSV FROM 'file:///fuzz.csv' AS a
       |        RETURN 1 AS x
       |      }
       |      SKIP 0
       |    }
       |    AND COUNT {
       |      MATCH ()-[r5]-()
       |      SEARCH r5 IN (VECTOR INDEX fuzz_relationship_vector_index FOR [1.0, 0.0, 0.0] LIMIT 3)
       |    } >= 0
       |  }
       |}]->()
       |RETURN 1 AS x""".stripMargin

  private def operatorSkeleton(runtime: String): Seq[String] =
    execute("EXPLAIN " + issueQuery(runtime)).executionPlanDescription().flatten.map(_.name)

  test("pipelined fallback plans like slotted and completes with the same result") {
    writeFuzzCsv()
    setUpIssueGraph()

    // Operator names in traversal order capture the plan structure, including Cartesian
    // product orientation, without coupling to IDs, estimates, or rendering details.
    // Before the fix the pipelined-hinted plan arranged the Cartesian product differently.
    operatorSkeleton("pipelined") should equal(operatorSkeleton("slotted"))

    val slottedRows = execute(issueQuery("slotted")).toList
    slottedRows should equal(List(Map("x" -> 1)))

    val pipelinedResult = execute(issueQuery("pipelined"))
    pipelinedResult.toList should equal(List(Map("x" -> 1)))

    // The Community fallback contract is unchanged: pipelined still falls back to slotted
    // with a notification; only the planning assumptions changed.
    pipelinedResult should containNotifications(
      runtimeUnsupported(
        InputPosition.empty,
        "runtime=pipelined",
        "runtime=slotted",
        "This version of Neo4j does not support the requested runtime: `pipelined`"
      )
    )
  }
}
