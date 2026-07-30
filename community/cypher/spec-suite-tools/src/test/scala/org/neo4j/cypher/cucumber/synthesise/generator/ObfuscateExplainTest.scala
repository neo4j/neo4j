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
package org.neo4j.cypher.cucumber.synthesise.generator

import io.cucumber.datatable.DataTable
import org.neo4j.cypher.cucumber.glue.regular.TestConf
import org.neo4j.cypher.cucumber.steps.Result
import org.neo4j.cypher.cucumber.synthesise.CucumberSalad
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertGqlError
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertResults
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.AssertSucceeds
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.CommitTransaction
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.Execute
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.ExecuteInOpenTx
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.HavingExecuted
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.OpenTransaction
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RecordedScenario
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.RecordedStep
import org.neo4j.cypher.cucumber.synthesise.glue.scenario.SideEffects
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.kernel.api.query.QueryObfuscator
import org.neo4j.test.RandomSupport

import java.net.URI
import java.nio.file.Paths
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.View

class ObfuscateExplainTest extends CypherFunSuite {

  private val parser = new CachingParser(CypherVersion.Cypher25)

  private def testRenderer(): QueryObfuscator.ObfuscatedLiteralRenderer = {
    val counter = new AtomicInteger()
    (typeName: String) => "$" + typeName + "_" + counter.incrementAndGet()
  }

  /** The production format, to prove the emitted tokens are parseable Cypher. */
  private def productionShapedRenderer(): QueryObfuscator.ObfuscatedLiteralRenderer = {
    val counter = new AtomicInteger()
    (typeName: String) => "$`OBFUSCATED " + typeName + " " + counter.incrementAndGet() + "`"
  }

  private def explain(cypher: String): String = generator().explainObfuscated(cypher)

  test("replaces literals with typed tokens in query-text order and prefixes EXPLAIN") {
    explain("MATCH (n) WHERE n.a = 1 AND n.b = 'x' RETURN 2.5, true, null") shouldBe
      "EXPLAIN\nMATCH (n) WHERE n.a = $INTEGER_1 AND n.b = $STRING_2 RETURN $FLOAT_3, $BOOLEAN_4, $ANY_5"
  }

  test("a literal repeated verbatim keeps an ORDER BY expression identical to its projection") {
    explain("RETURN n.p + 1 AS b1 ORDER BY n.p + 1") shouldBe
      "EXPLAIN\nRETURN n.p + $INTEGER_1 AS b1 ORDER BY n.p + $INTEGER_1"
  }

  test("preparser options and EXPLAIN/PROFILE are stripped, never doubled") {
    explain("PROFILE MATCH (n) RETURN n") shouldBe "EXPLAIN\nMATCH (n) RETURN n"
    explain("EXPLAIN RETURN 1 AS x") shouldBe "EXPLAIN\nRETURN $INTEGER_1 AS x"
    explain("CYPHER runtime=slotted RETURN 1 AS x") shouldBe "EXPLAIN\nRETURN $INTEGER_1 AS x"
  }

  test("tokens in the production format parse") {
    val generated = new ObfuscateExplain(ingredients(), () => productionShapedRenderer())
    val result = generated.explainObfuscated("RETURN 1, 'a', 2.0, false, null, [1, 'b']")
    result should include("$`OBFUSCATED STRING 2`")
    noException should be thrownBy parser.parse(result)
  }

  test("keeps obfuscating when a procedure cannot be resolved without a database") {
    explain("CALL my.unregistered.proc(1) YIELD x RETURN x") shouldBe
      "EXPLAIN\nCALL my.unregistered.proc($INTEGER_1) YIELD x RETURN x"
  }

  test("obfuscates queries that semantic analysis would reject, and honours SKIP/LIMIT exemptions") {
    explain("MATCH (n) RETURN n.p ORDER BY count(*) LIMIT 1") shouldBe
      "EXPLAIN\nMATCH (n) RETURN n.p ORDER BY count(*) LIMIT 1"
  }

  private def ingredients(): CucumberSalad.Ingredients =
    CucumberSalad.Ingredients(Seq.empty, new RandomSupport(), Paths.get("unused"), TestConf.Default.Cypher25.conf)

  private def generator(): ObfuscateExplain = new ObfuscateExplain(ingredients(), () => testRenderer())

  private def scenario(
    steps: Seq[RecordedStep],
    tags: Set[String] = Set.empty
  ): RecordedScenario =
    RecordedScenario(new URI("features/Example.feature"), 1, "example", steps, tags)

  private val row = java.util.List.of(java.util.List.of("n"), java.util.List.of("1"))

  test("rewrites test queries, replaces result assertions, keeps setup verbatim") {
    val s = scenario(Seq(
      HavingExecuted("CREATE ({p: 1})"),
      Execute("MATCH (n) WHERE n.p = 1 RETURN n"),
      AssertResults(DataTable.create(row), Result.Single(Result.InAnyOrder)),
      SideEffects(DataTable.emptyDataTable())
    ))
    val generated = generator().generateScenarios(View(s)).iterator.toSeq
    generated should have size 1
    generated.head.steps shouldBe Seq(
      HavingExecuted("CREATE ({p: 1})"),
      Execute("EXPLAIN\nMATCH (n) WHERE n.p = $INTEGER_1 RETURN n"),
      AssertSucceeds
    )
  }

  test("filter drops SEARCH queries, whose index name cannot be a parameter") {
    val search = scenario(Seq(Execute("MATCH (m) SEARCH m IN (VECTOR INDEX idx FOR [1, 2] LIMIT 5) RETURN m")))
    val setupOnlySearch = scenario(Seq(
      HavingExecuted("MATCH (m) SEARCH m IN (VECTOR INDEX idx FOR [1, 2] LIMIT 5) RETURN m"),
      Execute("RETURN 1 AS x")
    ))
    val g = generator()
    g.filter.build(search) shouldBe false
    g.filter.build(setupOnlySearch) shouldBe true
  }

  test("keeps the source tags and records provenance in the comment") {
    val s = scenario(
      Seq(Execute("RETURN 1 AS x"), AssertResults(DataTable.create(row), Result.Single(Result.InAnyOrder))),
      tags = Set("@fails:parallel-runtime", "@allowCustomErrors")
    )
    val generated = generator().generateScenarios(View(s)).iterator.toSeq
    generated.head.tags shouldBe Set("@fails:parallel-runtime", "@allowCustomErrors")
    generated.head.comment should include("based on")
  }

  test("filter keeps scenarios muted for other configs but drops conf-incompatible and generator-muted ones") {
    val muted = scenario(Seq(Execute("RETURN 1 AS x")), tags = Set("@ignore:generator:obfuscate"))
    val otherConf = scenario(Seq(Execute("RETURN 1 AS x")), tags = Set("@fails:parallel-runtime", "@ignore:composite"))
    val wrongVersion = scenario(Seq(Execute("RETURN 1 AS x")), tags = Set("@ignore:cypher-25"))
    val failsEverywhere = scenario(Seq(Execute("RETURN 1 AS x")), tags = Set("@fails"))
    val g = generator()
    g.filter.build(muted) shouldBe false
    g.filter.build(otherConf) shouldBe true
    g.filter.build(wrongVersion) shouldBe false
    g.filter.build(failsEverywhere) shouldBe false
  }

  test("filter drops error-expecting, open-tx, unparseable and test-query-less scenarios") {
    val expectsError = scenario(Seq(
      Execute("RETURN 1 AS x"),
      AssertGqlError(null)
    ))
    val openTx = scenario(Seq(OpenTransaction, ExecuteInOpenTx("RETURN 1 AS x"), CommitTransaction))
    val unparseable = scenario(Seq(Execute("THIS IS NOT CYPHER !!!")))
    val setupOnly = scenario(Seq(HavingExecuted("CREATE ()")))
    val g = generator()
    g.filter.build(expectsError) shouldBe false
    g.filter.build(openTx) shouldBe false
    g.filter.build(unparseable) shouldBe false
    g.filter.build(setupOnly) shouldBe false
  }

  test("write queries and commands are kept") {
    val write = scenario(Seq(Execute("CREATE ({p: 'secret'})")))
    val command = scenario(Seq(Execute("CREATE INDEX FOR (n:L) ON (n.p)")))
    val g = generator()
    g.filter.build(write) shouldBe true
    g.filter.build(command) shouldBe true
    val generated = g.generateScenarios(View(write)).iterator.toSeq
    generated.head.steps.head shouldBe Execute("EXPLAIN\nCREATE ({p: $STRING_1})")
  }
}
