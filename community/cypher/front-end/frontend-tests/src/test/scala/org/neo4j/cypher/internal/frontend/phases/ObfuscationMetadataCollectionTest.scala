/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.frontend.phases

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.LiteralExtraction
import org.neo4j.cypher.internal.parser.AstParserFactory
import org.neo4j.cypher.internal.rewriting.rewriters.Forced
import org.neo4j.cypher.internal.rewriting.rewriters.IfNoParameter
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtractionStrategy
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.ObfuscationMetadata
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite

class ObfuscationMetadataCollectionTest extends CypherFunSuite with AstConstructionTestSupport {

  private val version = CypherVersion.Cypher25

  // Queries spanning the tricky literal categories: extracted + residual mix, whole-list extraction,
  // skip-and-stay clauses, ContainerIndex string keys, and the AdministrationCommand path (sensitive-only
  // extraction, where a non-secret literal must still appear in the all-view).
  private val queries: Seq[String] = Seq(
    "MATCH (n) WHERE n.prop = 1 RETURN n LIMIT 5",
    "RETURN [1, 2, 3] AS xs",
    "MATCH (n) WHERE n.x IN [10, 20] RETURN n SKIP 2 LIMIT 3",
    "RETURN n['key'] AS v",
    "CREATE USER user SET PASSWORD 'secret' CHANGE REQUIRED"
  )

  private val strategies: Seq[LiteralExtractionStrategy] = Seq(Forced, IfNoParameter)

  private def parse(query: String): Statement =
    AstParserFactory(version)(query, Neo4jCypherExceptionFactory(query, None), None, Seq()).singleStatement()

  private def collect(statement: Statement, extractWith: Option[LiteralExtractionStrategy]): ObfuscationMetadata = {
    val context = new ErrorCollectingContext(version)
    val base: BaseState =
      InitialState("mock", NoPlannerName, new AnonymousVariableNameGenerator).withStatement(statement)
    val state: BaseState = extractWith match {
      case Some(strategy) => LiteralExtraction(strategy).process(base, context)
      case None           => base
    }
    ObfuscationMetadataCollection.process(state, context)
      .maybeObfuscationMetadata.getOrElse(ObfuscationMetadata.empty())
  }

  for {
    query <- queries
    strategy <- strategies
  } {
    test(s"all-literals view is reconstructed after extraction [$strategy]: $query") {
      // Ground truth: collect pre-extraction, when every literal is still present in the AST.
      val expected = collect(parse(query), extractWith = None).allLiteralOffsets
      // After extraction the literals move into the extracted-params map; the view must be rebuilt from
      // the residual statement literals plus the recovered original expressions.
      val afterExtraction = collect(parse(query), extractWith = Some(strategy)).allLiteralOffsets
      afterExtraction shouldBe expected
    }
  }

  test("the secret view is a subset of the all-literals view (CREATE USER password)") {
    val metadata = collect(parse("CREATE USER user SET PASSWORD 'secret' CHANGE REQUIRED"), extractWith = Some(Forced))
    withClue(
      s"""
         |secret: ${metadata.sensitiveLiteralOffsets}
         |all:    ${metadata.allLiteralOffsets}
         |params: ${metadata.sensitiveParameterNames}
         |""".stripMargin
    ) {
      metadata.sensitiveLiteralOffsets should not be empty
      metadata.sensitiveLiteralOffsets.toSet.subsetOf(metadata.allLiteralOffsets.toSet) shouldBe true
    }
  }

  test("a sensitive credential literal appears in both the secret and the all-literals view") {
    val metadata =
      collect(parse("LOAD CSV FROM 'ftp://user:password@host/file.csv' AS line RETURN line"), extractWith = None)
    metadata.sensitiveLiteralOffsets should not be empty
    metadata.sensitiveLiteralOffsets shouldBe metadata.allLiteralOffsets
  }

  // Mirror the pipeline for a sensitive argument: extract literals, then mark the auto-parameter sensitive in the statement, as SensitiveParameterRewriter does.
  // The original literal is recovered from the map into the secret view with a known length.
  test("a recovered sensitive auto-parameter puts the original literal's offset in the secret view") {
    val context = new ErrorCollectingContext(version)
    val extracted = LiteralExtraction(Forced).process(
      InitialState("mock", NoPlannerName, new AnonymousVariableNameGenerator)
        .withStatement(parse("MATCH (n) WHERE n.name = 'secret' RETURN n")),
      context
    )
    val sensitiveState =
      extracted.withStatement(SensitiveParameterRewriter.apply(extracted.statement()).asInstanceOf[Statement])
    val metadata = ObfuscationMetadataCollection.process(sensitiveState, context)
      .maybeObfuscationMetadata.getOrElse(ObfuscationMetadata.empty())
    metadata.sensitiveLiteralOffsets should have size 1
    metadata.sensitiveLiteralOffsets.head.length should not be empty
    metadata.allLiteralOffsets should contain(metadata.sensitiveLiteralOffsets.head)
  }

  // extracted-params map is dropped so the original cannot be recovered. The
  // collector must emit a single unknown-length (None) offset rather than silently skipping a secret
  test("an unresolved sensitive auto-parameter contributes an unknown-length offset (fail-closed)") {
    val context = new ErrorCollectingContext(version)
    val extracted = LiteralExtraction(Forced).process(
      InitialState("mock", NoPlannerName, new AnonymousVariableNameGenerator)
        .withStatement(parse("MATCH (n) WHERE n.name = 'secret' RETURN n")),
      context
    )
    val sensitiveStatement = SensitiveParameterRewriter.apply(extracted.statement()).asInstanceOf[Statement]
    val metadata = collect(sensitiveStatement, extractWith = None) // fresh state => empty extracted-params map
    metadata.sensitiveLiteralOffsets should have size 1
    metadata.sensitiveLiteralOffsets.head.length shouldBe None
    metadata.allLiteralOffsets should contain(metadata.sensitiveLiteralOffsets.head)
  }
}
