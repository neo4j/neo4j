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
import org.neo4j.cypher.internal.ast.Limit
import org.neo4j.cypher.internal.ast.Skip
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.expressions.AutoExtractedParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.frontend.helpers.ErrorCollectingContext
import org.neo4j.cypher.internal.frontend.helpers.NoPlannerName
import org.neo4j.cypher.internal.frontend.phases.parserTransformers.LiteralExtraction
import org.neo4j.cypher.internal.parser.AstParserFactory
import org.neo4j.cypher.internal.rewriting.rewriters.Forced
import org.neo4j.cypher.internal.rewriting.rewriters.IfNoParameter
import org.neo4j.cypher.internal.rewriting.rewriters.LiteralExtractionStrategy
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.Foldable.FoldableAny
import org.neo4j.cypher.internal.util.Neo4jCypherExceptionFactory
import org.neo4j.cypher.internal.util.ObfuscationMetadata
import org.neo4j.cypher.internal.util.Rewriter
import org.neo4j.cypher.internal.util.bottomUp
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
    "CREATE USER user SET PASSWORD 'secret' CHANGE REQUIRED",
    """RETURN s"hello there {1} what is happening {2}" AS x""",
    "LOAD CSV FROM 'http://host/file.csv' AS line FIELDTERMINATOR ';' RETURN line",
    "SHOW PROCEDURES YIELD name SKIP 1 LIMIT 2"
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

  test("collects the literal type of each primitive kind, tagging null as ANY") {
    val metadata = collect(parse("RETURN 'str', 42, 4.5, true, null"), extractWith = None)
    // Offsets are normalized into ascending start order, so they match the textual order above.
    metadata.allLiteralOffsets.map(_.literalTypeName) shouldBe Vector(
      "STRING",
      "INTEGER",
      "FLOAT",
      "BOOLEAN",
      "ANY"
    )
  }

  test("tags each inner literal of a list with its own type (leaf tagging)") {
    val metadata = collect(parse("RETURN [1, 'two']"), extractWith = None)
    metadata.allLiteralOffsets.map(_.literalTypeName) shouldBe Vector(
      "INTEGER",
      "STRING"
    )
  }

  test("tags each literal inside a point constructor with its own type") {
    val metadata = collect(
      parse("WITH point({longitude: 12.34, latitude: 56.78, crs: 'WGS-84'}) AS p RETURN p"),
      extractWith = None
    )
    metadata.allLiteralOffsets.map(_.literalTypeName) shouldBe Vector(
      "FLOAT",
      "FLOAT",
      "STRING"
    )
  }

  test("tags each literal inside a VECTOR constructor with its own type") {
    val metadata = collect(parse("RETURN VECTOR([1, 2, 3], 3, INT64) AS v"), extractWith = None)
    metadata.allLiteralOffsets.map(_.literalTypeName) shouldBe Vector(
      "INTEGER",
      "INTEGER",
      "INTEGER",
      "INTEGER"
    )
  }

  test("tags a literal inside a temporal constructor with its own type") {
    val metadata = collect(parse("RETURN date('2023-01-01') AS d"), extractWith = None)
    metadata.allLiteralOffsets.map(_.literalTypeName) shouldBe Vector("STRING")
  }

  test("SKIP and LIMIT literals are exempt from the all-literals view") {
    val query = "MATCH (n) WHERE n.prop = 1 RETURN n SKIP 2 LIMIT 3"
    val metadata = collect(parse(query), extractWith = None)
    metadata.sensitiveLiteralOffsets shouldBe empty
    metadata.allLiteralOffsets.map(_.start(0)) should equal(Vector(query.indexOf("1")))
  }

  test("literals in a constant SKIP expression are exempt") {
    val query = "MATCH (n) RETURN n SKIP 1 + 2"
    val metadata = collect(parse(query), extractWith = None)
    metadata.allLiteralOffsets shouldBe empty
  }

  test("YIELD SKIP and LIMIT literals are exempt") {
    val query = "SHOW PROCEDURES YIELD name SKIP 1 LIMIT 2"
    val metadata = collect(parse(query), extractWith = None)
    metadata.allLiteralOffsets shouldBe empty
  }

  test("FIELDTERMINATOR is exempt while an ordinary URL is still collected") {
    val query = "LOAD CSV FROM 'http://host/file.csv' AS line FIELDTERMINATOR ';' RETURN line"
    val metadata = collect(parse(query), extractWith = None)
    metadata.sensitiveLiteralOffsets shouldBe empty
    metadata.allLiteralOffsets.map(_.start(0)) should equal(Vector(query.indexOf("'http")))
  }

  test("FIELDTERMINATOR is exempt while a credential URL stays redacted in both views") {
    val query = "LOAD CSV FROM 'ftp://user:password@host/file.csv' AS line FIELDTERMINATOR ';' RETURN line"
    val metadata = collect(parse(query), extractWith = None)
    metadata.sensitiveLiteralOffsets.map(_.start(0)) should equal(Vector(query.indexOf("'ftp")))
    metadata.allLiteralOffsets should equal(metadata.sensitiveLiteralOffsets)
  }

  test("exemptions survive merged collector passes") {
    val query = "MATCH (n) WHERE n.prop = 1 RETURN n SKIP 2 LIMIT 3"
    val context = new ErrorCollectingContext(version)
    val firstPass = ObfuscationMetadataCollection.process(
      InitialState("mock", NoPlannerName, new AnonymousVariableNameGenerator).withStatement(parse(query)),
      context
    )
    val metadata = ObfuscationMetadataCollection.process(firstPass, context)
      .maybeObfuscationMetadata.getOrElse(ObfuscationMetadata.empty())
    metadata.sensitiveLiteralOffsets shouldBe empty
    metadata.allLiteralOffsets.map(_.start(0)) should equal(Vector(query.indexOf("1")))
  }

  test("SKIP and LIMIT subtrees are never auto-parameterized") {
    val query = "MATCH (n) WHERE n.prop = 1 RETURN n SKIP 2 LIMIT 3"
    val context = new ErrorCollectingContext(version)
    val extracted = LiteralExtraction(Forced).process(
      InitialState("mock", NoPlannerName, new AnonymousVariableNameGenerator).withStatement(parse(query)),
      context
    )
    val statement = extracted.statement()
    val slicingClauses: Seq[Any] =
      statement.folder.findAllByClass[Skip] ++ statement.folder.findAllByClass[Limit]
    withClue("extraction must actually run, otherwise this guard passes vacuously: ") {
      extracted.maybeExtractedParams.getOrElse(Map.empty) should not be empty
      slicingClauses should have size 2
    }
    withClue(
      "literalReplacement skips Skip/Limit subtrees, which is what lets ObfuscationMetadataCollection " +
        "exempt them: the collector's context-free fold over the extracted-params map would otherwise " +
        "re-introduce their offsets, and the sensitive auto-parameter recovery branch inside the " +
        "Skip/Limit re-fold would become reachable and untested. If this fails, revisit both. "
    ) {
      slicingClauses.flatMap(_.folder.findAllByClass[AutoExtractedParameter]) shouldBe empty
    }
  }

  private def markLimitsSensitive(statement: Statement): Statement =
    bottomUp(Rewriter.lift {
      case limit: Limit =>
        Limit(SensitiveParameterRewriter.apply(limit.expression).asInstanceOf[Expression])(limit.position)
    }).apply(statement).asInstanceOf[Statement]

  test("a sensitive literal inside LIMIT is still redacted in both views") {
    val query = "MATCH (n) WHERE n.prop = 1 RETURN n LIMIT 5"
    val metadata = collect(markLimitsSensitive(parse(query)), extractWith = None)
    metadata.sensitiveLiteralOffsets.map(_.start(0)) should equal(Vector(query.indexOf("5")))
    metadata.allLiteralOffsets.map(_.start(0)) should equal(Vector(query.indexOf("1"), query.indexOf("5")))
  }
}
