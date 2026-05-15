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

import org.neo4j.configuration.GraphDatabaseInternalSettings
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.RewindableExecutionResult
import org.neo4j.cypher.internal.runtime.QueryStatistics
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.InvalidSyntaxStatus
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.Reparsesable_42I67
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlException
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.cypher.internal.util.test_helpers.WindowsStringSafe
import org.neo4j.exceptions.CantCompileQueryException
import org.neo4j.exceptions.CypherExecutionException
import org.neo4j.exceptions.RuntimeUnsupportedException
import org.neo4j.exceptions.SyntaxException
import org.neo4j.gqlstatus.GqlStatusInfoCodes
import org.neo4j.gqlstatus.NotificationClassification
import org.neo4j.graphdb.NotificationCategory
import org.neo4j.graphdb.SeverityLevel
import org.neo4j.graphdb.config.Setting
import org.neo4j.graphdb.schema.ConstraintType
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_DIMENSIONS
import org.neo4j.graphdb.schema.IndexSettingImpl.VECTOR_SIMILARITY_FUNCTION
import org.neo4j.graphdb.schema.IndexType
import org.neo4j.internal.schema.AllIndexProviderDescriptors
import org.neo4j.kernel.api.exceptions.Status
import org.neo4j.kernel.impl.api.index.IndexingService

import scala.jdk.CollectionConverters.IterableHasAsScala

class CommunityIndexAndConstraintCommandAcceptanceTest extends ExecutionEngineFunSuite with GraphDatabaseTestSupport {
  implicit val windowsSafe: WindowsStringSafe.type = WindowsStringSafe

  private val indexName = "myIndex"
  private val constraintName = "myConstraint"
  private val label = "myLabel"
  private val relType = "myRelType"
  private val prop = "myProp"

  private val cypherVersions =
    (CypherVersion.values().map(cv => (s"CYPHER ${cv.versionName} ", cv.equals(CypherVersion.Cypher5)))
      :+ ("", dbmsDefaultQueryLanguage == CypherVersion.Cypher5))

  private def withoutIdColumn(result: List[Map[String, AnyRef]]) =
    result.map(m => m.filterNot { case (key, _) => key.equals("id") })

  private def removeExistingLookupIndexes(): Unit = graph.withTx(tx => {
    tx.schema().getIndexes().asScala
      .filter(id => id.getIndexType.equals(IndexType.LOOKUP))
      .foreach(i => i.drop())
  })

  private def anyMap(elems: (String, Any)*): Map[String, Any] = Map[String, Any](elems*)

  private def assertVectorIndexDimensionsNotSpecifiedWarning(result: RewindableExecutionResult): Unit = {
    val vectorIndexDimensionsNotSpecifiedDescription =
      "When creating a vector index, `vector.dimensions` should be specified. Omitting it is allowed, but " +
        "specifying dimensions ensures that only vectors of that size are indexed and makes dimension mismatches " +
        "fail clearly at query time. For example, set `OPTIONS { indexConfig: { `vector.dimensions`: 1536 } }` " +
        "when creating the index."
    val notification = result.notifications
      .find(_.getCode == Status.Schema.VectorIndexDimensionsNotSpecified.code.serialize())
      .getOrElse(fail(s"Expected vector index dimensions notification, but got: ${result.notifications}"))

    notification should have(
      Symbol("code")(Status.Schema.VectorIndexDimensionsNotSpecified.code.serialize()),
      Symbol("severity")(SeverityLevel.INFORMATION),
      Symbol("category")(NotificationCategory.SCHEMA),
      Symbol("title")("Vector index dimensions not specified."),
      Symbol("description")(vectorIndexDimensionsNotSpecifiedDescription)
    )

    val gqlStatusObject = result.gqlStatusObjects
      .find(_.gqlStatus == GqlStatusInfoCodes.STATUS_00NA2.getStatusString)
      .getOrElse(fail(s"Expected 00NA2 in GQL status objects, but got: ${result.gqlStatusObjects}"))

    gqlStatusObject should have(
      Symbol("severity")(SeverityLevel.INFORMATION),
      Symbol("classification")(NotificationClassification.SCHEMA),
      Symbol("statusDescription")(
        "note: successful completion - vector index dimensions not specified. " +
          vectorIndexDimensionsNotSpecifiedDescription
      )
    )
  }

  override def databaseConfig(): Map[Setting[?], Object] = super.databaseConfig() ++ Map(
    GraphDatabaseInternalSettings.graph_type_enabled -> java.lang.Boolean.TRUE,
    GraphDatabaseInternalSettings.dependent_constraints_enabled -> java.lang.Boolean.TRUE,
    GraphDatabaseInternalSettings.relationship_endpoint_label_and_node_label_existence_constraints -> java.lang.Boolean.TRUE
  )

  // Index commands

  test("Create node range index") {
    // WHEN
    val statistics = execute(s"CREATE INDEX $indexName FOR (n:$label) ON n.$prop").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.RANGE)
  }

  test("Create relationship range index") {
    // WHEN
    val statistics =
      execute(s"CREATE INDEX $$param FOR ()-[r:$relType]-() ON r.$prop", Map("param" -> indexName)).queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.RANGE)
  }

  test("Create node text index") {
    // WHEN
    val statistics = execute(s"CREATE TEXT INDEX $indexName FOR (n:$label) ON n.$prop").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.TEXT)
  }

  test("Create relationship text index") {
    // WHEN
    val statistics =
      execute(s"CREATE TEXT INDEX $indexName FOR ()-[r:$relType]-() ON r.$prop").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.TEXT)
  }

  test("Create node point index") {
    // WHEN
    val statistics = execute(s"CREATE POINT INDEX $indexName FOR (n:$label) ON n.$prop").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.POINT)
  }

  test("Create relationship point index") {
    // WHEN
    val statistics =
      execute(s"CREATE POINT INDEX $indexName FOR ()-[r:$relType]-() ON r.$prop").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.POINT)
  }

  test("Create node vector index") {
    // WHEN
    val statistics = execute(
      s"CREATE VECTOR INDEX $indexName FOR (n:$label) ON n.$prop OPTIONS {indexConfig: $$map}",
      Map("map" -> anyMap(
        VECTOR_DIMENSIONS.getSettingName -> 50,
        VECTOR_SIMILARITY_FUNCTION.getSettingName -> "COSINE"
      ))
    ).queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.VECTOR)
  }

  test("Create node vector index without dimensions should warn") {
    // WHEN
    val result = execute(
      s"CREATE VECTOR INDEX $indexName FOR (n:$label) ON n.$prop OPTIONS {indexConfig: $$map}",
      Map("map" -> anyMap(
        VECTOR_SIMILARITY_FUNCTION.getSettingName -> "COSINE"
      ))
    )

    // THEN
    result.queryStatistics() should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.VECTOR)
    assertVectorIndexDimensionsNotSpecifiedWarning(result)
  }

  test("Create node vector index without dimensions should warn on repeated IF NOT EXISTS") {
    val query =
      s"CREATE VECTOR INDEX $indexName IF NOT EXISTS FOR (n:$label) ON n.$prop OPTIONS {indexConfig: $$map}"
    val params = Map("map" -> anyMap(
      VECTOR_SIMILARITY_FUNCTION.getSettingName -> "COSINE"
    ))

    // WHEN
    val firstResult = execute(query, params)
    val secondResult = execute(query, params)

    // THEN
    firstResult.queryStatistics() should be(QueryStatistics(indexesAdded = 1))
    firstResult.notifications.size shouldBe 1
    assertVectorIndexDimensionsNotSpecifiedWarning(firstResult)

    secondResult.queryStatistics() should be(QueryStatistics(indexesAdded = 0))
    secondResult.notifications.size shouldBe 2
    secondResult.notifications.map(_.getCode) should contain(
      Status.Schema.IndexOrConstraintAlreadyExists.code.serialize()
    )
    secondResult.gqlStatusObjects.map(_.gqlStatus) should contain(GqlStatusInfoCodes.STATUS_00NA0.getStatusString)
    assertVectorIndexDimensionsNotSpecifiedWarning(secondResult)
  }

  test("Create node vector index with dimensions should not warn") {
    // WHEN
    val result = execute(
      s"CREATE VECTOR INDEX $indexName FOR (n:$label) ON n.$prop OPTIONS {indexConfig: $$map}",
      Map("map" -> anyMap(
        VECTOR_DIMENSIONS.getSettingName -> 50,
        VECTOR_SIMILARITY_FUNCTION.getSettingName -> "COSINE"
      ))
    )

    // THEN
    result.queryStatistics() should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.VECTOR)
    result.notifications.map(_.getCode) should not contain
      Status.Schema.VectorIndexDimensionsNotSpecified.code.serialize()
    result.gqlStatusObjects.map(_.gqlStatus) should not contain
      GqlStatusInfoCodes.STATUS_00NA2.getStatusString
  }

  test("Create relationship vector index") {
    // WHEN
    val result = execute(
      s"CREATE VECTOR INDEX $indexName FOR ()-[r:$relType]-() ON r.$prop OPTIONS {indexConfig: $$map}",
      Map("map" -> anyMap(
        VECTOR_DIMENSIONS.getSettingName -> 50,
        VECTOR_SIMILARITY_FUNCTION.getSettingName -> "COSINE"
      ))
    )

    // THEN
    result.queryStatistics() should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.VECTOR)
    result.notifications.map(_.getCode) should not contain
      Status.Schema.VectorIndexDimensionsNotSpecified.code.serialize()
    result.gqlStatusObjects.map(_.gqlStatus) should not contain
      GqlStatusInfoCodes.STATUS_00NA2.getStatusString
  }

  test("Create relationship vector index without dimensions should warn") {
    // WHEN
    val result = execute(
      s"CREATE VECTOR INDEX $indexName FOR ()-[r:$relType]-() ON r.$prop OPTIONS {indexConfig: $$map}",
      Map("map" -> anyMap(
        VECTOR_SIMILARITY_FUNCTION.getSettingName -> "COSINE"
      ))
    )

    // THEN
    result.queryStatistics() should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.VECTOR)
    assertVectorIndexDimensionsNotSpecifiedWarning(result)
  }

  test("Create relationship vector index without dimensions should warn on repeated IF NOT EXISTS") {
    val query =
      s"CREATE VECTOR INDEX $indexName IF NOT EXISTS FOR ()-[r:$relType]-() ON r.$prop OPTIONS {indexConfig: $$map}"
    val params = Map("map" -> anyMap(
      VECTOR_SIMILARITY_FUNCTION.getSettingName -> "COSINE"
    ))

    // WHEN
    val firstResult = execute(query, params)
    val secondResult = execute(query, params)

    // THEN
    firstResult.queryStatistics() should be(QueryStatistics(indexesAdded = 1))
    firstResult.notifications.size shouldBe 1
    assertVectorIndexDimensionsNotSpecifiedWarning(firstResult)

    secondResult.queryStatistics() should be(QueryStatistics(indexesAdded = 0))
    secondResult.notifications.size shouldBe 2
    secondResult.notifications.map(_.getCode) should contain(
      Status.Schema.IndexOrConstraintAlreadyExists.code.serialize()
    )
    secondResult.gqlStatusObjects.map(_.gqlStatus) should contain(GqlStatusInfoCodes.STATUS_00NA0.getStatusString)
    assertVectorIndexDimensionsNotSpecifiedWarning(secondResult)
  }

  test("Create multi-label vector index with additional properties") {
    // This test only applies to Cypher 25 as it will not parse in Cypher 5

    // WHEN
    val statistics = execute(
      s"CYPHER 25 CREATE VECTOR INDEX $indexName FOR (n:Label|Label2) ON n.vectorProp WITH [n.additionalProp]"
    ).queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.VECTOR)
  }

  test("Create node fulltext index") {
    // WHEN
    val statistics = execute(s"CREATE FULLTEXT INDEX $indexName FOR (n:$label) ON EACH [n.$prop]").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.FULLTEXT)
  }

  test("Create relationship fulltext index") {
    // WHEN
    val statistics =
      execute(s"CREATE FULLTEXT INDEX $indexName FOR ()-[r:$relType]-() ON EACH [r.$prop]").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.FULLTEXT)
  }

  test("Create node lookup index") {
    // GIVEN
    removeExistingLookupIndexes()

    // WHEN
    val statistics = execute(s"CREATE LOOKUP INDEX $indexName FOR (n) ON EACH labels(n)").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.LOOKUP)
  }

  test("Create relationship lookup index") {
    // GIVEN
    removeExistingLookupIndexes()

    // WHEN
    val statistics =
      execute(s"CREATE LOOKUP INDEX $indexName FOR ()-[r]-() ON EACH type(r)").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesAdded = 1))

    graph.indexExists(indexName) should be(true)
    graph.getIndexTypeByName(indexName) should be(IndexType.LOOKUP)
  }

  test("Drop index") {
    // GIVEN
    graph.createNodeIndexWithName(indexName, label, prop)

    // WHEN
    val statistics = execute(s"DROP INDEX $indexName").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(indexesRemoved = 1))
    graph.indexExists(constraintName) should be(false)
  }

  test("Show index", Tags.NoSpdOverride) {
    // GIVEN
    graph.createRelationshipIndexWithName(indexName, relType, prop)
    graph.getDependencyResolver.resolveDependency(classOf[IndexingService]).reportUsageStatistics()

    // WHEN
    val result = execute("SHOW RANGE INDEXES")

    // THEN
    withoutIdColumn(result.toList) should be(List(Map[String, AnyRef](
      "name" -> indexName,
      "state" -> "ONLINE",
      "populationPercent" -> 100.0.asInstanceOf[AnyRef],
      "type" -> IndexType.RANGE.name(),
      "entityType" -> "RELATIONSHIP",
      "labelsOrTypes" -> List(relType),
      "properties" -> List(prop),
      "indexProvider" -> AllIndexProviderDescriptors.RANGE_DESCRIPTOR.name(),
      "owningConstraint" -> null,
      "lastRead" -> null,
      "readCount" -> 0L.asInstanceOf[AnyRef]
    )))
  }

  test("Index commands with Cypher versions") {
    // GIVEN
    removeExistingLookupIndexes()
    val createCommands = Seq(
      (s"CREATE INDEX $indexName FOR (n:$label) ON n.$prop", "RANGE"),
      (s"CREATE INDEX $indexName FOR ()-[r:$relType]-() ON r.$prop", "RANGE"),
      (s"CREATE TEXT INDEX $indexName FOR (n:$label) ON n.$prop", "TEXT"),
      (s"CREATE TEXT INDEX $indexName FOR ()-[r:$relType]-() ON r.$prop", "TEXT"),
      (s"CREATE POINT INDEX $indexName FOR (n:$label) ON n.$prop", "POINT"),
      (s"CREATE POINT INDEX $indexName FOR ()-[r:$relType]-() ON r.$prop", "POINT"),
      (s"CREATE VECTOR INDEX $indexName FOR (n:$label) ON n.$prop", "VECTOR"),
      (s"CREATE VECTOR INDEX $indexName FOR ()-[r:$relType]-() ON r.$prop", "VECTOR"),
      (s"CREATE FULLTEXT INDEX $indexName FOR (n:$label) ON EACH [n.$prop]", "FULLTEXT"),
      (s"CREATE FULLTEXT INDEX $indexName FOR ()-[r:$relType]-() ON EACH [r.$prop]", "FULLTEXT"),
      (s"CREATE LOOKUP INDEX $indexName FOR (n) ON EACH labels(n)", "LOOKUP"),
      (s"CREATE LOOKUP INDEX $indexName FOR ()-[r]-() ON EACH type(r)", "LOOKUP")
    )
    val dropCommand = s"DROP INDEX $indexName"
    val showCommand = "SHOW INDEXES YIELD name, type"

    // WHEN .. THEN
    cypherVersions.foreach { case (cypherVersionString, _) =>
      createCommands.foreach { case (createCommand, indexType) =>
        withClue(cypherVersionString + createCommand) {
          // Create
          val resCreate = execute(cypherVersionString + createCommand)
          resCreate.queryStatistics().indexesAdded should be(1)

          // Show
          val resShow = execute(cypherVersionString + showCommand)
          resShow.toList should be(List(Map("name" -> indexName, "type" -> indexType)))

          // Drop
          val resDrop = execute(cypherVersionString + dropCommand)
          resDrop.queryStatistics().indexesRemoved should be(1)
        }
      }
    }
  }

  // Constraint commands

  test("Create node uniqueness constraint") {
    // WHEN
    val statistics =
      execute(s"CREATE CONSTRAINT $constraintName FOR (n:$label) REQUIRE n.$prop IS UNIQUE").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(nodePropUniquenessConstraintsAdded = 1))
    statistics.constraintsAdded should be(1)

    graph.constraintExists(constraintName) should be(true)
    graph.getConstraintTypeByName(constraintName) should be(ConstraintType.UNIQUENESS)
  }

  test("Create relationship uniqueness constraint") {
    // WHEN
    val statistics = execute(
      s"CREATE CONSTRAINT $$param FOR ()-[r:$relType]-() REQUIRE r.$prop IS UNIQUE",
      Map("param" -> constraintName)
    ).queryStatistics()

    // THEN
    statistics should be(QueryStatistics(relPropUniquenessConstraintsAdded = 1))
    statistics.constraintsAdded should be(1)

    graph.constraintExists(constraintName) should be(true)
    graph.getConstraintTypeByName(constraintName) should be(ConstraintType.RELATIONSHIP_UNIQUENESS)
  }

  test("Create node key constraint", Tags.NoSpdOverride) {
    // WHEN
    val exception = the[CypherExecutionException] thrownBy {
      execute(s"CREATE CONSTRAINT $constraintName FOR (n:$label) REQUIRE n.$prop IS NODE KEY")
    }

    // THEN
    exception should be(gqlException(
      s"""Unable to create Constraint( type='NODE KEY', schema=(:$label {$prop}) ):
         |Node Key constraint requires Neo4j Enterprise Edition. Note that only the first found violation is shown.""".stripMargin,
      gqlStatus(
        GqlStatusInfoCodes.STATUS_50N11,
        s"error: general processing exception - constraint creation failed. Unable to create 'Constraint( type='NODE KEY', schema=(:$label {$prop}) )'."
      ).withCause(
        GqlStatusInfoCodes.STATUS_51N27,
        "error: system configuration or operation exception - not supported in this edition. Key constraint is not supported in community edition."
      )
    ))
    graph.constraintExists(constraintName) should be(false)
  }

  test("Create relationship key constraint", Tags.NoSpdOverride) {
    // WHEN
    val exception = the[CypherExecutionException] thrownBy {
      execute(s"CREATE CONSTRAINT $constraintName FOR ()-[r:$relType]-() REQUIRE r.$prop IS REL KEY")
    }

    // THEN
    exception should be(gqlException(
      s"""Unable to create Constraint( type='RELATIONSHIP KEY', schema=()-[:$relType {$prop}]-() ):
         |Relationship Key constraint requires Neo4j Enterprise Edition. Note that only the first found violation is shown.""".stripMargin,
      gqlStatus(
        GqlStatusInfoCodes.STATUS_50N11,
        s"error: general processing exception - constraint creation failed. Unable to create 'Constraint( type='RELATIONSHIP KEY', schema=()-[:$relType {$prop}]-() )'."
      ).withCause(
        GqlStatusInfoCodes.STATUS_51N27,
        "error: system configuration or operation exception - not supported in this edition. Key constraint is not supported in community edition."
      )
    ))
    graph.constraintExists(constraintName) should be(false)
  }

  test("Create node property existence constraint", Tags.NoSpdOverride) {
    // WHEN
    val exception = the[CypherExecutionException] thrownBy {
      execute(s"CREATE CONSTRAINT $constraintName FOR (n:$label) REQUIRE n.$prop IS NOT NULL")
    }

    // THEN
    exception should be(gqlException(
      s"""Unable to create Constraint( type='NODE PROPERTY EXISTENCE', schema=(:$label {$prop}) ):
         |Property existence constraint requires Neo4j Enterprise Edition. Note that only the first found violation is shown.""".stripMargin,
      gqlStatus(
        GqlStatusInfoCodes.STATUS_50N11,
        s"error: general processing exception - constraint creation failed. Unable to create 'Constraint( type='NODE PROPERTY EXISTENCE', schema=(:$label {$prop}) )'."
      ).withCause(
        GqlStatusInfoCodes.STATUS_51N27,
        "error: system configuration or operation exception - not supported in this edition. Property existence constraint is not supported in community edition."
      )
    ))
    graph.constraintExists(constraintName) should be(false)
  }

  test("Create relationship property existence constraint", Tags.NoSpdOverride) {
    // WHEN
    val exception = the[CypherExecutionException] thrownBy {
      execute(s"CREATE CONSTRAINT $constraintName FOR ()-[r:$relType]-() REQUIRE r.$prop IS NOT NULL")
    }

    // THEN
    exception should be(gqlException(
      s"""Unable to create Constraint( type='RELATIONSHIP PROPERTY EXISTENCE', schema=()-[:$relType {$prop}]-() ):
         |Property existence constraint requires Neo4j Enterprise Edition. Note that only the first found violation is shown.""".stripMargin,
      gqlStatus(
        GqlStatusInfoCodes.STATUS_50N11,
        s"error: general processing exception - constraint creation failed. Unable to create 'Constraint( type='RELATIONSHIP PROPERTY EXISTENCE', schema=()-[:$relType {$prop}]-() )'."
      ).withCause(
        GqlStatusInfoCodes.STATUS_51N27,
        "error: system configuration or operation exception - not supported in this edition. Property existence constraint is not supported in community edition."
      )
    ))
    graph.constraintExists(constraintName) should be(false)
  }

  test("Create node property type constraint", Tags.NoSpdOverride) {
    // WHEN
    val exception = the[CypherExecutionException] thrownBy {
      execute(s"CREATE CONSTRAINT $constraintName FOR (n:$label) REQUIRE n.$prop IS :: INT")
    }

    // THEN
    exception should be(gqlException(
      s"""Unable to create Constraint( name='$constraintName', type='NODE PROPERTY TYPE', schema=(:$label {$prop}), propertyType=INTEGER ):
         |Property type constraint requires Neo4j Enterprise Edition. Note that only the first found violation is shown.""".stripMargin,
      gqlStatus(
        GqlStatusInfoCodes.STATUS_50N11,
        s"error: general processing exception - constraint creation failed. Unable to create '$constraintName'."
      ).withCause(
        GqlStatusInfoCodes.STATUS_51N27,
        "error: system configuration or operation exception - not supported in this edition. Property type constraint is not supported in community edition."
      )
    ))
    graph.constraintExists(constraintName) should be(false)
  }

  test("Create relationship property type constraint", Tags.NoSpdOverride) {
    // WHEN
    val exception = the[CypherExecutionException] thrownBy {
      execute(s"CREATE CONSTRAINT $constraintName FOR ()-[r:$relType]-() REQUIRE r.$prop IS :: INT")
    }

    // THEN
    exception should be(gqlException(
      s"""Unable to create Constraint( name='$constraintName', type='RELATIONSHIP PROPERTY TYPE', schema=()-[:$relType {$prop}]-(), propertyType=INTEGER ):
         |Property type constraint requires Neo4j Enterprise Edition. Note that only the first found violation is shown.""".stripMargin,
      gqlStatus(
        GqlStatusInfoCodes.STATUS_50N11,
        s"error: general processing exception - constraint creation failed. Unable to create '$constraintName'."
      ).withCause(
        GqlStatusInfoCodes.STATUS_51N27,
        "error: system configuration or operation exception - not supported in this edition. Property type constraint is not supported in community edition."
      )
    ))
    graph.constraintExists(constraintName) should be(false)
  }

  test("Drop constraint") {
    // GIVEN
    graph.createNodeUniquenessConstraintWithName(constraintName, label, prop)

    // WHEN
    val statistics = execute(s"DROP CONSTRAINT $constraintName").queryStatistics()

    // THEN
    statistics should be(QueryStatistics(constraintsRemoved = 1))
    graph.constraintExists(constraintName) should be(false)
  }

  test("Show constraints") {
    // GIVEN
    graph.createRelationshipUniquenessConstraintWithName(constraintName, relType, prop)

    // WHEN
    val result = execute("SHOW CONSTRAINTS")

    // THEN
    val expectedBase = Map[String, AnyRef](
      "name" -> constraintName,
      "entityType" -> "RELATIONSHIP",
      "labelsOrTypes" -> List(relType),
      "properties" -> List(prop),
      "ownedIndex" -> constraintName,
      "propertyType" -> null
    )
    val expectedCypher5 = expectedBase ++ Map[String, AnyRef](
      "type" -> ConstraintType.RELATIONSHIP_UNIQUENESS.name()
    )
    val expectedCypher25 = expectedBase ++ Map[String, AnyRef](
      "type" -> "RELATIONSHIP_PROPERTY_UNIQUENESS",
      "enforcedLabel" -> null
    )
    val expected = if (dbmsDefaultQueryLanguage == CypherVersion.Cypher5) expectedCypher5 else expectedCypher25
    withoutIdColumn(result.toList) should be(List(expected))
  }

  test("Constraint commands with Cypher versions", Tags.NoSpdOverride) {
    // GIVEN
    val allowedCreateCommands = Seq(
      (
        s"CREATE CONSTRAINT $constraintName FOR (n:$label) REQUIRE n.$prop IS UNIQUE",
        "NODE_PROPERTY_UNIQUENESS",
        "UNIQUENESS"
      ),
      (
        s"CREATE CONSTRAINT $constraintName FOR ()-[r:$relType]-() REQUIRE r.$prop IS UNIQUE",
        "RELATIONSHIP_PROPERTY_UNIQUENESS",
        "RELATIONSHIP_UNIQUENESS"
      )
    )
    val failingCreateCommands: Seq[(String, String, String)] = Seq(
      (
        s"CREATE CONSTRAINT $constraintName FOR (n:$label) REQUIRE n.$prop IS NODE KEY",
        s"Constraint( type='NODE KEY', schema=(:$label {$prop}) )",
        "Key"
      ),
      (
        s"CREATE CONSTRAINT $constraintName FOR ()-[r:$relType]-() REQUIRE r.$prop IS REL KEY",
        s"Constraint( type='RELATIONSHIP KEY', schema=()-[:$relType {$prop}]-() )",
        "Key"
      ),
      (
        s"CREATE CONSTRAINT $constraintName FOR (n:$label) REQUIRE n.$prop IS NOT NULL",
        s"Constraint( type='NODE PROPERTY EXISTENCE', schema=(:$label {$prop}) )",
        "Property existence"
      ),
      (
        s"CREATE CONSTRAINT $constraintName FOR ()-[r:$relType]-() REQUIRE r.$prop IS NOT NULL",
        s"Constraint( type='RELATIONSHIP PROPERTY EXISTENCE', schema=()-[:$relType {$prop}]-() )",
        "Property existence"
      ),
      (s"CREATE CONSTRAINT $constraintName FOR (n:$label) REQUIRE n.$prop IS :: INT", constraintName, "Property type"),
      (
        s"CREATE CONSTRAINT $constraintName FOR ()-[r:$relType]-() REQUIRE r.$prop IS :: INT",
        constraintName,
        "Property type"
      )
    )
    val dropCommand = s"DROP CONSTRAINT $constraintName"
    val showCommand = "SHOW CONSTRAINTS YIELD name, type"

    // WHEN .. THEN
    cypherVersions.foreach { case (cypherVersionString, usesCypher5) =>
      allowedCreateCommands.foreach { case (createCommand, constraintType, constraintTypeCypher5) =>
        withClue(cypherVersionString + createCommand) {
          // Create
          val resCreate = execute(cypherVersionString + createCommand)
          resCreate.queryStatistics().constraintsAdded should be(1)

          // Show
          val resShow = execute(cypherVersionString + showCommand)
          resShow.toList should be(List(Map(
            "name" -> constraintName,
            "type" -> (if (usesCypher5) constraintTypeCypher5 else constraintType)
          )))

          // Drop
          val resDrop = execute(cypherVersionString + dropCommand)
          resDrop.queryStatistics().constraintsRemoved should be(1)
        }
      }

      failingCreateCommands.foreach {

        case (createCommand, constraintNameOrDescr, constraintType) =>
          withClue(cypherVersionString + createCommand) {
            val exception = the[CypherExecutionException] thrownBy {
              execute(cypherVersionString + createCommand)
            }
            val exceptionMessage = exception.getMessage
            exceptionMessage should startWith("Unable to create Constraint(")
            exceptionMessage should endWith(
              "constraint requires Neo4j Enterprise Edition. Note that only the first found violation is shown."
            )
            exception should be(gqlStatus(
              GqlStatusInfoCodes.STATUS_50N11,
              s"error: general processing exception - constraint creation failed. Unable to create '$constraintNameOrDescr'."
            ).withCause(
              GqlStatusInfoCodes.STATUS_51N27,
              s"error: system configuration or operation exception - not supported in this edition. $constraintType constraint is not supported in community edition."
            ))
            graph.constraintExists(constraintName) should be(false)
          }
      }

      // Columns added in Cypher 25 (and behind the graph type feature flag)
      Seq("enforcedLabel", "classification").foreach(column =>
        withClue(cypherVersionString + column) {
          if (usesCypher5) {
            val exception = the[SyntaxException] thrownBy {
              execute(cypherVersionString + "SHOW CONSTRAINTS YIELD " + column)
            }
            exception.getMessage should startWith(s"Trying to YIELD non-existing column: `$column`")
            exception should be(InvalidSyntaxStatus.withCause(gqlStatus(
              GqlStatusInfoCodes.STATUS_22N04,
              s"error: data exception - invalid input value. Invalid input '$column' for column name. " +
                "Expected 'createStatement', 'entityType', 'id', 'labelsOrTypes', 'name', 'options', 'ownedIndex', 'properties', 'propertyType' or 'type'."
            )))
          } else {
            // no constraints, so empty result but no failure
            execute(cypherVersionString + "SHOW CONSTRAINTS YIELD " + column).toList should have size 0
          }
        }
      )
    }
  }

  // Graph type commands

  test("Alter current graph type with empty graph type", Tags.NoSpdOverride) {
    Seq("SET", "ADD", "DROP", "ALTER").foreach(operation =>
      withClue(operation) {
        // WHEN
        val exception = the[CantCompileQueryException] thrownBy {
          execute(s"CYPHER 25 ALTER CURRENT GRAPH TYPE $operation {}")
        }

        // THEN
        exception should be(gqlException(
          s"51N27: 'ALTER CURRENT GRAPH TYPE $operation' is not supported in community edition.",
          gqlStatus(
            GqlStatusInfoCodes.STATUS_51N27,
            "error: system configuration or operation exception - not supported in this edition. " +
              s"'ALTER CURRENT GRAPH TYPE $operation' is not supported in community edition."
          )
        ))
      }
    )
  }

  test("Create property uniqueness constraints through graph type", Tags.NoSpdOverride) {
    Seq(
      s"(e:$label)",
      s"()-[e:$relType]->()"
    ).foreach { pattern =>
      withClue(pattern) {
        // WHEN
        val exception = the[CantCompileQueryException] thrownBy {
          execute(
            s"""CYPHER 25 ALTER CURRENT GRAPH TYPE SET {
               | CONSTRAINT $constraintName FOR $pattern REQUIRE e.$prop IS UNIQUE
               |}""".stripMargin
          )
        }

        // THEN
        exception should be(gqlException(
          "51N27: 'ALTER CURRENT GRAPH TYPE SET' is not supported in community edition.",
          gqlStatus(
            GqlStatusInfoCodes.STATUS_51N27,
            "error: system configuration or operation exception - not supported in this edition. " +
              "'ALTER CURRENT GRAPH TYPE SET' is not supported in community edition."
          )
        ))
        graph.constraintExists(constraintName) should be(false)
      }
    }
  }

  test("Create key constraints through graph type", Tags.NoSpdOverride) {
    Seq(
      s"(e:$label)",
      s"()-[e:$relType]->()"
    ).foreach { pattern =>
      withClue(pattern) {
        // WHEN
        val exception = the[CantCompileQueryException] thrownBy {
          execute(
            s"""CYPHER 25 ALTER CURRENT GRAPH TYPE SET {
               | CONSTRAINT $constraintName FOR $pattern REQUIRE e.$prop IS KEY
               |}""".stripMargin
          )
        }

        // THEN
        exception should be(gqlException(
          "51N27: 'ALTER CURRENT GRAPH TYPE SET' is not supported in community edition.",
          gqlStatus(
            GqlStatusInfoCodes.STATUS_51N27,
            "error: system configuration or operation exception - not supported in this edition. " +
              "'ALTER CURRENT GRAPH TYPE SET' is not supported in community edition."
          )
        ))
        graph.constraintExists(constraintName) should be(false)
      }
    }
  }

  test("Create property existence constraints through graph type", Tags.NoSpdOverride) {
    Seq(
      s"(:$label => {$prop :: ANY NOT NULL})",
      s"()-[:$relType => {$prop :: ANY NOT NULL}]->()",
      s"CONSTRAINT $constraintName FOR (e:$label) REQUIRE e.$prop IS NOT NULL",
      s"CONSTRAINT $constraintName FOR ()-[e:$relType]->() REQUIRE e.$prop IS NOT NULL"
    ).foreach { constraint =>
      withClue(constraint) {
        // WHEN
        val exception = the[CantCompileQueryException] thrownBy {
          execute(
            s"""CYPHER 25 ALTER CURRENT GRAPH TYPE SET {
               | $constraint
               |}""".stripMargin
          )
        }

        // THEN
        exception should be(gqlException(
          "51N27: 'ALTER CURRENT GRAPH TYPE SET' is not supported in community edition.",
          gqlStatus(
            GqlStatusInfoCodes.STATUS_51N27,
            "error: system configuration or operation exception - not supported in this edition. " +
              "'ALTER CURRENT GRAPH TYPE SET' is not supported in community edition."
          )
        ))
      }
    }
  }

  test("Create property type constraints through graph type", Tags.NoSpdOverride) {
    Seq(
      s"(:$label => {$prop :: STRING})",
      s"()-[:$relType => {$prop :: STRING}]->()",
      s"CONSTRAINT $constraintName FOR (e:$label) REQUIRE e.$prop IS :: STRING",
      s"CONSTRAINT $constraintName FOR ()-[e:$relType]->() REQUIRE e.$prop IS :: STRING"
    ).foreach { constraint =>
      withClue(constraint) {
        // WHEN
        val exception = the[CantCompileQueryException] thrownBy {
          execute(
            s"""CYPHER 25 ALTER CURRENT GRAPH TYPE SET {
               | $constraint
               |}""".stripMargin
          )
        }

        // THEN
        exception should be(gqlException(
          "51N27: 'ALTER CURRENT GRAPH TYPE SET' is not supported in community edition.",
          gqlStatus(
            GqlStatusInfoCodes.STATUS_51N27,
            "error: system configuration or operation exception - not supported in this edition. " +
              "'ALTER CURRENT GRAPH TYPE SET' is not supported in community edition."
          )
        ))
      }
    }
  }

  test("Create label existence constraints through graph type", Tags.NoSpdOverride) {
    // WHEN
    val exception = the[CantCompileQueryException] thrownBy {
      execute(
        s"""CYPHER 25 ALTER CURRENT GRAPH TYPE SET {
           | (:$label => :$relType)
           |}""".stripMargin
      )
    }

    // THEN
    exception should be(gqlException(
      "51N27: 'ALTER CURRENT GRAPH TYPE SET' is not supported in community edition.",
      gqlStatus(
        GqlStatusInfoCodes.STATUS_51N27,
        "error: system configuration or operation exception - not supported in this edition. " +
          "'ALTER CURRENT GRAPH TYPE SET' is not supported in community edition."
      )
    ))
  }

  test("Create label endpoint constraints through graph type", Tags.NoSpdOverride) {
    Seq(
      s"(:$label)-[:$relType =>]->()",
      s"()-[:$relType =>]->(:$label)"
    ).foreach { constraint =>
      withClue(constraint) {
        // WHEN
        val exception = the[CantCompileQueryException] thrownBy {
          execute(
            s"""CYPHER 25 ALTER CURRENT GRAPH TYPE SET {
               | $constraint
               |}""".stripMargin
          )
        }

        // THEN
        exception should be(gqlException(
          "51N27: 'ALTER CURRENT GRAPH TYPE SET' is not supported in community edition.",
          gqlStatus(
            GqlStatusInfoCodes.STATUS_51N27,
            "error: system configuration or operation exception - not supported in this edition. " +
              "'ALTER CURRENT GRAPH TYPE SET' is not supported in community edition."
          )
        ))
      }
    }
  }

  test("Show current graph type", Tags.NoSpdOverride) {
    Seq(
      "SHOW CURRENT GRAPH TYPE",
      "SHOW CURRENT GRAPH TYPE AS GRAPH"
    ).foreach(query => {
      // WHEN
      val exception = the[RuntimeUnsupportedException] thrownBy {
        execute(s"CYPHER 25 $query")
      }

      // THEN
      exception should be(gqlException(
        s"51N27: '$query' is not supported in community edition.",
        gqlStatus(
          GqlStatusInfoCodes.STATUS_51N27,
          "error: system configuration or operation exception - not supported in this edition. " +
            s"'$query' is not supported in community edition."
        )
      ))
      val cause = exception.getCause
      cause should not be null
      cause shouldBe a[CantCompileQueryException]
      cause.asInstanceOf[CantCompileQueryException] should be(gqlException(
        s"51N27: '$query' is not supported in community edition.",
        gqlStatus(
          GqlStatusInfoCodes.STATUS_51N27,
          "error: system configuration or operation exception - not supported in this edition. " +
            s"'$query' is not supported in community edition."
        )
      ))
    })
  }

  test("Graph type commands are not available in Cypher 5") {
    Seq("SET", "DROP", "ADD", "ALTER").foreach(operation =>
      withClue(operation) {
        // WHEN
        val exception = the[SyntaxException] thrownBy {
          execute(s"CYPHER 5 ALTER CURRENT GRAPH TYPE $operation {}")
        }

        // THEN
        exception should be(gqlException(
          "Invalid input 'GRAPH': expected 'USER SET PASSWORD FROM'",
          InvalidSyntaxStatus.withCause(gqlStatus(
            GqlStatusInfoCodes.STATUS_42I06,
            "error: syntax error or access rule violation - invalid input. Invalid input 'GRAPH', expected: 'USER SET PASSWORD FROM'."
          ).withCause(Reparsesable_42I67)),
          fuzzyMsg = true
        ))
      }
    )

    // WHEN
    val exception = the[SyntaxException] thrownBy {
      execute(s"CYPHER 5 SHOW CURRENT GRAPH TYPE")
    }

    // THEN
    exception should be(
      gqlException(
        "Invalid input 'GRAPH': expected 'USER'",
        InvalidSyntaxStatus.withCause(gqlStatus(
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input 'GRAPH', expected: 'USER'."
        ).withCause(Reparsesable_42I67)),
        fuzzyMsg = true
      )
    )
  }
}
