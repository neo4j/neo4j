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
package org.neo4j.cypher.internal.schema

import org.eclipse.collections.api.factory.Lists
import org.neo4j.configuration.Config
import org.neo4j.cypher.CommunityCypherTestSuite
import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.literalBoolean
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.literalFloat
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.literalInt
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport.literalString
import org.neo4j.cypher.internal.expressions.ExplicitParameter
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.FunctionInvocation
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentOrder
import org.neo4j.cypher.internal.expressions.FunctionInvocation.ArgumentUnordered
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.util.FunctionName
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.exceptions.InvalidArgumentException
import org.neo4j.graphdb.schema.ConstraintType
import org.neo4j.internal.schema.AllIndexProviderDescriptors.DEFAULT_VECTOR_DESCRIPTOR
import org.neo4j.internal.schema.AllIndexProviderDescriptors.VECTOR_V1_DESCRIPTOR
import org.neo4j.internal.schema.AllIndexProviderDescriptors.VECTOR_V3_DESCRIPTOR
import org.neo4j.internal.schema.IndexConfig
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeExistence
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeKey
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodePropertyType
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.NodeUniqueness
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipExistence
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipKey
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipPropertyType
import org.neo4j.internal.schema.SchemaCommand.ConstraintCommand.Create.RelationshipUniqueness
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeFulltext
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeLookup
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodePoint
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeRange
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeText
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.NodeVector
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipFulltext
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipLookup
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipPoint
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipRange
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipText
import org.neo4j.internal.schema.SchemaCommand.IndexCommand.Create.RelationshipVector
import org.neo4j.internal.schema.SchemaCommand.SchemaCommandReaderException
import org.neo4j.internal.schema.constraints.PropertyTypeSet
import org.neo4j.internal.schema.constraints.SchemaValueType
import org.neo4j.kernel.api.exceptions.InvalidArgumentsException
import org.neo4j.kernel.api.impl.schema.vector.VectorIndexVersion
import org.neo4j.kernel.api.schema.vector.VectorTestUtils.VectorIndexSettings
import org.neo4j.kernel.impl.api.index.IndexProviderNotFoundException
import org.neo4j.test.LatestVersions
import org.neo4j.values.storable.Values
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import java.util

import scala.jdk.CollectionConverters.IterableHasAsJava

class SchemaCommandConverterTest extends CommunityCypherTestSuite {
  private val config = Config.defaults

  private val cypher5InvalidOptionMessage =
    "Invalid option provided, valid options are `indexProvider` and `indexConfig`"
  private val cypher25InvalidOptionMessage = "22N04: Invalid input 'duff' for 'OPTIONS'. Expected 'indexConfig'"

  class WrappedSchemaCommandConverter(cypherVersion: CypherVersion, latestVectorIndexVersion: VectorIndexVersion) {
    private val converter = new SchemaCommandConverter(config)
    def apply(command: ast.SchemaCommand) = converter.apply(command, cypherVersion, latestVectorIndexVersion)
  }

  private val vectorV1ConverterForDefaultCypherVersion =
    new WrappedSchemaCommandConverter(CypherVersion.Legacy.legacyVersion(), VectorIndexVersion.V1_0)

  private val vectorV1ConverterForCypher5 =
    new WrappedSchemaCommandConverter(CypherVersion.Cypher5, VectorIndexVersion.V1_0)

  private val vectorV1ConverterForCypher25 =
    new WrappedSchemaCommandConverter(CypherVersion.Cypher25, VectorIndexVersion.V1_0)

  private val vectorV3ConverterForDefaultCypherVersion =
    new WrappedSchemaCommandConverter(CypherVersion.Legacy.legacyVersion(), VectorIndexVersion.V3_0)

  private val vectorV3ConverterForCypher5 =
    new WrappedSchemaCommandConverter(CypherVersion.Cypher5, VectorIndexVersion.V3_0)

  private val vectorV3ConverterForCypher25 =
    new WrappedSchemaCommandConverter(CypherVersion.Cypher25, VectorIndexVersion.V3_0)

  private val converterForDefaultCypherVersion =
    new WrappedSchemaCommandConverter(CypherVersion.Legacy.legacyVersion(), LatestVersions.LATEST_VECTOR_INDEX_VERSION)

  private val converterForCypher5 =
    new WrappedSchemaCommandConverter(CypherVersion.Cypher5, LatestVersions.LATEST_VECTOR_INDEX_VERSION)

  private val converterForCypher25 =
    new WrappedSchemaCommandConverter(CypherVersion.Cypher25, LatestVersions.LATEST_VECTOR_INDEX_VERSION)

  private val VECTOR_V1_CONFIG = VectorIndexSettings.create
    .withDimensions(768)
    .withSimilarityFunction("COSINE")
    .toIndexConfigWith(VectorIndexVersion.V1_0)

  private val VECTOR_V3_CONFIG = VectorIndexSettings.create
    .toIndexConfigWith(VectorIndexVersion.V3_0)

  private val VECTOR_LATEST_CONFIG = VectorIndexSettings.create
    .toIndexConfigWith(LatestVersions.LATEST_VECTOR_INDEX_VERSION)

  private val v = Variable("v")(InputPosition.NONE, Variable.isIsolatedDefault)

  private val label = labelName("L")

  private val relType = relTypeName("R")

  private val parameter = ExplicitParameter("p", CTString)(InputPosition.NONE)
  private val parameterMap = ExplicitParameter("p", CTMap)(InputPosition.NONE)

  private val array60 = Values.doubleArray(Array(60.0, 60.0))
  private val array40 = Values.doubleArray(Array(-40.0, -40.0))

  Seq("", "my_index").foreach {
    ixName =>
      test(s"CREATE INDEX $ixName FOR (v:L) ON (v.name)") {
        assert(converterForDefaultCypherVersion.apply(rangeNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodeRange(commandName(ixName), label.name, asList("name"), false))
      }

      test(s"CREATE INDEX $ixName IF NOT EXISTS FOR (v:L) ON (v.name)") {
        assert(converterForDefaultCypherVersion.apply(rangeNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new NodeRange(commandName(ixName), label.name, asList("name"), true))
      }

      test(s"CREATE INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {}") {
        assert(converterForDefaultCypherVersion.apply(rangeNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)(InputPosition.NONE)
        )) == new NodeRange(commandName(ixName), label.name, asList("name"), false))
      }

      test(s"CREATE INDEX $ixName FOR (v:L) ON (v.name1, v.name2)") {
        assert(converterForDefaultCypherVersion.apply(rangeNodeIndex(
          List(prop("name1"), prop("name2")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodeRange(commandName(ixName), label.name, asList("name1", "name2"), false))
      }

      test(s"CYPHER 5 CREATE INDEX $ixName IF NOT EXISTS FOR (v:L) ON (v.name) OPTIONS {indexProvider : 'range-1.0'}") {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        assert(converterForCypher5.apply(rangeNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.OptionsMap(Map("indexProvider" -> literalString("range-1.0")))(InputPosition.NONE)
        )) == new NodeRange(commandName(ixName), label.name, asList("name"), true))
      }

      test(s"CREATE LOOKUP INDEX $ixName FOR (v) ON EACH labels(v)") {
        assert(converterForDefaultCypherVersion.apply(lookupNodeIndex(
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodeLookup(commandName(ixName), false))
      }

      test(s"CREATE LOOKUP INDEX $ixName IF NOT EXISTS FOR (v) ON EACH labels(v)") {
        assert(converterForDefaultCypherVersion.apply(lookupNodeIndex(
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new NodeLookup(commandName(ixName), true))
      }

      test(s"CREATE LOOKUP INDEX $ixName FOR (v) ON EACH labels(v) OPTIONS {}") {
        assert(converterForDefaultCypherVersion.apply(lookupNodeIndex(
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)(InputPosition.NONE)
        )) == new NodeLookup(commandName(ixName), false))
      }

      test(
        s"CYPHER 5 CREATE LOOKUP INDEX $ixName FOR (v) ON EACH labels(v) OPTIONS {indexProvider : 'token-lookup-1.0'}"
      ) {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        assert(converterForCypher5.apply(lookupNodeIndex(
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("token-lookup-1.0")))(InputPosition.NONE)
        )) == new NodeLookup(commandName(ixName), false))
      }

      test(s"CREATE TEXT INDEX $ixName FOR (v:L) ON (v.name)") {
        assert(converterForDefaultCypherVersion.apply(textNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodeText(commandName(ixName), label.name, "name", false))
      }

      test(s"CREATE TEXT INDEX $ixName IF NOT EXISTS FOR (v:L) ON (v.name)") {
        assert(converterForDefaultCypherVersion.apply(textNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new NodeText(commandName(ixName), label.name, "name", true))
      }

      test(s"CREATE TEXT INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {}") {
        assert(converterForDefaultCypherVersion.apply(textNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)(InputPosition.NONE)
        )) == new NodeText(commandName(ixName), label.name, "name", false))
      }

      test(s"CYPHER 5 CREATE TEXT INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexProvider : 'text-1.0'}") {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        assert(converterForCypher5.apply(textNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("text-1.0")))(InputPosition.NONE)
        )) == new NodeText(commandName(ixName), label.name, "name", false))
      }

      test(s"CYPHER 5 CREATE TEXT INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexProvider : 'text-2.0'}") {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        assert(converterForCypher5.apply(textNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("text-2.0")))(InputPosition.NONE)
        )) == new NodeText(commandName(ixName), label.name, "name", false))
      }

      test(s"CYPHER 5 CREATE TEXT INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexProvider : 'text-3.0'}") {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        assert(converterForCypher5.apply(textNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("text-3.0")))(InputPosition.NONE)
        )) == new NodeText(commandName(ixName), label.name, "name", false))
      }

      test(s"CREATE POINT INDEX $ixName FOR (v:L) ON (v.name)") {
        assert(converterForDefaultCypherVersion.apply(pointNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodePoint(commandName(ixName), label.name, "name", false, IndexConfig.empty()))
      }

      test(s"CREATE POINT INDEX $ixName IF NOT EXISTS FOR (v:L) ON (v.name)") {
        assert(converterForDefaultCypherVersion.apply(pointNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new NodePoint(commandName(ixName), label.name, "name", true, IndexConfig.empty()))
      }

      test(s"CREATE POINT INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {}") {
        assert(converterForDefaultCypherVersion.apply(pointNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)(InputPosition.NONE)
        )) == new NodePoint(commandName(ixName), label.name, "name", false, IndexConfig.empty()))
      }

      test(s"CYPHER 5 CREATE POINT INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexProvider : 'point-1.0'}") {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        assert(converterForCypher5.apply(pointNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("point-1.0")))(InputPosition.NONE)
        )) == new NodePoint(
          commandName(ixName),
          label.name,
          "name",
          false,
          IndexConfig.empty()
        ))
      }

      test(
        s"CREATE POINT INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}}"
      ) {
        assert(converterForDefaultCypherVersion.apply(pointNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
            "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
          )))(InputPosition.NONE)
        )) == new NodePoint(
          commandName(ixName),
          label.name,
          "name",
          false,
          IndexConfig.`with`(util.Map.of("spatial.wgs-84.max", array60, "spatial.wgs-84.min", array40))
        ))
      }

      test(
        s"CREATE POINT INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexConfig : {`immaterial.wgs-84.max`: [60.0,60.0], `immaterial.wgs-84.min`: [-40.0,-40.0]}}"
      ) {
        val error = the[InvalidArgumentException] thrownBy (converterForDefaultCypherVersion.apply(pointNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "immaterial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
            "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
          )))(InputPosition.NONE)
        )))
        error should have message ("Invalid index config key 'immaterial.wgs-84.max', it was not recognized as an index setting.")
        error.gqlStatusObject().gqlStatus() shouldBe "22G03"
        error.gqlStatusObject().cause().get().gqlStatus() shouldBe "22N27"
      }

      Seq(
        (
          converterForCypher5,
          s"CYPHER 5 CREATE POINT INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexProvider : 'point-1.0', indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}}",
          Some("point-1.0")
        ),
        (
          converterForCypher25,
          s"CYPHER 25 CREATE POINT INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}}",
          None
        )
      ).foreach { case (conv, query, ixProvider) =>
        test(query) {
          assert(conv.apply(pointNodeIndex(
            List(prop("name")),
            indexName(ixName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexConfig" -> mapOf(
                "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
                "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
              )
            ) ++ ixProvider.map(i => "indexProvider" -> literalString(i)))(InputPosition.NONE)
          )) == new NodePoint(
            commandName(ixName),
            label.name,
            "name",
            false,
            IndexConfig.`with`(util.Map.of("spatial.wgs-84.max", array60, "spatial.wgs-84.min", array40))
          ))
        }
      }

      test(s"CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.name)") {
        assert(converterForDefaultCypherVersion.apply(vectorNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodeVector(
          commandName(ixName),
          asList(label.name),
          "name",
          asList(),
          DEFAULT_VECTOR_DESCRIPTOR,
          false,
          VECTOR_LATEST_CONFIG
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName IF NOT EXISTS FOR (v:L) ON (v.name)") {
        assert(converterForDefaultCypherVersion.apply(vectorNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new NodeVector(
          commandName(ixName),
          asList(label.name),
          "name",
          asList(),
          DEFAULT_VECTOR_DESCRIPTOR,
          true,
          VECTOR_LATEST_CONFIG
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.name) V3") {
        assert(vectorV3ConverterForDefaultCypherVersion.apply(vectorNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodeVector(
          commandName(ixName),
          asList(label.name),
          "name",
          asList(),
          VECTOR_V3_DESCRIPTOR,
          false,
          VECTOR_V3_CONFIG
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName IF NOT EXISTS FOR (v:L) ON (v.name) V3") {
        assert(vectorV3ConverterForDefaultCypherVersion.apply(vectorNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new NodeVector(
          commandName(ixName),
          asList(label.name),
          "name",
          asList(),
          VECTOR_V3_DESCRIPTOR,
          true,
          VECTOR_V3_CONFIG
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {}") {
        assert(converterForDefaultCypherVersion.apply(vectorNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)(InputPosition.NONE)
        )) == new NodeVector(
          commandName(ixName),
          asList(label.name),
          "name",
          asList(),
          DEFAULT_VECTOR_DESCRIPTOR,
          false,
          VECTOR_LATEST_CONFIG
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexConfig : {`vector.dimensions`: 1536}}") {
        assert(converterForDefaultCypherVersion.apply(vectorNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "vector.dimensions" -> literalInt(1536)
          )))(InputPosition.NONE)
        )) == new NodeVector(
          commandName(ixName),
          asList(label.name),
          "name",
          asList(),
          DEFAULT_VECTOR_DESCRIPTOR,
          false,
          VectorIndexSettings.create
            .withDimensions(1536)
            .toIndexConfigWith(LatestVersions.LATEST_VECTOR_INDEX_VERSION)
        ))
      }

      test(
        s"CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexConfig : {`vector.dimensions`: 1536}} V3"
      ) {
        assert(vectorV3ConverterForDefaultCypherVersion.apply(vectorNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "vector.dimensions" -> literalInt(1536)
          )))(InputPosition.NONE)
        )) == new NodeVector(
          commandName(ixName),
          asList(label.name),
          "name",
          asList(),
          VECTOR_V3_DESCRIPTOR,
          false,
          VectorIndexSettings.create
            .withDimensions(1536)
            .toIndexConfigWith(VectorIndexVersion.V3_0)
        ))
      }

      test(
        s"CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexConfig : {`vector.dimensions`: 768, `vector.hnsw.m`: 8}}"
      ) {
        assert(converterForDefaultCypherVersion.apply(vectorNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "vector.dimensions" -> literalInt(768),
            "vector.hnsw.m" -> literalInt(8)
          )))(InputPosition.NONE)
        )) == new NodeVector(
          commandName(ixName),
          asList(label.name),
          "name",
          asList(),
          DEFAULT_VECTOR_DESCRIPTOR,
          false,
          VectorIndexSettings.create
            .withDimensions(768)
            .withHnswM(8)
            .toIndexConfigWith(LatestVersions.LATEST_VECTOR_INDEX_VERSION)
        ))
      }

      Seq(
        (
          vectorV1ConverterForCypher5,
          s"CYPHER 5 CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.v1name) OPTIONS {indexProvider : 'vector-1.0', indexConfig : {`vector.dimensions`: 768, `vector.similarity_function`: 'COSINE'}}",
          Some("vector-1.0")
        ),
        (
          vectorV1ConverterForCypher25,
          s"CYPHER 25 CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.v1name) OPTIONS {indexConfig : {`vector.dimensions`: 768, `vector.similarity_function`: 'COSINE'}}",
          None
        )
      ).foreach { case (conv, query, ixProvider) =>
        test(query) {
          assert(conv.apply(vectorNodeIndex(
            List(prop("v1name")),
            indexName(ixName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexConfig" -> mapOf(
                "vector.dimensions" -> literalInt(768),
                "vector.similarity_function" -> literalString("COSINE")
              )
            ) ++ ixProvider.map(i => "indexProvider" -> literalString(i)))(InputPosition.NONE)
          )) == new NodeVector(
            commandName(ixName),
            asList(label.name),
            "v1name",
            asList(),
            VECTOR_V1_DESCRIPTOR,
            false,
            VECTOR_V1_CONFIG
          ))
        }
      }

      Seq(
        (
          vectorV1ConverterForCypher5,
          s"CYPHER 5 CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.v2name) OPTIONS {indexProvider : 'vector-1.0', indexConfig: {`vector.dimensions`: 768, `vector.similarity_function`: 'COSINE'}}",
          Some("vector-1.0")
        ),
        (
          vectorV1ConverterForCypher25,
          s"CYPHER 25 CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.v2name) OPTIONS {indexConfig : {`vector.dimensions`: 768, `vector.similarity_function`: 'COSINE'}}",
          None
        )
      ).foreach { case (conv, query, ixProvider) =>
        test(query) {
          assert(conv.apply(vectorNodeIndex(
            List(prop("v2name")),
            indexName(ixName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexConfig" -> mapOf(
                "vector.dimensions" -> literalInt(768),
                "vector.similarity_function" -> literalString("COSINE")
              )
            ) ++ ixProvider.map(i => "indexProvider" -> literalString(i)))(InputPosition.NONE)
          )) == new NodeVector(
            commandName(ixName),
            asList(label.name),
            "v2name",
            asList(),
            VECTOR_V1_DESCRIPTOR,
            false,
            VECTOR_V1_CONFIG
          ))
        }
      }

      test(s"CYPHER 5 CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexProvider : 'vector-2.0'}") {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        assert(converterForCypher5.apply(vectorNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("vector-2.0")))(InputPosition.NONE)
        )) == new NodeVector(
          commandName(ixName),
          asList(label.name),
          "name",
          asList(),
          DEFAULT_VECTOR_DESCRIPTOR, // ignores legacy provider, uses latest
          false,
          VECTOR_LATEST_CONFIG // and the config parsed as if latest
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName FOR (v:L1|L2) ON (v.embedding)") {
        assert(converterForDefaultCypherVersion.apply(vectorNodeIndexMulti(
          List("L1", "L2"),
          List(prop("embedding")),
          List.empty,
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodeVector(
          commandName(ixName),
          asList("L1", "L2"),
          "embedding",
          asList(),
          DEFAULT_VECTOR_DESCRIPTOR,
          false,
          VECTOR_LATEST_CONFIG
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.embedding) WITH [v.f1, v.f2]") {
        assert(converterForDefaultCypherVersion.apply(vectorNodeIndexMulti(
          List(label.name),
          List(prop("embedding")),
          List(prop("f1"), prop("f2")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodeVector(
          commandName(ixName),
          asList(label.name),
          "embedding",
          asList("f1", "f2"),
          DEFAULT_VECTOR_DESCRIPTOR,
          false,
          VECTOR_LATEST_CONFIG
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName FOR (v:L1|L2) ON (v.embedding) WITH [v.f1, v.f2]") {
        assert(converterForDefaultCypherVersion.apply(vectorNodeIndexMulti(
          List("L1", "L2"),
          List(prop("embedding")),
          List(prop("f1"), prop("f2")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodeVector(
          commandName(ixName),
          asList("L1", "L2"),
          "embedding",
          asList("f1", "f2"),
          DEFAULT_VECTOR_DESCRIPTOR,
          false,
          VECTOR_LATEST_CONFIG
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName IF NOT EXISTS FOR (v:L1|L2) ON (v.embedding) WITH [v.f1]") {
        assert(converterForDefaultCypherVersion.apply(vectorNodeIndexMulti(
          List("L1", "L2"),
          List(prop("embedding")),
          List(prop("f1")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new NodeVector(
          commandName(ixName),
          asList("L1", "L2"),
          "embedding",
          asList("f1"),
          DEFAULT_VECTOR_DESCRIPTOR,
          true,
          VECTOR_LATEST_CONFIG
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR (v:L) ON EACH [v.name]") {
        assert(converterForDefaultCypherVersion.apply(fulltextNodeIndex(
          List(prop("name")),
          List(label.name),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodeFulltext(
          commandName(ixName),
          asList(label.name),
          asList("name"),
          false,
          IndexConfig.empty()
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName IF NOT EXISTS FOR (v:L) ON EACH [v.name]") {
        assert(converterForDefaultCypherVersion.apply(fulltextNodeIndex(
          List(prop("name")),
          List(label.name),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new NodeFulltext(
          commandName(ixName),
          asList(label.name),
          asList("name"),
          true,
          IndexConfig.empty()
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName IF NOT EXISTS FOR (v:L1|L2) ON EACH [v.name]") {
        assert(converterForDefaultCypherVersion.apply(fulltextNodeIndex(
          List(prop("name")),
          List("L1", "L2"),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new NodeFulltext(
          commandName(ixName),
          asList("L1", "L2"),
          asList("name"),
          true,
          IndexConfig.empty()
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR (v:L) ON EACH [v.name1, v.name2]") {
        assert(converterForDefaultCypherVersion.apply(fulltextNodeIndex(
          List(prop("name1"), prop("name2")),
          List(label.name),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodeFulltext(
          commandName(ixName),
          asList(label.name),
          asList("name1", "name2"),
          false,
          IndexConfig.empty()
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName IF NOT EXISTS FOR (v:L1|L2) ON EACH [v.name1, v.name2]") {
        assert(converterForDefaultCypherVersion.apply(fulltextNodeIndex(
          List(prop("name1"), prop("name2")),
          List("L1", "L2"),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new NodeFulltext(
          commandName(ixName),
          asList("L1", "L2"),
          asList("name1", "name2"),
          true,
          IndexConfig.empty()
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR (v:L) ON EACH [v.name] OPTIONS {}") {
        assert(converterForDefaultCypherVersion.apply(fulltextNodeIndex(
          List(prop("name")),
          List(label.name),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)(InputPosition.NONE)
        )) == new NodeFulltext(
          commandName(ixName),
          asList(label.name),
          asList("name"),
          false,
          IndexConfig.empty()
        ))
      }

      test(
        s"CREATE FULLTEXT INDEX $ixName FOR (v:L) ON EACH [v.name] OPTIONS {indexConfig : {`fulltext.eventually_consistent`: false}}"
      ) {
        assert(converterForDefaultCypherVersion.apply(fulltextNodeIndex(
          List(prop("name")),
          List(label.name),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "fulltext.eventually_consistent" -> literalBoolean(false)
          )))(InputPosition.NONE)
        )) == new NodeFulltext(
          commandName(ixName),
          asList(label.name),
          asList("name"),
          false,
          IndexConfig.empty().withIfAbsent("fulltext.eventually_consistent", Values.booleanValue(false))
        ))
      }

      Seq(
        (
          converterForCypher5,
          s"CYPHER 5 CREATE FULLTEXT INDEX $ixName FOR (v:L) ON EACH [v.name] OPTIONS {indexProvider : 'fulltext-1.0', indexConfig : {`fulltext.eventually_consistent`: true}}",
          Some("fulltext-1.0")
        ),
        (
          converterForCypher25,
          s"CYPHER 25 CREATE FULLTEXT INDEX $ixName FOR (v:L) ON EACH [v.name] OPTIONS {indexConfig : {`fulltext.eventually_consistent`: true}}",
          None
        ),
        (
          vectorV3ConverterForCypher5,
          s"CYPHER 5 CREATE FULLTEXT INDEX $ixName FOR (v:L) ON EACH [v.name] OPTIONS {indexProvider : 'fulltext-2.0', indexConfig : {`fulltext.eventually_consistent`: true}}",
          Some("fulltext-2.0")
        ),
        (
          vectorV3ConverterForCypher25,
          s"CYPHER 25 CREATE FULLTEXT INDEX $ixName FOR (v:L) ON EACH [v.name] OPTIONS {indexConfig : {`fulltext.eventually_consistent`: false}}",
          None
        )
      ).foreach { case (conv, query, ixProvider) =>
        test(query) {
          assert(conv.apply(fulltextNodeIndex(
            List(prop("name")),
            List(label.name),
            indexName(ixName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexConfig" -> mapOf(
                "fulltext.eventually_consistent" -> literalBoolean(true)
              )
            ) ++ ixProvider.map(i => "indexProvider" -> literalString(i)))(InputPosition.NONE)
          )) == new NodeFulltext(
            commandName(ixName),
            asList(label.name),
            asList("name"),
            false,
            IndexConfig.empty().withIfAbsent("fulltext.eventually_consistent", Values.booleanValue(true))
          ))
        }
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR (v:L) ON EACH [v.name,v.name]") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(fulltextNodeIndex(
            List(prop("name"), prop("name")),
            List(label.name),
            indexName(ixName),
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf(
          "Invalid fulltext node index as property 'name' is duplicated"
        )
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR (v:L|L) ON EACH [v.name]") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(fulltextNodeIndex(
            List(prop("name")),
            List(label.name, label.name),
            indexName(ixName),
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf(
          "Invalid fulltext node index as label 'L' is duplicated"
        )
      }

      test(s"CREATE INDEX $ixName FOR ()-[v:R]-() ON (v.name)") {
        assert(converterForDefaultCypherVersion.apply(rangeRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipRange(commandName(ixName), relType.name, asList("name"), false))
      }

      test(s"CREATE INDEX $ixName IF NOT EXISTS FOR ()-[v:R]-() ON (v.name)") {
        assert(converterForDefaultCypherVersion.apply(rangeRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new RelationshipRange(commandName(ixName), relType.name, asList("name"), true))
      }

      test(s"CREATE INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {}") {
        assert(converterForDefaultCypherVersion.apply(rangeRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)(InputPosition.NONE)
        )) == new RelationshipRange(commandName(ixName), relType.name, asList("name"), false))
      }

      test(s"CREATE INDEX $ixName FOR ()-[v:R]-() ON (v.name1, v.nam2)") {
        assert(converterForDefaultCypherVersion.apply(rangeRelIndex(
          List(prop("name1"), prop("name2")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipRange(
          commandName(ixName),
          relType.name,
          asList("name1", "name2"),
          false
        ))
      }

      test(s"CYPHER 5 CREATE INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {indexProvider : 'range-1.0'}") {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        assert(converterForCypher5.apply(rangeRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("range-1.0")))(InputPosition.NONE)
        )) == new RelationshipRange(
          commandName(ixName),
          relType.name,
          asList("name"),
          false
        ))
      }

      test(s"CREATE TEXT INDEX $ixName FOR ()-[v:R]-() ON (v.name)") {
        assert(converterForDefaultCypherVersion.apply(textRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipText(commandName(ixName), relType.name, "name", false))
      }

      test(s"CREATE TEXT INDEX $ixName IF NOT EXISTS FOR ()-[v:R]-() ON (v.name)") {
        assert(converterForDefaultCypherVersion.apply(textRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new RelationshipText(commandName(ixName), relType.name, "name", true))
      }

      test(s"CREATE TEXT INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {}") {
        assert(converterForDefaultCypherVersion.apply(textRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)(InputPosition.NONE)
        )) == new RelationshipText(commandName(ixName), relType.name, "name", false))
      }

      test(s"CYPHER 5 CREATE TEXT INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {indexProvider : 'text-1.0'}") {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        assert(converterForCypher5.apply(textRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("text-1.0")))(InputPosition.NONE)
        )) == new RelationshipText(
          commandName(ixName),
          relType.name,
          "name",
          false
        ))
      }

      test(s"CYPHER 5 CREATE TEXT INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {indexProvider : 'text-2.0'}") {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        assert(converterForCypher5.apply(textRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("text-2.0")))(InputPosition.NONE)
        )) == new RelationshipText(
          commandName(ixName),
          relType.name,
          "name",
          false
        ))
      }

      test(s"CREATE POINT INDEX $ixName FOR ()-[v:R]-() ON (v.name)") {
        assert(converterForDefaultCypherVersion.apply(pointRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipPoint(
          commandName(ixName),
          relType.name,
          "name",
          false,
          IndexConfig.empty()
        ))
      }

      test(s"CREATE POINT INDEX $ixName IF NOT EXISTS FOR ()-[v:R]-() ON (v.name)") {
        assert(converterForDefaultCypherVersion.apply(pointRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new RelationshipPoint(
          commandName(ixName),
          relType.name,
          "name",
          true,
          IndexConfig.empty()
        ))
      }

      test(s"CREATE POINT INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {}") {
        assert(converterForDefaultCypherVersion.apply(pointRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)(InputPosition.NONE)
        )) == new RelationshipPoint(
          commandName(ixName),
          relType.name,
          "name",
          false,
          IndexConfig.empty()
        ))
      }

      test(s"CYPHER 5 CREATE POINT INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {indexProvider : 'point-1.0'}") {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        assert(converterForCypher5.apply(pointRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("point-1.0")))(InputPosition.NONE)
        )) == new RelationshipPoint(
          commandName(ixName),
          relType.name,
          "name",
          false,
          IndexConfig.empty()
        ))
      }

      test(
        s"CREATE POINT INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}}"
      ) {
        assert(converterForDefaultCypherVersion.apply(pointRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
            "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
          )))(InputPosition.NONE)
        )) == new RelationshipPoint(
          commandName(ixName),
          relType.name,
          "name",
          false,
          IndexConfig.`with`(util.Map.of("spatial.wgs-84.max", array60, "spatial.wgs-84.min", array40))
        ))
      }

      Seq(
        (
          converterForCypher5,
          s"CYPHER 5 CREATE POINT INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {indexProvider : 'point-1.0', indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}}",
          Some("point-1.0")
        ),
        (
          converterForCypher25,
          s"CYPHER 25 CREATE POINT INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}}",
          None
        )
      ).foreach { case (conv, query, ixProvider) =>
        test(query) {
          assert(conv.apply(pointRelIndex(
            List(prop("name")),
            indexName(ixName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexConfig" -> mapOf(
                "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
                "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
              )
            ) ++ ixProvider.map(i => "indexProvider" -> literalString(i)))(InputPosition.NONE)
          )) == new RelationshipPoint(
            commandName(ixName),
            relType.name,
            "name",
            false,
            IndexConfig.`with`(util.Map.of("spatial.wgs-84.max", array60, "spatial.wgs-84.min", array40))
          ))
        }
      }

      test(s"CREATE VECTOR INDEX $ixName FOR ()-[v:R]-() ON (v.name)") {
        assert(converterForDefaultCypherVersion.apply(vectorRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipVector(
          commandName(ixName),
          asList(relType.name),
          "name",
          asList(),
          DEFAULT_VECTOR_DESCRIPTOR,
          false,
          VECTOR_LATEST_CONFIG
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName IF NOT EXISTS FOR ()-[v:R]-() ON (v.name)") {
        assert(converterForDefaultCypherVersion.apply(vectorRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new RelationshipVector(
          commandName(ixName),
          asList(relType.name),
          "name",
          asList(),
          DEFAULT_VECTOR_DESCRIPTOR,
          true,
          VECTOR_LATEST_CONFIG
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {}") {
        assert(converterForDefaultCypherVersion.apply(vectorRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)(InputPosition.NONE)
        )) == new RelationshipVector(
          commandName(ixName),
          asList(relType.name),
          "name",
          asList(),
          DEFAULT_VECTOR_DESCRIPTOR,
          false,
          VECTOR_LATEST_CONFIG
        ))
      }

      test(
        s"CREATE VECTOR INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {indexConfig : {`vector.dimensions`: 1536}}"
      ) {
        assert(converterForDefaultCypherVersion.apply(vectorRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "vector.dimensions" -> literalInt(1536)
          )))(InputPosition.NONE)
        )) == new RelationshipVector(
          commandName(ixName),
          asList(relType.name),
          "name",
          asList(),
          DEFAULT_VECTOR_DESCRIPTOR,
          false,
          VectorIndexSettings.create
            .withDimensions(1536)
            .toIndexConfigWith(LatestVersions.LATEST_VECTOR_INDEX_VERSION)
        ))
      }

      Seq(
        (
          vectorV1ConverterForCypher5,
          s"CYPHER 5 CREATE VECTOR INDEX $ixName FOR ()-[v:R]-() ON (v.v1name) OPTIONS {indexProvider : 'vector-1.0',indexConfig : {`vector.dimensions`: 768, `vector.similarity_function`: 'COSINE'}}",
          Some("vector-1.0")
        ),
        (
          vectorV1ConverterForCypher25,
          s"CYPHER 25 CREATE VECTOR INDEX $ixName FOR ()-[v:R]-() ON (v.v1name) OPTIONS {indexConfig : {`vector.dimensions`: 768, `vector.similarity_function`: 'COSINE'}}",
          None
        )
      ).foreach { case (conv, query, ixProvider) =>
        test(query) {
          assert(conv.apply(vectorRelIndex(
            List(prop("v1name")),
            indexName(ixName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexConfig" -> mapOf(
                "vector.dimensions" -> literalInt(768),
                "vector.similarity_function" -> literalString("COSINE")
              )
            ) ++ ixProvider.map(i => "indexProvider" -> literalString(i)))(InputPosition.NONE)
          )) == new RelationshipVector(
            commandName(ixName),
            asList(relType.name),
            "v1name",
            asList(),
            VECTOR_V1_DESCRIPTOR,
            false,
            VECTOR_V1_CONFIG
          ))
        }
      }

      Seq(
        (
          vectorV1ConverterForCypher5,
          s"CYPHER 5 CREATE VECTOR INDEX $ixName FOR ()-[v:R]-() ON (v.v2name) OPTIONS {indexProvider : 'vector-1.0',indexConfig : {`vector.dimensions`: 768, `vector.similarity_function`: 'COSINE'}}",
          Some("vector-1.0")
        ),
        (
          vectorV1ConverterForCypher25,
          s"CYPHER 25 CREATE VECTOR INDEX $ixName FOR ()-[v:R]-() ON (v.v2name) OPTIONS {indexConfig : {`vector.dimensions`: 768, `vector.similarity_function`: 'COSINE'}}",
          None
        )
      ).foreach { case (conv, query, ixProvider) =>
        test(query) {
          assert(conv.apply(vectorRelIndex(
            List(prop("v2name")),
            indexName(ixName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexConfig" -> mapOf(
                "vector.dimensions" -> literalInt(768),
                "vector.similarity_function" -> literalString("COSINE")
              )
            ) ++ ixProvider.map(i => "indexProvider" -> literalString(i)))(InputPosition.NONE)
          )) == new RelationshipVector(
            commandName(ixName),
            asList(relType.name),
            "v2name",
            asList(),
            VECTOR_V1_DESCRIPTOR,
            false,
            VECTOR_V1_CONFIG
          ))
        }
      }

      test(s"CREATE VECTOR INDEX $ixName FOR ()-[v:R1|R2]-() ON (v.embedding)") {
        assert(converterForDefaultCypherVersion.apply(vectorRelIndexMulti(
          List("R1", "R2"),
          List(prop("embedding")),
          List.empty,
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipVector(
          commandName(ixName),
          asList("R1", "R2"),
          "embedding",
          asList(),
          DEFAULT_VECTOR_DESCRIPTOR,
          false,
          VECTOR_LATEST_CONFIG
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName FOR ()-[v:R]-() ON (v.embedding) WITH [v.f1, v.f2]") {
        assert(converterForDefaultCypherVersion.apply(vectorRelIndexMulti(
          List(relType.name),
          List(prop("embedding")),
          List(prop("f1"), prop("f2")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipVector(
          commandName(ixName),
          asList(relType.name),
          "embedding",
          asList("f1", "f2"),
          DEFAULT_VECTOR_DESCRIPTOR,
          false,
          VECTOR_LATEST_CONFIG
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName FOR ()-[v:R1|R2]-() ON (v.embedding) WITH [v.f1, v.f2]") {
        assert(converterForDefaultCypherVersion.apply(vectorRelIndexMulti(
          List("R1", "R2"),
          List(prop("embedding")),
          List(prop("f1"), prop("f2")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipVector(
          commandName(ixName),
          asList("R1", "R2"),
          "embedding",
          asList("f1", "f2"),
          DEFAULT_VECTOR_DESCRIPTOR,
          false,
          VECTOR_LATEST_CONFIG
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName IF NOT EXISTS FOR ()-[v:R1|R2]-() ON (v.embedding) WITH [v.f1]") {
        assert(converterForDefaultCypherVersion.apply(vectorRelIndexMulti(
          List("R1", "R2"),
          List(prop("embedding")),
          List(prop("f1")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new RelationshipVector(
          commandName(ixName),
          asList("R1", "R2"),
          "embedding",
          asList("f1"),
          DEFAULT_VECTOR_DESCRIPTOR,
          true,
          VECTOR_LATEST_CONFIG
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR ()-[v:R]-() ON EACH [v.name]") {
        assert(converterForDefaultCypherVersion.apply(fulltextRelIndex(
          List(prop("name")),
          List(relType.name),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipFulltext(
          commandName(ixName),
          asList(relType.name),
          asList("name"),
          false,
          IndexConfig.empty()
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName IF NOT EXISTS FOR ()-[v:R]-() ON EACH [v.name]") {
        assert(converterForDefaultCypherVersion.apply(fulltextRelIndex(
          List(prop("name")),
          List(relType.name),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new RelationshipFulltext(
          commandName(ixName),
          asList(relType.name),
          asList("name"),
          true,
          IndexConfig.empty()
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR ()-[v:R]-() ON EACH [v.name1, v.name2]") {
        assert(converterForDefaultCypherVersion.apply(fulltextRelIndex(
          List(prop("name1"), prop("name2")),
          List(relType.name),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipFulltext(
          commandName(ixName),
          asList(relType.name),
          asList("name1", "name2"),
          false,
          IndexConfig.empty()
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR ()-[v:R]-() ON EACH [v.name] OPTIONS {}") {
        assert(converterForDefaultCypherVersion.apply(fulltextRelIndex(
          List(prop("name")),
          List(relType.name),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)(InputPosition.NONE)
        )) == new RelationshipFulltext(
          commandName(ixName),
          asList(relType.name),
          asList("name"),
          false,
          IndexConfig.empty()
        ))
      }

      test(
        s"CREATE FULLTEXT INDEX $ixName FOR ()-[v:R]-() ON EACH [v.name] OPTIONS {indexConfig : {`fulltext.eventually_consistent`: false}}"
      ) {
        assert(converterForDefaultCypherVersion.apply(fulltextRelIndex(
          List(prop("name")),
          List(relType.name),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "fulltext.eventually_consistent" -> literalBoolean(false)
          )))(InputPosition.NONE)
        )) == new RelationshipFulltext(
          commandName(ixName),
          asList(relType.name),
          asList("name"),
          false,
          IndexConfig.empty().withIfAbsent("fulltext.eventually_consistent", Values.booleanValue(false))
        ))
      }

      Seq(
        (
          vectorV1ConverterForCypher5,
          s"CYPHER 5 CREATE FULLTEXT INDEX $ixName FOR ()-[v:R]-() ON EACH [v.name] OPTIONS {indexProvider : 'fulltext-1.0', indexConfig : {`fulltext.eventually_consistent`: true}}",
          Some("fulltext-1.0")
        ),
        (
          vectorV1ConverterForCypher25,
          s"CYPHER 25 CREATE FULLTEXT INDEX $ixName FOR ()-[v:R]-() ON EACH [v.name] OPTIONS {indexConfig : {`fulltext.eventually_consistent`: true}}",
          None
        )
      ).foreach { case (conv, query, ixProvider) =>
        test(query) {
          assert(conv.apply(fulltextRelIndex(
            List(prop("name")),
            List(relType.name),
            indexName(ixName),
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexConfig" -> mapOf(
                "fulltext.eventually_consistent" -> literalBoolean(true)
              )
            ) ++ ixProvider.map(i => "indexProvider" -> literalString(i)))(InputPosition.NONE)
          )) == new RelationshipFulltext(
            commandName(ixName),
            asList(relType.name),
            asList("name"),
            false,
            IndexConfig.empty().withIfAbsent("fulltext.eventually_consistent", Values.booleanValue(true))
          ))
        }
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR ()-[v:R1|R2]-() ON EACH [v.name]") {
        converterForDefaultCypherVersion.apply(fulltextRelIndex(
          List(prop("name")),
          List("R1", "R2"),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipFulltext(
          commandName(ixName),
          asList("R1", "R2"),
          asList("name"),
          false,
          IndexConfig.empty()
        )
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR ()-[v:R1|R2]-() ON EACH [v.name1,v.name1]") {
        converterForDefaultCypherVersion.apply(fulltextRelIndex(
          List(prop("name1"), prop("name2")),
          List("R1", "R2"),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipFulltext(
          commandName(ixName),
          asList("R1", "R2"),
          asList("name1", "name2"),
          false,
          IndexConfig.empty()
        )
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR ()-[v:R]-() ON EACH [v.name,v.name]") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(fulltextRelIndex(
            List(prop("name"), prop("name")),
            List(relType.name),
            indexName(ixName),
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf(
          "Invalid fulltext relationship index as property 'name' is duplicated"
        )
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR ()-[v:R|R]-() ON EACH [v.name]") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(fulltextRelIndex(
            List(prop("name")),
            List(relType.name, relType.name),
            indexName(ixName),
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf(
          "Invalid fulltext relationship index as relationship 'R' is duplicated"
        )
      }

      test(s"CREATE LOOKUP INDEX $ixName FOR ()-[v:R]-() ON EACH type(v)") {
        assert(converterForDefaultCypherVersion.apply(lookupRelIndex(
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipLookup(commandName(ixName), false))
      }

      test(s"CREATE LOOKUP INDEX $ixName IF NOT EXISTS FOR ()-[v:R]-() ON EACH type(v)") {
        assert(converterForDefaultCypherVersion.apply(lookupRelIndex(
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new RelationshipLookup(commandName(ixName), true))
      }

      test(s"CREATE LOOKUP INDEX $ixName FOR ()-[v:R]-() ON EACH type(v) OPTIONS {}") {
        assert(converterForDefaultCypherVersion.apply(lookupRelIndex(
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)(InputPosition.NONE)
        )) == new RelationshipLookup(commandName(ixName), false))
      }

      test(
        s"CYPHER 5 CREATE LOOKUP INDEX $ixName FOR ()-[v:R]-() ON EACH type(v) OPTIONS {indexProvider : 'token-lookup-1.0'}"
      ) {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        assert(converterForCypher5.apply(lookupRelIndex(
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("token-lookup-1.0")))(InputPosition.NONE)
        )) == new RelationshipLookup(commandName(ixName), false))
      }
  }

  Seq(
    ("(v)", "labels(v)", "node", lookupNodeIndex: CreateLookupIndexFunction),
    ("()-[v]-()", "type(v)", "relationship", lookupRelIndex: CreateLookupIndexFunction)
  ).foreach {
    case (pattern, suffix, entityType, createIndex: CreateLookupIndexFunction) =>
      test(s"CREATE LOOKUP INDEX $$boom FOR $pattern ON EACH $suffix") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(createIndex(
            Some(parameter),
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf(
          "Parameters are not allowed to be used as a",
          entityType,
          "lookup index name in import schema commands"
        )
      }

      test(s"CYPHER 5 CREATE LOOKUP INDEX FOR $pattern ON EACH $suffix OPTIONS {indexProvider : 'duff'}") {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        val error = intercept[IndexProviderNotFoundException] {
          converterForCypher5.apply(createIndex(
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("duff")))(InputPosition.NONE)
          ))
        }
        error.getMessage should includeAllOf("Unable to find the IndexProviderDescriptor for the name: duff")
      }

      Seq(
        ("TEXT", "text-1.0"),
        ("VECTOR", "vector-1.0"),
        ("RANGE", "range-1.0"),
        ("POINT", "point-1.0"),
        ("FULLTEXT", "fulltext-1.0")
      ).foreach {
        case (indexType, indexName) =>
          test(s"CYPHER 5 CREATE LOOKUP INDEX FOR $pattern ON EACH $suffix OPTIONS {indexProvider : '$indexName'}") {
            // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
            val error = intercept[SchemaCommandReaderException] {
              converterForCypher5.apply(createIndex(
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexProvider" -> literalString(indexName)))(InputPosition.NONE)
              ))
            }
            error.getMessage should includeAllOf(
              "The provider '",
              indexName,
              "' of type ",
              indexType,
              " does not match the expected type of LOOKUP"
            )
          }
      }

      Seq(
        (
          CypherVersion.Cypher5,
          converterForCypher5,
          Seq("Failed to create token lookup index", cypher5InvalidOptionMessage)
        ),
        (
          CypherVersion.Cypher25,
          converterForCypher25,
          Seq(cypher25InvalidOptionMessage)
        )
      ).foreach { case (version, conv, expectedErrors) =>
        test(s"${version.description} CREATE LOOKUP INDEX FOR $pattern ON EACH $suffix OPTIONS {duff : 13}") {
          val error = intercept[InvalidArgumentsException] {
            conv.apply(createIndex(
              None,
              ast.IfExistsThrowError,
              ast.OptionsMap(Map(
                "duff" -> literalInt(13)
              ))(InputPosition.NONE)
            ))
          }
          error.getMessage should includeAllOf(expectedErrors: _*)
        }
      }

      Seq(
        "$$options",
        "{indexProvider: $providerParam}",
        "{indexConfig: $configMapParam}",
        "{indexConfig: {`index.setting.name`: $configSettingValueParam}}"
      ).foreach { optionsText =>
        test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $suffix OPTIONS $optionsText") {
          val error = intercept[SchemaCommandReaderException] {
            converterForDefaultCypherVersion.apply(createIndex(
              None,
              ast.IfExistsThrowError,
              ast.OptionsParam(parameterMap)(InputPosition.NONE)
            ))
          }
          error.getMessage should includeAllOf("Parameterised options are not allowed in import schema commands")
        }
      }
  }

  Seq(
    ("(v:L)", "node", rangeNodeIndex: CreateIndexFunction),
    ("()-[v:R]-()", "relationship", rangeRelIndex: CreateIndexFunction)
  ).foreach {
    case (pattern, entityType, createIndex: CreateIndexFunction) =>
      test(s"CREATE INDEX $$boom FOR $pattern ON (v.name)") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(createIndex(
            List(prop("name")),
            Some(parameter),
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf(
          "Parameters are not allowed to be used as a range",
          entityType,
          "property index name in import schema commands"
        )
      }

      test(s"CREATE INDEX FOR $pattern ON (v.name, v.name)") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(createIndex(
            List(prop("name"), prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf(
          "Invalid range",
          entityType,
          "property index as property 'name' is duplicated"
        )
      }

      test(s"CYPHER 5 CREATE INDEX FOR $pattern ON (v.name) OPTIONS {indexProvider : 'duff'}") {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        val error = intercept[IndexProviderNotFoundException] {
          converterForCypher5.apply(createIndex(
            List(prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("duff")))(InputPosition.NONE)
          ))
        }
        error.getMessage should includeAllOf("Unable to find the IndexProviderDescriptor for the name: duff")
      }

      Seq(
        ("TEXT", "text-1.0"),
        ("FULLTEXT", "fulltext-1.0"),
        ("POINT", "point-1.0"),
        ("VECTOR", "vector-1.0"),
        ("LOOKUP", "token-lookup-1.0")
      ).foreach {
        case (indexType, indexName) =>
          test(s"CYPHER 5 CREATE INDEX FOR $pattern ON (v.name) OPTIONS {indexProvider : '$indexName'}") {
            // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
            val error = intercept[SchemaCommandReaderException] {
              converterForCypher5.apply(createIndex(
                List(prop("name")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexProvider" -> literalString(indexName)))(InputPosition.NONE)
              ))
            }
            error.getMessage should includeAllOf(
              "The provider '",
              indexName,
              "' of type ",
              indexType,
              " does not match the expected type of RANGE"
            )
          }
      }

      test(
        s"CREATE INDEX FOR $pattern ON (v.name) OPTIONS {indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}"
      ) {
        val error = intercept[InvalidArgumentsException] {
          converterForDefaultCypherVersion.apply(createIndex(
            List(prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexConfig" -> mapOf(
                "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
                "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
              )
            ))(InputPosition.NONE)
          ))
        }
        error.getMessage should includeAllOf(
          "Could not create range",
          entityType,
          "property index with specified index config",
          "contains spatial config settings options"
        )
      }

      Seq(
        (
          CypherVersion.Cypher5,
          converterForCypher5,
          Seq("Failed to create range", entityType, cypher5InvalidOptionMessage)
        ),
        (
          CypherVersion.Cypher25,
          converterForCypher25,
          Seq(cypher25InvalidOptionMessage)
        )
      ).foreach { case (version, conv, expectedErrors) =>
        test(s"${version.description} CREATE INDEX FOR $pattern ON (v.name) OPTIONS {duff : 13}") {
          val error = intercept[InvalidArgumentsException] {
            conv.apply(createIndex(
              List(prop("name")),
              None,
              ast.IfExistsThrowError,
              ast.OptionsMap(Map(
                "duff" -> literalInt(13)
              ))(InputPosition.NONE)
            ))
          }
          error.getMessage should includeAllOf(expectedErrors: _*)
        }
      }

      Seq(
        "$$options",
        "{indexProvider: $providerParam}",
        "{indexConfig: $configMapParam}",
        "{indexConfig: {`index.setting.name`: $configSettingValueParam}}"
      ).foreach { optionsText =>
        test(s"CREATE INDEX FOR $pattern ON (v.name) OPTIONS $optionsText") {
          val error = intercept[SchemaCommandReaderException] {
            converterForDefaultCypherVersion.apply(createIndex(
              List(prop("name")),
              None,
              ast.IfExistsThrowError,
              ast.OptionsParam(parameterMap)(InputPosition.NONE)
            ))
          }
          error.getMessage should includeAllOf("Parameterised options are not allowed in import schema commands")
        }
      }
  }

  Seq(
    ("(v:L)", "node", pointNodeIndex: CreateIndexFunction),
    ("()-[v:R]-()", "relationship", pointRelIndex: CreateIndexFunction)
  ).foreach {
    case (pattern, entityType, createIndex: CreateIndexFunction) =>
      test(s"CREATE POINT INDEX $$boom FOR $pattern ON (v.name)") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(createIndex(
            List(prop("name")),
            Some(parameter),
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf(
          "Parameters are not allowed to be used as a point",
          entityType,
          "index name in import schema commands"
        )
      }

      test(s"CREATE POINT INDEX FOR $pattern ON (v.name, v.name)") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(createIndex(
            List(prop("name"), prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf("Expected only a single property")
      }

      test(s"CREATE POINT INDEX FOR $pattern ON (v.name1, v.name2)") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(createIndex(
            List(prop("name1"), prop("name2")),
            None,
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf("Expected only a single property")
      }

      test(s"CYPHER 5 CREATE POINT INDEX FOR $pattern ON (v.name) OPTIONS {indexProvider : 'duff'}") {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        val error = intercept[IndexProviderNotFoundException] {
          converterForCypher5.apply(createIndex(
            List(prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("duff")))(InputPosition.NONE)
          ))
        }
        error.getMessage should includeAllOf("Unable to find the IndexProviderDescriptor for the name: duff")
      }

      Seq(
        ("TEXT", "text-1.0"),
        ("FULLTEXT", "fulltext-1.0"),
        ("RANGE", "range-1.0"),
        ("VECTOR", "vector-1.0"),
        ("LOOKUP", "token-lookup-1.0")
      ).foreach {
        case (indexType, indexName) =>
          test(s"CYPHER 5 CREATE POINT INDEX FOR $pattern ON (v.name) OPTIONS {indexProvider : '$indexName'}") {
            // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
            val error = intercept[SchemaCommandReaderException] {
              converterForCypher5.apply(createIndex(
                List(prop("name")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexProvider" -> literalString(indexName)))(InputPosition.NONE)
              ))
            }
            error.getMessage should includeAllOf(
              "The provider '",
              indexName,
              "' of type ",
              indexType,
              " does not match the expected type of POINT"
            )
          }
      }

      Seq(
        (
          CypherVersion.Cypher5,
          converterForCypher5,
          Seq("Failed to create point index", cypher5InvalidOptionMessage)
        ),
        (
          CypherVersion.Cypher25,
          converterForCypher25,
          Seq(cypher25InvalidOptionMessage)
        )
      ).foreach { case (version, conv, expectedErrors) =>
        test(s"${version.description} CREATE POINT INDEX FOR $pattern ON (v.name) OPTIONS {duff : 13}") {
          val error = intercept[InvalidArgumentsException] {
            conv.apply(createIndex(
              List(prop("name")),
              None,
              ast.IfExistsThrowError,
              ast.OptionsMap(Map(
                "duff" -> literalInt(13)
              ))(InputPosition.NONE)
            ))
          }
          error.getMessage should includeAllOf(expectedErrors: _*)
        }
      }

      Seq(
        "$$options",
        "{indexProvider: $providerParam}",
        "{indexConfig: $configMapParam}",
        "{indexConfig: {`index.setting.name`: $configSettingValueParam}}"
      ).foreach { optionsText =>
        test(s"CREATE POINT INDEX FOR $pattern ON (v.name) OPTIONS $optionsText") {
          val error = intercept[SchemaCommandReaderException] {
            converterForDefaultCypherVersion.apply(createIndex(
              List(prop("name")),
              None,
              ast.IfExistsThrowError,
              ast.OptionsParam(parameterMap)(InputPosition.NONE)
            ))
          }
          error.getMessage should includeAllOf("Parameterised options are not allowed in import schema commands")
        }
      }
  }

  Seq(
    ("(v:L)", "node", textNodeIndex: CreateIndexFunction),
    ("()-[v:R]-()", "relationship", textRelIndex: CreateIndexFunction)
  ).foreach {
    case (pattern, entityType, createIndex: CreateIndexFunction) =>
      test(s"CREATE TEXT INDEX $$boom FOR $pattern ON (v.name)") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(createIndex(
            List(prop("name")),
            Some(parameter),
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf(
          "Parameters are not allowed to be used as a text",
          entityType,
          "index name in import schema commands"
        )
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (v.name, v.name)") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(createIndex(
            List(prop("name"), prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf("Expected only a single property")
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (v.name1, v.name2)") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(createIndex(
            List(prop("name1"), prop("name2")),
            None,
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf("Expected only a single property")
      }

      test(s"CYPHER 5 CREATE TEXT INDEX FOR $pattern ON (v.name) OPTIONS {indexProvider : 'duff'}") {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        val error = intercept[IndexProviderNotFoundException] {
          converterForCypher5.apply(createIndex(
            List(prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("duff")))(InputPosition.NONE)
          ))
        }
        error.getMessage should includeAllOf("Unable to find the IndexProviderDescriptor for the name: duff")
      }

      Seq(
        ("POINT", "point-1.0"),
        ("FULLTEXT", "fulltext-1.0"),
        ("RANGE", "range-1.0"),
        ("VECTOR", "vector-1.0"),
        ("LOOKUP", "token-lookup-1.0")
      ).foreach {
        case (indexType, indexName) =>
          test(s"CYPHER 5 CREATE TEXT INDEX FOR $pattern ON (v.name) OPTIONS {indexProvider : '$indexName'}") {
            // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
            val error = intercept[SchemaCommandReaderException] {
              converterForCypher5.apply(createIndex(
                List(prop("name")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexProvider" -> literalString(indexName)))(InputPosition.NONE)
              ))
            }
            error.getMessage should includeAllOf(
              "The provider '",
              indexName,
              "' of type ",
              indexType,
              " does not match the expected type of TEXT"
            )
          }
      }

      Seq(
        (
          CypherVersion.Cypher5,
          converterForCypher5,
          Seq("Failed to create text index", cypher5InvalidOptionMessage)
        ),
        (
          CypherVersion.Cypher25,
          converterForCypher25,
          Seq(cypher25InvalidOptionMessage)
        )
      ).foreach { case (version, conv, expectedErrors) =>
        test(s"${version.description} CREATE TEXT INDEX FOR $pattern ON (v.name) OPTIONS {duff : 13}") {
          val error = intercept[InvalidArgumentsException] {
            conv.apply(createIndex(
              List(prop("name")),
              None,
              ast.IfExistsThrowError,
              ast.OptionsMap(Map(
                "duff" -> literalInt(13)
              ))(InputPosition.NONE)
            ))
          }
          error.getMessage should includeAllOf(expectedErrors: _*)
        }
      }

      Seq(
        "$$options",
        "{indexProvider: $providerParam}",
        "{indexConfig: $configMapParam}",
        "{indexConfig: {`index.setting.name`: $configSettingValueParam}}"
      ).foreach { optionsText =>
        test(s"CREATE TEXT INDEX FOR $pattern ON (v.name) OPTIONS $optionsText") {
          val error = intercept[SchemaCommandReaderException] {
            converterForDefaultCypherVersion.apply(createIndex(
              List(prop("name")),
              None,
              ast.IfExistsThrowError,
              ast.OptionsParam(parameterMap)(InputPosition.NONE)
            ))
          }
          error.getMessage should includeAllOf("Parameterised options are not allowed in import schema commands")
        }
      }
  }

  Seq(
    ("(v:L)", "node", vectorNodeIndex: CreateIndexFunction),
    ("()-[v:R]-()", "relationship", vectorRelIndex: CreateIndexFunction)
  ).foreach {
    case (pattern, entityType, createIndex: CreateIndexFunction) =>
      test(s"CREATE VECTOR INDEX $$boom FOR $pattern ON (v.name)") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(createIndex(
            List(prop("name")),
            Some(parameter),
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf(
          "Parameters are not allowed to be used as a vector",
          entityType,
          "index name in import schema commands"
        )
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON (v.name, v.name)") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(createIndex(
            List(prop("name"), prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf("Expected only a single property")
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON (v.name1, v.name2)") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(createIndex(
            List(prop("name1"), prop("name2")),
            None,
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf("Expected only a single property")
      }

      test(s"CYPHER 5 CREATE VECTOR INDEX FOR $pattern ON (v.name) OPTIONS {indexProvider : 'duff'}") {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        val error = intercept[IndexProviderNotFoundException] {
          converterForCypher5.apply(createIndex(
            List(prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("duff")))(InputPosition.NONE)
          ))
        }
        error.getMessage should includeAllOf("Unable to find the IndexProviderDescriptor for the name: duff")
      }

      Seq(
        ("TEXT", "text-1.0"),
        ("FULLTEXT", "fulltext-1.0"),
        ("RANGE", "range-1.0"),
        ("POINT", "point-1.0"),
        ("LOOKUP", "token-lookup-1.0")
      ).foreach {
        case (indexType, indexName) =>
          test(s"CYPHER 5 CREATE VECTOR INDEX FOR $pattern ON (v.name) OPTIONS {indexProvider : '$indexName'}") {
            // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
            val error = intercept[SchemaCommandReaderException] {
              converterForCypher5.apply(createIndex(
                List(prop("name")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexProvider" -> literalString(indexName)))(InputPosition.NONE)
              ))
            }
            error.getMessage should includeAllOf(
              "The provider '",
              indexName,
              "' of type ",
              indexType,
              " does not match the expected type of VECTOR"
            )
          }
      }

      Seq(
        (
          CypherVersion.Cypher5,
          converterForCypher5,
          Seq("Failed to create vector index", cypher5InvalidOptionMessage)
        ),
        (
          CypherVersion.Cypher25,
          converterForCypher25,
          Seq(cypher25InvalidOptionMessage)
        )
      ).foreach { case (version, conv, expectedErrors) =>
        test(s"${version.description} CREATE VECTOR INDEX FOR $pattern ON (v.name) OPTIONS {duff : 13}") {
          val error = intercept[InvalidArgumentsException] {
            conv.apply(createIndex(
              List(prop("name")),
              None,
              ast.IfExistsThrowError,
              ast.OptionsMap(Map(
                "duff" -> literalInt(13)
              ))(InputPosition.NONE)
            ))
          }
          error.getMessage should includeAllOf(expectedErrors: _*)
        }
      }

      Seq(
        "$$options",
        "{indexProvider: $providerParam}",
        "{indexConfig: $configMapParam}",
        "{indexConfig: {`index.setting.name`: $configSettingValueParam}}"
      ).foreach { optionsText =>
        test(s"CREATE VECTOR INDEX FOR $pattern ON (v.name) OPTIONS $optionsText") {
          val error = intercept[SchemaCommandReaderException] {
            converterForDefaultCypherVersion.apply(createIndex(
              List(prop("name")),
              None,
              ast.IfExistsThrowError,
              ast.OptionsParam(parameterMap)(InputPosition.NONE)
            ))
          }
          error.getMessage should includeAllOf("Parameterised options are not allowed in import schema commands")
        }
      }

      test(
        s"CREATE VECTOR INDEX FOR $pattern ON (v.name) OPTIONS { indexConfig : {`vector.dimensions`: 1536, `vector.quantization.type`: 'SCALAR' }"
      ) {
        val error = intercept[InvalidArgumentsException] {
          vectorV1ConverterForDefaultCypherVersion.apply(createIndex(
            List(prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexConfig" -> mapOf(
                "vector.dimensions" -> literalInt(1536),
                "vector.quantization.type" -> literalString("SCALAR")
              )
            ))(InputPosition.NONE)
          ))
        }
        error.getMessage should includeAllOf(
          "Could not create vector index with specified index config '{vector.dimensions: 1536, vector.quantization.type: \"SCALAR\"}'",
          "'vector.quantization.type' is an unrecognized setting. Supported: [vector.dimensions, vector.similarity_function]"
        )
      }
  }

  Seq(
    ("(v:L)", label.name, "node", fulltextNodeIndex: CreateFulltextIndexFunction),
    ("()-[v:R]-()", relType.name, "relationship", fulltextRelIndex: CreateFulltextIndexFunction)
  ).foreach {
    case (pattern, entity, entityType, createIndex: CreateFulltextIndexFunction) =>
      test(s"CREATE FULLTEXT INDEX $$boom FOR $pattern ON EACH [v.name]") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(createIndex(
            List(prop("name")),
            List(entity),
            Some(parameter),
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf(
          "Parameters are not allowed to be used as a fulltext",
          entityType,
          "index name in import schema commands"
        )
      }

      test(s"CYPHER 5 CREATE FULLTEXT INDEX FOR $pattern ON EACH [v.name] OPTIONS {indexProvider : 'duff'}") {
        // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
        val error = intercept[IndexProviderNotFoundException] {
          converterForCypher5.apply(createIndex(
            List(prop("name")),
            List(entity),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("duff")))(InputPosition.NONE)
          ))
        }
        error.getMessage should includeAllOf("Unable to find the IndexProviderDescriptor for the name: duff")
      }

      Seq(
        ("TEXT", "text-1.0"),
        ("VECTOR", "vector-1.0"),
        ("RANGE", "range-1.0"),
        ("POINT", "point-1.0"),
        ("LOOKUP", "token-lookup-1.0")
      ).foreach {
        case (indexType, indexName) =>
          test(s"CYPHER 5 CREATE FULLTEXT INDEX FOR $pattern ON EACH [v.name] OPTIONS {indexProvider : '$indexName'}") {
            // Note: This test is only relevant for cypher5. This is because indexProvider is removed in cypher25
            val error = intercept[SchemaCommandReaderException] {
              converterForCypher5.apply(createIndex(
                List(prop("name")),
                List(entity),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexProvider" -> literalString(indexName)))(InputPosition.NONE)
              ))
            }
            error.getMessage should includeAllOf(
              "The provider '",
              indexName,
              "' of type ",
              indexType,
              " does not match the expected type of FULLTEXT"
            )
          }
      }
      Seq(
        (
          CypherVersion.Cypher5,
          converterForCypher5,
          Seq("Failed to create fulltext index", cypher5InvalidOptionMessage)
        ),
        (
          CypherVersion.Cypher25,
          converterForCypher25,
          Seq(cypher25InvalidOptionMessage)
        )
      ).foreach { case (version, conv, expectedErrors) =>
        test(s"${version.description} CREATE FULLTEXT INDEX FOR $pattern ON EACH [v.name] OPTIONS {duff : 13}") {
          val error = intercept[InvalidArgumentsException] {
            conv.apply(createIndex(
              List(prop("name")),
              List(entity),
              None,
              ast.IfExistsThrowError,
              ast.OptionsMap(Map(
                "duff" -> literalInt(13)
              ))(InputPosition.NONE)
            ))
          }
          error.getMessage should includeAllOf(expectedErrors: _*)
        }
      }

      Seq(
        "$$options",
        "{indexProvider: $providerParam}",
        "{indexConfig: $configMapParam}",
        "{indexConfig: {`index.setting.name`: $configSettingValueParam}}"
      ).foreach { optionsText =>
        test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [v.name] OPTIONS $optionsText") {
          val error = intercept[SchemaCommandReaderException] {
            converterForDefaultCypherVersion.apply(createIndex(
              List(prop("name")),
              List(entity),
              None,
              ast.IfExistsThrowError,
              ast.OptionsParam(parameterMap)(InputPosition.NONE)
            ))
          }
          error.getMessage should includeAllOf("Parameterised options are not allowed in import schema commands")
        }
      }
  }

  Seq(
    ("(v:L)", "", "UNIQUE", ConstraintType.UNIQUENESS, uniquenessNodeConstraint: CreateConstraintFunction),
    ("(v:L)", "my_index", "UNIQUE", ConstraintType.UNIQUENESS, uniquenessNodeConstraint: CreateConstraintFunction),
    (
      "()-[v:R]-()",
      "",
      "UNIQUE",
      ConstraintType.RELATIONSHIP_UNIQUENESS,
      uniquenessRelConstraint: CreateConstraintFunction
    ),
    (
      "()-[v:R]-()",
      "my_index",
      "UNIQUE",
      ConstraintType.RELATIONSHIP_UNIQUENESS,
      uniquenessRelConstraint: CreateConstraintFunction
    ),
    (
      "(v:L)",
      "",
      "NOT NULL",
      ConstraintType.NODE_PROPERTY_EXISTENCE,
      existenceNodeConstraint: CreateConstraintFunction
    ),
    (
      "(v:L)",
      "my_index",
      "NOT NULL",
      ConstraintType.NODE_PROPERTY_EXISTENCE,
      existenceNodeConstraint: CreateConstraintFunction
    ),
    (
      "()-[v:R]-()",
      "",
      "NOT NULL",
      ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE,
      existenceRelConstraint: CreateConstraintFunction
    ),
    (
      "()-[v:R]-()",
      "my_index",
      "NOT NULL",
      ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE,
      existenceRelConstraint: CreateConstraintFunction
    ),
    ("(v:L)", "", ":: STRING", ConstraintType.NODE_PROPERTY_TYPE, propertyNodeConstraint: CreateConstraintFunction),
    (
      "(v:L)",
      "my_index",
      ":: STRING",
      ConstraintType.NODE_PROPERTY_TYPE,
      propertyNodeConstraint: CreateConstraintFunction
    ),
    (
      "()-[v:R]-()",
      "",
      ":: STRING",
      ConstraintType.RELATIONSHIP_PROPERTY_TYPE,
      propertyRelConstraint: CreateConstraintFunction
    ),
    (
      "()-[v:R]-()",
      "my_index",
      ":: STRING",
      ConstraintType.RELATIONSHIP_PROPERTY_TYPE,
      propertyRelConstraint: CreateConstraintFunction
    ),
    ("(v:L)", "", "NODE KEY", ConstraintType.NODE_KEY, keyNodeConstraint: CreateConstraintFunction),
    ("(v:L)", "my_index", "NODE KEY", ConstraintType.NODE_KEY, keyNodeConstraint: CreateConstraintFunction),
    ("()-[v:R]-()", "", "REL KEY", ConstraintType.RELATIONSHIP_KEY, keyRelConstraint: CreateConstraintFunction),
    ("()-[v:R]-()", "my_index", "REL KEY", ConstraintType.RELATIONSHIP_KEY, keyRelConstraint: CreateConstraintFunction)
  ).foreach {
    case (pattern, ixName, suffix, constraintType, createConstraint: CreateConstraintFunction) =>
      test(s"CREATE CONSTRAINT $ixName FOR $pattern REQUIRE v.name IS $suffix") {
        assert(converterForDefaultCypherVersion.apply(createConstraint(
          prop("name"),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == (constraintType match {
          case ConstraintType.UNIQUENESS => new NodeUniqueness(
              commandName(ixName),
              label.name,
              asList("name"),
              false
            )
          case ConstraintType.NODE_PROPERTY_EXISTENCE => new NodeExistence(
              commandName(ixName),
              label.name,
              "name",
              false,
              false
            )
          case ConstraintType.NODE_PROPERTY_TYPE => new NodePropertyType(
              commandName(ixName),
              label.name,
              "name",
              PropertyTypeSet.of(SchemaValueType.STRING),
              false,
              false
            )
          case ConstraintType.NODE_KEY => new NodeKey(
              commandName(ixName),
              label.name,
              asList("name"),
              false
            )
          case ConstraintType.RELATIONSHIP_UNIQUENESS => new RelationshipUniqueness(
              commandName(ixName),
              relType.name,
              asList("name"),
              false
            )
          case ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE => new RelationshipExistence(
              commandName(ixName),
              relType.name,
              "name",
              false,
              false
            )
          case ConstraintType.RELATIONSHIP_PROPERTY_TYPE => new RelationshipPropertyType(
              commandName(ixName),
              relType.name,
              "name",
              PropertyTypeSet.of(SchemaValueType.STRING),
              false,
              false
            )
          case ConstraintType.RELATIONSHIP_KEY => new RelationshipKey(
              commandName(ixName),
              relType.name,
              asList("name"),
              false
            )
          case ConstraintType.RELATIONSHIP_SOURCE_LABEL => fail("Not yet supported - waiting for graph type syntax")
          case ConstraintType.RELATIONSHIP_TARGET_LABEL => fail("Not yet supported - waiting for graph type syntax")
          case ConstraintType.NODE_LABEL_EXISTENCE      => fail("Not yet supported - waiting for graph type syntax")
        }))
      }

      test(s"CREATE CONSTRAINT $ixName IF NOT EXISTS FOR $pattern REQUIRE v.name IS $suffix") {
        assert(converterForDefaultCypherVersion.apply(createConstraint(
          prop("name"),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == (constraintType match {
          case ConstraintType.UNIQUENESS => new NodeUniqueness(
              commandName(ixName),
              label.name,
              asList("name"),
              true
            )
          case ConstraintType.NODE_PROPERTY_EXISTENCE => new NodeExistence(
              commandName(ixName),
              label.name,
              "name",
              false,
              true
            )
          case ConstraintType.NODE_PROPERTY_TYPE => new NodePropertyType(
              commandName(ixName),
              label.name,
              "name",
              PropertyTypeSet.of(SchemaValueType.STRING),
              false,
              true
            )
          case ConstraintType.NODE_KEY => new NodeKey(
              commandName(ixName),
              label.name,
              asList("name"),
              true
            )
          case ConstraintType.RELATIONSHIP_UNIQUENESS => new RelationshipUniqueness(
              commandName(ixName),
              relType.name,
              asList("name"),
              true
            )
          case ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE => new RelationshipExistence(
              commandName(ixName),
              relType.name,
              "name",
              false,
              true
            )
          case ConstraintType.RELATIONSHIP_PROPERTY_TYPE => new RelationshipPropertyType(
              commandName(ixName),
              relType.name,
              "name",
              PropertyTypeSet.of(SchemaValueType.STRING),
              false,
              true
            )
          case ConstraintType.RELATIONSHIP_KEY => new RelationshipKey(
              commandName(ixName),
              relType.name,
              asList("name"),
              true
            )
          case ConstraintType.RELATIONSHIP_SOURCE_LABEL => fail("Not yet supported - waiting for graph type syntax")
          case ConstraintType.RELATIONSHIP_TARGET_LABEL => fail("Not yet supported - waiting for graph type syntax")
          case ConstraintType.NODE_LABEL_EXISTENCE      => fail("Not yet supported - waiting for graph type syntax")
        }))
      }
  }

  Seq(
    ("(v:L1)", "UNIQUE", "uniqueness", uniquenessNodeConstraint: CreateConstraintFunction),
    ("(v:L2)", "NOT NULL", "existence", existenceNodeConstraint: CreateConstraintFunction),
    ("(v:L3)", ":: STRING", "property type", propertyNodeConstraint: CreateConstraintFunction),
    ("(v:L4)", "NODE KEY", "node key", keyNodeConstraint: CreateConstraintFunction),
    ("()-[v:R1]-()", "UNIQUE", "uniqueness", uniquenessRelConstraint: CreateConstraintFunction),
    ("()-[v:R2]-()", "NOT NULL", "existence", existenceRelConstraint: CreateConstraintFunction),
    ("()-[v:R3]-()", ":: STRING", "property type", propertyRelConstraint: CreateConstraintFunction),
    ("()-[v:R4]-()", "REL KEY", "relationship key", keyRelConstraint: CreateConstraintFunction)
  ).foreach {
    case (pattern, suffix, constraintType, createConstraint: CreateConstraintFunction) =>
      test(s"CREATE CONSTRAINT $$boom FOR $pattern REQUIRE v.name IS $suffix") {
        val error = intercept[SchemaCommandReaderException] {
          converterForDefaultCypherVersion.apply(createConstraint(
            prop("name"),
            Some(parameter),
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf(
          "Parameters are not allowed to be used as a",
          constraintType,
          "name in import schema commands"
        )
      }

      Seq(
        "$$options",
        "{indexProvider: $providerParam}",
        "{indexConfig: $configMapParam}",
        "{indexConfig: {`index.setting.name`: $configSettingValueParam}}"
      ).foreach { optionsText =>
        test(s"CREATE CONSTRAINT my_index FOR $pattern REQUIRE v.name IS $suffix OPTIONS $optionsText") {
          val error = intercept[SchemaCommandReaderException] {
            converterForDefaultCypherVersion.apply(createConstraint(
              prop("name"),
              indexName("my_index"),
              ast.IfExistsThrowError,
              ast.OptionsParam(parameterMap)(InputPosition.NONE)
            ))
          }
          error.getMessage should includeAllOf("Parameterised options are not allowed in import schema commands")
        }
      }
  }

  type CreateConstraintFunction = (
    Property,
    Option[Expression],
    ast.IfExistsDo,
    ast.Options
  ) => ast.CreateConstraint

  private def uniquenessNodeConstraint(
    prop: Property,
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateConstraint =
    ast.CreateConstraint.createNodePropertyUniquenessConstraint(
      v,
      label,
      List(prop),
      name,
      ifExistsDo,
      options,
      fromCypher5 = false
    )(InputPosition.NONE)

  private def uniquenessRelConstraint(
    prop: Property,
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateConstraint =
    ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
      v,
      relType,
      List(prop),
      name,
      ifExistsDo,
      options,
      fromCypher5 = false
    )(InputPosition.NONE)

  private def existenceNodeConstraint(
    prop: Property,
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateConstraint =
    ast.CreateConstraint.createNodePropertyExistenceConstraint(
      v,
      label,
      prop,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def existenceRelConstraint(
    prop: Property,
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateConstraint =
    ast.CreateConstraint.createRelationshipPropertyExistenceConstraint(
      v,
      relType,
      prop,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def propertyNodeConstraint(
    prop: Property,
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateConstraint =
    ast.CreateConstraint.createNodePropertyTypeConstraint(
      v,
      label,
      prop,
      CTString,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def propertyRelConstraint(
    prop: Property,
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateConstraint =
    ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
      v,
      relType,
      prop,
      CTString,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def keyNodeConstraint(
    prop: Property,
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateConstraint =
    ast.CreateConstraint.createNodeKeyConstraint(
      v,
      label,
      List(prop),
      name,
      ifExistsDo,
      options,
      fromCypher5 = false
    )(InputPosition.NONE)

  private def keyRelConstraint(
    prop: Property,
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateConstraint =
    ast.CreateConstraint.createRelationshipKeyConstraint(
      v,
      relType,
      List(prop),
      name,
      ifExistsDo,
      options,
      fromCypher5 = false
    )(InputPosition.NONE)

  type CreateBTreeIndexFunction = (
    List[Property],
    Option[Expression],
    ast.IfExistsDo,
    ast.Options
  ) => ast.CreateIndex

  type CreateIndexFunction = (
    List[Property],
    Option[Expression],
    ast.IfExistsDo,
    ast.Options
  ) => ast.CreateIndex

  private def rangeNodeIndex(
    props: List[Property],
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createRangeNodeIndex(
      v,
      label,
      props,
      name,
      ifExistsDo,
      options,
      fromDefault = true
    )(InputPosition.NONE)

  private def rangeRelIndex(
    props: List[Property],
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createRangeRelationshipIndex(
      v,
      relType,
      props,
      name,
      ifExistsDo,
      options,
      fromDefault = true
    )(InputPosition.NONE)

  type CreateLookupIndexFunction =
    (Option[Expression], ast.IfExistsDo, ast.Options) => ast.CreateIndex

  private def lookupNodeIndex(
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createLookupIndex(
      v,
      isNodeIndex = true,
      function(Labels.name, v),
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def lookupRelIndex(
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createLookupIndex(
      v,
      isNodeIndex = false,
      function(Type.name, v),
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  type CreateFulltextIndexFunction = (
    List[Property],
    List[String],
    Option[Expression],
    ast.IfExistsDo,
    ast.Options
  ) => ast.CreateIndex

  private def fulltextNodeIndex(
    props: List[Property],
    labels: List[String],
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createFulltextNodeIndex(
      v,
      labels.map(labelName),
      props,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def fulltextRelIndex(
    props: List[Property],
    types: List[String],
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createFulltextRelationshipIndex(
      v,
      types.map(relTypeName),
      props,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def textNodeIndex(
    props: List[Property],
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createTextNodeIndex(
      v,
      label,
      props,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def textRelIndex(
    props: List[Property],
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createTextRelationshipIndex(
      v,
      relType,
      props,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def pointNodeIndex(
    props: List[Property],
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createPointNodeIndex(
      v,
      label,
      props,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def pointRelIndex(
    props: List[Property],
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createPointRelationshipIndex(
      v,
      relType,
      props,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def vectorNodeIndex(
    props: List[Property],
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createVectorNodeIndex(
      v,
      List(label),
      props,
      List.empty,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def vectorNodeIndexMulti(
    labels: List[String],
    props: List[Property],
    additionalProps: List[Property],
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createVectorNodeIndex(
      v,
      labels.map(labelName),
      props,
      additionalProps,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def vectorRelIndex(
    props: List[Property],
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createVectorRelationshipIndex(
      v,
      List(relType),
      props,
      List.empty,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def vectorRelIndexMulti(
    types: List[String],
    props: List[Property],
    additionalProps: List[Property],
    name: Option[Expression],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createVectorRelationshipIndex(
      v,
      types.map(relTypeName),
      props,
      additionalProps,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def indexName(name: String) = if (name.isBlank) None else Some(StringLiteral(name)(InputPosition.NONE))

  private def commandName(name: String) = if (name.isBlank) null else name

  private def labelName(name: String) = LabelName(name)(InputPosition.NONE)

  private def relTypeName(name: String) = RelTypeName(name)(InputPosition.NONE)

  private def prop(propKey: String): Property =
    Property(v, propName(propKey))(InputPosition.NONE)

  private def propName(s: String): PropertyKeyName = PropertyKeyName(s)(InputPosition.NONE)

  private def function(name: String, args: Expression*): FunctionInvocation =
    function(name, ArgumentUnordered, args: _*)

  private def function(name: String, order: ArgumentOrder, args: Expression*): FunctionInvocation =
    FunctionInvocation(
      FunctionName(name)(InputPosition.NONE),
      distinct = false,
      args.toIndexedSeq,
      order
    )(InputPosition.NONE)

  def listOf(expressions: Expression*): ListLiteral =
    ListLiteral(expressions)(InputPosition.NONE)

  def mapOf(keysAndValues: (String, Expression)*): MapExpression =
    MapExpression(keysAndValues.map {
      case (k, v) => propName(k) -> v
    })(InputPosition.NONE)

  private def asList[TYPE](items: TYPE*): util.List[TYPE] = Lists.mutable.withAll(items.asJava)

  private def includeAllOf(expectedSubstrings: String*): Matcher[String] =
    (left: String) =>
      MatchResult(
        expectedSubstrings forall left.contains,
        s"""String "$left" did not include all of those substrings: ${
            expectedSubstrings.map(s =>
              s""""$s""""
            ).mkString(", ")
          }""",
        s"""String "$left" contained all of those substrings: ${
            expectedSubstrings.map(s => s""""$s"""").mkString(
              ", "
            )
          }"""
      )
}
