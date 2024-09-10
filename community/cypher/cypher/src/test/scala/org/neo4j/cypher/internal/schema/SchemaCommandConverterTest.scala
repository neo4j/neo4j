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
import org.neo4j.cypher.internal.expressions.FunctionName
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.ListLiteral
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.expressions.Parameter
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.expressions.functions.Labels
import org.neo4j.cypher.internal.expressions.functions.Type
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTString
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.graphdb.schema.ConstraintType
import org.neo4j.internal.schema.AllIndexProviderDescriptors.FULLTEXT_DESCRIPTOR
import org.neo4j.internal.schema.AllIndexProviderDescriptors.POINT_DESCRIPTOR
import org.neo4j.internal.schema.AllIndexProviderDescriptors.RANGE_DESCRIPTOR
import org.neo4j.internal.schema.AllIndexProviderDescriptors.TEXT_V1_DESCRIPTOR
import org.neo4j.internal.schema.AllIndexProviderDescriptors.TEXT_V2_DESCRIPTOR
import org.neo4j.internal.schema.AllIndexProviderDescriptors.TOKEN_DESCRIPTOR
import org.neo4j.internal.schema.AllIndexProviderDescriptors.VECTOR_V1_DESCRIPTOR
import org.neo4j.internal.schema.AllIndexProviderDescriptors.VECTOR_V2_DESCRIPTOR
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
import org.neo4j.kernel.impl.api.index.IndexProviderNotFoundException
import org.neo4j.values.storable.Values
import org.scalatest.matchers.MatchResult
import org.scalatest.matchers.Matcher

import java.util

import scala.jdk.CollectionConverters.IterableHasAsJava

class SchemaCommandConverterTest extends CypherFunSuite {

  private val v1VectorConverter = new SchemaCommandConverter(CypherVersion.Default, VectorIndexVersion.V1_0)
  private val converter = new SchemaCommandConverter(CypherVersion.Default, VectorIndexVersion.V2_0)

  private val VECTOR_CONFIG_V1 = IndexConfig.`with`(util.Map.of(
    "vector.dimensions",
    Values.intValue(768),
    "vector.similarity_function",
    Values.stringValue("COSINE")
  ))

  private val VECTOR_CONFIG_V2 = IndexConfig.`with`(util.Map.of(
    "vector.hnsw.ef_construction",
    Values.intValue(100),
    "vector.hnsw.m",
    Values.intValue(16),
    "vector.quantization.enabled",
    Values.booleanValue(true),
    "vector.similarity_function",
    Values.stringValue("COSINE")
  ))

  private val VECTOR_CONFIG_V2_ALT = IndexConfig.`with`(util.Map.of(
    "vector.dimensions",
    Values.intValue(768),
    "vector.hnsw.ef_construction",
    Values.intValue(100),
    "vector.hnsw.m",
    Values.intValue(8),
    "vector.quantization.enabled",
    Values.booleanValue(true),
    "vector.similarity_function",
    Values.stringValue("COSINE")
  ))

  private val v = Variable("v")(InputPosition.NONE)

  private val label = labelName("L")

  private val relType = relTypeName("R")

  private val parameter = ExplicitParameter("p", CTString)(InputPosition.NONE)
  private val parameterMap = ExplicitParameter("p", CTMap)(InputPosition.NONE)

  private val array60 = Values.doubleArray(Array(60.0, 60.0))
  private val array40 = Values.doubleArray(Array(-40.0, -40.0))

  Seq("", "my_index").foreach {
    ixName =>
      test(s"CREATE INDEX $ixName FOR (v:L) ON (v.name)") {
        assert(converter.apply(rangeNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodeRange(commandName(ixName), label.name, asList("name"), false, util.Optional.empty()))
      }

      test(s"CREATE INDEX $ixName IF NOT EXISTS FOR (v:L) ON (v.name)") {
        assert(converter.apply(rangeNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new NodeRange(commandName(ixName), label.name, asList("name"), true, util.Optional.empty()))
      }

      test(s"CREATE INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {}") {
        assert(converter.apply(rangeNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)
        )) == new NodeRange(commandName(ixName), label.name, asList("name"), false, util.Optional.empty()))
      }

      test(s"CREATE INDEX $ixName FOR (v:L) ON (v.name1, v.name2)") {
        assert(converter.apply(rangeNodeIndex(
          List(prop("name1"), prop("name2")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodeRange(commandName(ixName), label.name, asList("name1", "name2"), false, util.Optional.empty()))
      }

      test(s"CREATE INDEX $ixName IF NOT EXISTS FOR (v:L) ON (v.name) OPTIONS {indexProvider : 'range-1.0'}") {
        assert(converter.apply(rangeNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.OptionsMap(Map("indexProvider" -> literalString("range-1.0")))
        )) == new NodeRange(commandName(ixName), label.name, asList("name"), true, util.Optional.of(RANGE_DESCRIPTOR)))
      }

      test(s"CREATE LOOKUP INDEX $ixName FOR (v) ON EACH labels(v)") {
        assert(converter.apply(lookupNodeIndex(
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodeLookup(commandName(ixName), false, util.Optional.empty()))
      }

      test(s"CREATE LOOKUP INDEX $ixName IF NOT EXISTS FOR (v) ON EACH labels(v)") {
        assert(converter.apply(lookupNodeIndex(
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new NodeLookup(commandName(ixName), true, util.Optional.empty()))
      }

      test(s"CREATE LOOKUP INDEX $ixName FOR (v) ON EACH labels(v) OPTIONS {}") {
        assert(converter.apply(lookupNodeIndex(
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)
        )) == new NodeLookup(commandName(ixName), false, util.Optional.empty()))
      }

      test(s"CREATE LOOKUP INDEX $ixName FOR (v) ON EACH labels(v) OPTIONS {indexProvider : 'token-lookup-1.0'}") {
        assert(converter.apply(lookupNodeIndex(
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("token-lookup-1.0")))
        )) == new NodeLookup(commandName(ixName), false, util.Optional.of(TOKEN_DESCRIPTOR)))
      }

      test(s"CREATE TEXT INDEX $ixName FOR (v:L) ON (v.name)") {
        assert(converter.apply(textNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodeText(commandName(ixName), label.name, "name", false, util.Optional.empty()))
      }

      test(s"CREATE TEXT INDEX $ixName IF NOT EXISTS FOR (v:L) ON (v.name)") {
        assert(converter.apply(textNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new NodeText(commandName(ixName), label.name, "name", true, util.Optional.empty()))
      }

      test(s"CREATE TEXT INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {}") {
        assert(converter.apply(textNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)
        )) == new NodeText(commandName(ixName), label.name, "name", false, util.Optional.empty()))
      }

      test(s"CREATE TEXT INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexProvider : 'text-1.0'}") {
        assert(converter.apply(textNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("text-1.0")))
        )) == new NodeText(commandName(ixName), label.name, "name", false, util.Optional.of(TEXT_V1_DESCRIPTOR)))
      }

      test(s"CREATE TEXT INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexProvider : 'text-2.0'}") {
        assert(converter.apply(textNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("text-2.0")))
        )) == new NodeText(commandName(ixName), label.name, "name", false, util.Optional.of(TEXT_V2_DESCRIPTOR)))
      }

      test(s"CREATE POINT INDEX $ixName FOR (v:L) ON (v.name)") {
        assert(converter.apply(pointNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodePoint(commandName(ixName), label.name, "name", false, util.Optional.empty(), IndexConfig.empty()))
      }

      test(s"CREATE POINT INDEX $ixName IF NOT EXISTS FOR (v:L) ON (v.name)") {
        assert(converter.apply(pointNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new NodePoint(commandName(ixName), label.name, "name", true, util.Optional.empty(), IndexConfig.empty()))
      }

      test(s"CREATE POINT INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {}") {
        assert(converter.apply(pointNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)
        )) == new NodePoint(commandName(ixName), label.name, "name", false, util.Optional.empty(), IndexConfig.empty()))
      }

      test(s"CREATE POINT INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexProvider : 'point-1.0'}") {
        assert(converter.apply(pointNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("point-1.0")))
        )) == new NodePoint(
          commandName(ixName),
          label.name,
          "name",
          false,
          util.Optional.of(POINT_DESCRIPTOR),
          IndexConfig.empty()
        ))
      }

      test(
        s"CREATE POINT INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}}"
      ) {
        assert(converter.apply(pointNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
            "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
          )))
        )) == new NodePoint(
          commandName(ixName),
          label.name,
          "name",
          false,
          util.Optional.empty(),
          IndexConfig.`with`(util.Map.of("spatial.wgs-84.max", array60, "spatial.wgs-84.min", array40))
        ))
      }

      test(
        s"CREATE POINT INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexProvider : 'point-1.0', indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}}"
      ) {
        assert(converter.apply(pointNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("point-1.0"),
            "indexConfig" -> mapOf(
              "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
              "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
            )
          ))
        )) == new NodePoint(
          commandName(ixName),
          label.name,
          "name",
          false,
          util.Optional.of(POINT_DESCRIPTOR),
          IndexConfig.`with`(util.Map.of("spatial.wgs-84.max", array60, "spatial.wgs-84.min", array40))
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.name)") {
        assert(converter.apply(vectorNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new NodeVector(commandName(ixName), label.name, "name", false, util.Optional.empty(), VECTOR_CONFIG_V2))
      }

      test(s"CREATE VECTOR INDEX $ixName IF NOT EXISTS FOR (v:L) ON (v.name)") {
        assert(converter.apply(vectorNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new NodeVector(commandName(ixName), label.name, "name", true, util.Optional.empty(), VECTOR_CONFIG_V2))
      }

      test(s"CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {}") {
        assert(converter.apply(vectorNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)
        )) == new NodeVector(commandName(ixName), label.name, "name", false, util.Optional.empty(), VECTOR_CONFIG_V2))
      }

      test(s"CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexConfig : {`vector.dimensions`: 1536}}") {
        assert(converter.apply(vectorNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "vector.dimensions" -> literalInt(1536)
          )))
        )) == new NodeVector(
          commandName(ixName),
          label.name,
          "name",
          false,
          util.Optional.empty(),
          VECTOR_CONFIG_V2.withIfAbsent("vector.dimensions", Values.intValue(1536))
        ))
      }

      test(
        s"CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexConfig : {`vector.dimensions`: 768, `vector.hnsw.m`:8}}"
      ) {
        assert(converter.apply(vectorNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "vector.dimensions" -> literalInt(768),
            "vector.hnsw.m" -> literalInt(8)
          )))
        )) == new NodeVector(
          commandName(ixName),
          label.name,
          "name",
          false,
          util.Optional.empty(),
          VECTOR_CONFIG_V2_ALT
        ))
      }

      test(
        s"CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.v1name) OPTIONS {indexProvider : 'vector-1.0',indexConfig : {`vector.dimensions`: 768, `vector.similarity_function`:'COSINE'}}"
      ) {
        assert(v1VectorConverter.apply(vectorNodeIndex(
          List(prop("v1name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("vector-1.0"),
            "indexConfig" -> mapOf(
              "vector.dimensions" -> literalInt(768),
              "vector.similarity_function" -> literalString("COSINE")
            )
          ))
        )) == new NodeVector(
          commandName(ixName),
          label.name,
          "v1name",
          false,
          util.Optional.of(VECTOR_V1_DESCRIPTOR),
          VECTOR_CONFIG_V1
        ))
      }

      test(
        s"CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.v2name) OPTIONS {indexProvider : 'vector-1.0',indexConfig : {`vector.dimensions`: 768, `vector.similarity_function`:'COSINE'}}"
      ) {
        assert(converter.apply(vectorNodeIndex(
          List(prop("v2name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("vector-1.0"),
            "indexConfig" -> mapOf(
              "vector.dimensions" -> literalInt(768),
              "vector.similarity_function" -> literalString("COSINE")
            )
          ))
        )) == new NodeVector(
          commandName(ixName),
          label.name,
          "v2name",
          false,
          util.Optional.of(VECTOR_V1_DESCRIPTOR),
          VECTOR_CONFIG_V1
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName FOR (v:L) ON (v.name) OPTIONS {indexProvider : 'vector-2.0'}") {
        assert(converter.apply(vectorNodeIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("vector-2.0")))
        )) == new NodeVector(
          commandName(ixName),
          label.name,
          "name",
          false,
          util.Optional.of(VECTOR_V2_DESCRIPTOR),
          VECTOR_CONFIG_V2
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR (v:L) ON EACH [v.name]") {
        assert(converter.apply(fulltextNodeIndex(
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
          util.Optional.empty(),
          IndexConfig.empty()
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName IF NOT EXISTS FOR (v:L) ON EACH [v.name]") {
        assert(converter.apply(fulltextNodeIndex(
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
          util.Optional.empty(),
          IndexConfig.empty()
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName IF NOT EXISTS FOR (v:L1|L2) ON EACH [v.name]") {
        assert(converter.apply(fulltextNodeIndex(
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
          util.Optional.empty(),
          IndexConfig.empty()
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR (v:L) ON EACH [v.name1, v.name2]") {
        assert(converter.apply(fulltextNodeIndex(
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
          util.Optional.empty(),
          IndexConfig.empty()
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName IF NOT EXISTS FOR (v:L1|L2) ON EACH [v.name1, v.name2]") {
        assert(converter.apply(fulltextNodeIndex(
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
          util.Optional.empty(),
          IndexConfig.empty()
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR (v:L) ON EACH [v.name] OPTIONS {}") {
        assert(converter.apply(fulltextNodeIndex(
          List(prop("name")),
          List(label.name),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)
        )) == new NodeFulltext(
          commandName(ixName),
          asList(label.name),
          asList("name"),
          false,
          util.Optional.empty(),
          IndexConfig.empty()
        ))
      }

      test(
        s"CREATE FULLTEXT INDEX $ixName FOR (v:L) ON EACH [v.name] OPTIONS {indexConfig : {`fulltext.eventually_consistent`: false}}"
      ) {
        assert(converter.apply(fulltextNodeIndex(
          List(prop("name")),
          List(label.name),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "fulltext.eventually_consistent" -> literalBoolean(false)
          )))
        )) == new NodeFulltext(
          commandName(ixName),
          asList(label.name),
          asList("name"),
          false,
          util.Optional.empty(),
          IndexConfig.empty().withIfAbsent("fulltext.eventually_consistent", Values.booleanValue(false))
        ))
      }

      test(
        s"CREATE FULLTEXT INDEX $ixName FOR (v:L) ON EACH [v.name] OPTIONS {indexProvider : 'fulltext-1.0', indexConfig : {`fulltext.eventually_consistent`: true}}"
      ) {
        assert(converter.apply(fulltextNodeIndex(
          List(prop("name")),
          List(label.name),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("fulltext-1.0"),
            "indexConfig" -> mapOf(
              "fulltext.eventually_consistent" -> literalBoolean(true)
            )
          ))
        )) == new NodeFulltext(
          commandName(ixName),
          asList(label.name),
          asList("name"),
          false,
          util.Optional.of(FULLTEXT_DESCRIPTOR),
          IndexConfig.empty().withIfAbsent("fulltext.eventually_consistent", Values.booleanValue(true))
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR (v:L) ON EACH [v.name,v.name]") {
        val error = intercept[SchemaCommandReaderException] {
          converter.apply(fulltextNodeIndex(
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
          converter.apply(fulltextNodeIndex(
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
        assert(converter.apply(rangeRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipRange(commandName(ixName), relType.name, asList("name"), false, util.Optional.empty()))
      }

      test(s"CREATE INDEX $ixName IF NOT EXISTS FOR ()-[v:R]-() ON (v.name)") {
        assert(converter.apply(rangeRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new RelationshipRange(commandName(ixName), relType.name, asList("name"), true, util.Optional.empty()))
      }

      test(s"CREATE INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {}") {
        assert(converter.apply(rangeRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)
        )) == new RelationshipRange(commandName(ixName), relType.name, asList("name"), false, util.Optional.empty()))
      }

      test(s"CREATE INDEX $ixName FOR ()-[v:R]-() ON (v.name1, v.nam2)") {
        assert(converter.apply(rangeRelIndex(
          List(prop("name1"), prop("name2")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipRange(
          commandName(ixName),
          relType.name,
          asList("name1", "name2"),
          false,
          util.Optional.empty()
        ))
      }

      test(s"CREATE INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {indexProvider : 'range-1.0'}") {
        assert(converter.apply(rangeRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("range-1.0")))
        )) == new RelationshipRange(
          commandName(ixName),
          relType.name,
          asList("name"),
          false,
          util.Optional.of(RANGE_DESCRIPTOR)
        ))
      }

      test(s"CREATE TEXT INDEX $ixName FOR ()-[v:R]-() ON (v.name)") {
        assert(converter.apply(textRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipText(commandName(ixName), relType.name, "name", false, util.Optional.empty()))
      }

      test(s"CREATE TEXT INDEX $ixName IF NOT EXISTS FOR ()-[v:R]-() ON (v.name)") {
        assert(converter.apply(textRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new RelationshipText(commandName(ixName), relType.name, "name", true, util.Optional.empty()))
      }

      test(s"CREATE TEXT INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {}") {
        assert(converter.apply(textRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)
        )) == new RelationshipText(commandName(ixName), relType.name, "name", false, util.Optional.empty()))
      }

      test(s"CREATE TEXT INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {indexProvider : 'text-1.0'}") {
        assert(converter.apply(textRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("text-1.0")))
        )) == new RelationshipText(
          commandName(ixName),
          relType.name,
          "name",
          false,
          util.Optional.of(TEXT_V1_DESCRIPTOR)
        ))
      }

      test(s"CREATE TEXT INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {indexProvider : 'text-2.0'}") {
        assert(converter.apply(textRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("text-2.0")))
        )) == new RelationshipText(
          commandName(ixName),
          relType.name,
          "name",
          false,
          util.Optional.of(TEXT_V2_DESCRIPTOR)
        ))
      }

      test(s"CREATE POINT INDEX $ixName FOR ()-[v:R]-() ON (v.name)") {
        assert(converter.apply(pointRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipPoint(
          commandName(ixName),
          relType.name,
          "name",
          false,
          util.Optional.empty(),
          IndexConfig.empty()
        ))
      }

      test(s"CREATE POINT INDEX $ixName IF NOT EXISTS FOR ()-[v:R]-() ON (v.name)") {
        assert(converter.apply(pointRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new RelationshipPoint(
          commandName(ixName),
          relType.name,
          "name",
          true,
          util.Optional.empty(),
          IndexConfig.empty()
        ))
      }

      test(s"CREATE POINT INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {}") {
        assert(converter.apply(pointRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)
        )) == new RelationshipPoint(
          commandName(ixName),
          relType.name,
          "name",
          false,
          util.Optional.empty(),
          IndexConfig.empty()
        ))
      }

      test(s"CREATE POINT INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {indexProvider : 'point-1.0'}") {
        assert(converter.apply(pointRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("point-1.0")))
        )) == new RelationshipPoint(
          commandName(ixName),
          relType.name,
          "name",
          false,
          util.Optional.of(POINT_DESCRIPTOR),
          IndexConfig.empty()
        ))
      }

      test(
        s"CREATE POINT INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}}"
      ) {
        assert(converter.apply(pointRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
            "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
          )))
        )) == new RelationshipPoint(
          commandName(ixName),
          relType.name,
          "name",
          false,
          util.Optional.empty(),
          IndexConfig.`with`(util.Map.of("spatial.wgs-84.max", array60, "spatial.wgs-84.min", array40))
        ))
      }

      test(
        s"CREATE POINT INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {indexProvider : 'point-1.0', indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}}"
      ) {
        assert(converter.apply(pointRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("point-1.0"),
            "indexConfig" -> mapOf(
              "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
              "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
            )
          ))
        )) == new RelationshipPoint(
          commandName(ixName),
          relType.name,
          "name",
          false,
          util.Optional.of(POINT_DESCRIPTOR),
          IndexConfig.`with`(util.Map.of("spatial.wgs-84.max", array60, "spatial.wgs-84.min", array40))
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName FOR ()-[v:R]-() ON (v.name)") {
        assert(converter.apply(vectorRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipVector(
          commandName(ixName),
          relType.name,
          "name",
          false,
          util.Optional.empty(),
          VECTOR_CONFIG_V2
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName IF NOT EXISTS FOR ()-[v:R]-() ON (v.name)") {
        assert(converter.apply(vectorRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new RelationshipVector(
          commandName(ixName),
          relType.name,
          "name",
          true,
          util.Optional.empty(),
          VECTOR_CONFIG_V2
        ))
      }

      test(s"CREATE VECTOR INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {}") {
        assert(converter.apply(vectorRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)
        )) == new RelationshipVector(
          commandName(ixName),
          relType.name,
          "name",
          false,
          util.Optional.empty(),
          VECTOR_CONFIG_V2
        ))
      }

      test(
        s"CREATE VECTOR INDEX $ixName FOR ()-[v:R]-() ON (v.name) OPTIONS {indexConfig : {`vector.dimensions`: 1536}}"
      ) {
        assert(converter.apply(vectorRelIndex(
          List(prop("name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "vector.dimensions" -> literalInt(1536)
          )))
        )) == new RelationshipVector(
          commandName(ixName),
          relType.name,
          "name",
          false,
          util.Optional.empty(),
          VECTOR_CONFIG_V2.withIfAbsent("vector.dimensions", Values.intValue(1536))
        ))
      }

      test(
        s"CREATE VECTOR INDEX $ixName FOR ()-[v:R]-() ON (v.v1name) OPTIONS {indexProvider : 'vector-1.0',indexConfig : {`vector.dimensions`: 768, `vector.similarity_function`:'COSINE'}}"
      ) {
        assert(v1VectorConverter.apply(vectorRelIndex(
          List(prop("v1name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("vector-1.0"),
            "indexConfig" -> mapOf(
              "vector.dimensions" -> literalInt(768),
              "vector.similarity_function" -> literalString("COSINE")
            )
          ))
        )) == new RelationshipVector(
          commandName(ixName),
          relType.name,
          "v1name",
          false,
          util.Optional.of(VECTOR_V1_DESCRIPTOR),
          VECTOR_CONFIG_V1
        ))
      }

      test(
        s"CREATE VECTOR INDEX $ixName FOR ()-[v:R]-() ON (v.v2name) OPTIONS {indexProvider : 'vector-1.0',indexConfig : {`vector.dimensions`: 768, `vector.similarity_function`:'COSINE'}}"
      ) {
        assert(converter.apply(vectorRelIndex(
          List(prop("v2name")),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("vector-1.0"),
            "indexConfig" -> mapOf(
              "vector.dimensions" -> literalInt(768),
              "vector.similarity_function" -> literalString("COSINE")
            )
          ))
        )) == new RelationshipVector(
          commandName(ixName),
          relType.name,
          "v2name",
          false,
          util.Optional.of(VECTOR_V1_DESCRIPTOR),
          VECTOR_CONFIG_V1
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR ()-[v:R]-() ON EACH [v.name]") {
        assert(converter.apply(fulltextRelIndex(
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
          util.Optional.empty(),
          IndexConfig.empty()
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName IF NOT EXISTS FOR ()-[v:R]-() ON EACH [v.name]") {
        assert(converter.apply(fulltextRelIndex(
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
          util.Optional.empty(),
          IndexConfig.empty()
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR ()-[v:R]-() ON EACH [v.name1, v.name2]") {
        assert(converter.apply(fulltextRelIndex(
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
          util.Optional.empty(),
          IndexConfig.empty()
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR ()-[v:R]-() ON EACH [v.name] OPTIONS {}") {
        assert(converter.apply(fulltextRelIndex(
          List(prop("name")),
          List(relType.name),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)
        )) == new RelationshipFulltext(
          commandName(ixName),
          asList(relType.name),
          asList("name"),
          false,
          util.Optional.empty(),
          IndexConfig.empty()
        ))
      }

      test(
        s"CREATE FULLTEXT INDEX $ixName FOR ()-[v:R]-() ON EACH [v.name] OPTIONS {indexConfig : {`fulltext.eventually_consistent`: false}}"
      ) {
        assert(converter.apply(fulltextRelIndex(
          List(prop("name")),
          List(relType.name),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf(
            "fulltext.eventually_consistent" -> literalBoolean(false)
          )))
        )) == new RelationshipFulltext(
          commandName(ixName),
          asList(relType.name),
          asList("name"),
          false,
          util.Optional.empty(),
          IndexConfig.empty().withIfAbsent("fulltext.eventually_consistent", Values.booleanValue(false))
        ))
      }

      test(
        s"CREATE FULLTEXT INDEX $ixName FOR ()-[v:R]-() ON EACH [v.name] OPTIONS {indexProvider : 'fulltext-1.0', indexConfig : {`fulltext.eventually_consistent`: true}}"
      ) {
        assert(converter.apply(fulltextRelIndex(
          List(prop("name")),
          List(relType.name),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("fulltext-1.0"),
            "indexConfig" -> mapOf(
              "fulltext.eventually_consistent" -> literalBoolean(true)
            )
          ))
        )) == new RelationshipFulltext(
          commandName(ixName),
          asList(relType.name),
          asList("name"),
          false,
          util.Optional.of(FULLTEXT_DESCRIPTOR),
          IndexConfig.empty().withIfAbsent("fulltext.eventually_consistent", Values.booleanValue(true))
        ))
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR ()-[v:R1|R2]-() ON EACH [v.name]") {
        converter.apply(fulltextRelIndex(
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
          util.Optional.of(FULLTEXT_DESCRIPTOR),
          IndexConfig.empty()
        )
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR ()-[v:R1|R2]-() ON EACH [v.name1,v.name1]") {
        converter.apply(fulltextRelIndex(
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
          util.Optional.of(FULLTEXT_DESCRIPTOR),
          IndexConfig.empty()
        )
      }

      test(s"CREATE FULLTEXT INDEX $ixName FOR ()-[v:R]-() ON EACH [v.name,v.name]") {
        val error = intercept[SchemaCommandReaderException] {
          converter.apply(fulltextRelIndex(
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
          converter.apply(fulltextRelIndex(
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
        assert(converter.apply(lookupRelIndex(
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == new RelationshipLookup(commandName(ixName), false, util.Optional.empty()))
      }

      test(s"CREATE LOOKUP INDEX $ixName IF NOT EXISTS FOR ()-[v:R]-() ON EACH type(v)") {
        assert(converter.apply(lookupRelIndex(
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == new RelationshipLookup(commandName(ixName), true, util.Optional.empty()))
      }

      test(s"CREATE LOOKUP INDEX $ixName FOR ()-[v:R]-() ON EACH type(v) OPTIONS {}") {
        assert(converter.apply(lookupRelIndex(
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)
        )) == new RelationshipLookup(commandName(ixName), false, util.Optional.empty()))
      }

      test(
        s"CREATE LOOKUP INDEX $ixName FOR ()-[v:R]-() ON EACH type(v) OPTIONS {indexProvider : 'token-lookup-1.0'}"
      ) {
        assert(converter.apply(lookupRelIndex(
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("token-lookup-1.0")))
        )) == new RelationshipLookup(commandName(ixName), false, util.Optional.of(TOKEN_DESCRIPTOR)))
      }
  }

  Seq(
    ("(v:L)", "node", btreeNodeIndex: CreateBTreeIndexFunction),
    ("()-[v:R]-()", "relationship", btreeRelIndex: CreateBTreeIndexFunction)
  ).foreach {
    case (pattern, entityType, createIndex: CreateBTreeIndexFunction) =>
      test(s"CREATE BTREE INDEX FOR $pattern ON (v.name)") {
        val error = intercept[SchemaCommandReaderException] {
          converter.apply(createIndex(
            List(prop("name")),
            Some(Right(parameter)),
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf(
          "Parameters are not allowed to be used as a btree",
          entityType,
          "index name in import schema commands"
        )
      }
  }

  Seq(
    ("(v)", "labels(v)", "node", lookupNodeIndex: CreateLookupIndexFunction),
    ("()-[v]-()", "type(v)", "relationship", lookupRelIndex: CreateLookupIndexFunction)
  ).foreach {
    case (pattern, suffix, entityType, createIndex: CreateLookupIndexFunction) =>
      test(s"CREATE LOOKUP INDEX $$boom FOR $pattern ON EACH $suffix") {
        val error = intercept[SchemaCommandReaderException] {
          converter.apply(createIndex(
            Some(Right(parameter)),
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

      test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $suffix OPTIONS {indexProvider : 'duff'}") {
        val error = intercept[IndexProviderNotFoundException] {
          converter.apply(createIndex(
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("duff")))
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
          test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $suffix OPTIONS {indexProvider : '$indexName'}") {
            val error = intercept[SchemaCommandReaderException] {
              converter.apply(createIndex(
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexProvider" -> literalString(indexName)))
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

      test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $suffix OPTIONS {duff : 13}") {
        val error = intercept[InvalidArgumentsException] {
          converter.apply(createIndex(
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "duff" -> literalInt(13)
            ))
          ))
        }
        error.getMessage should includeAllOf(
          "Failed to create token lookup index: Invalid option provided, valid options are `indexProvider` and `indexConfig`"
        )
      }

      Seq(
        "$$options",
        "{indexProvider: $providerParam}",
        "{indexConfig: $configMapParam}",
        "{indexConfig: {`index.setting.name`: $configSettingValueParam}}"
      ).foreach { optionsText =>
        test(s"CREATE LOOKUP INDEX FOR $pattern ON EACH $suffix OPTIONS $optionsText") {
          val error = intercept[SchemaCommandReaderException] {
            converter.apply(createIndex(
              None,
              ast.IfExistsThrowError,
              ast.OptionsParam(parameterMap)
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
          converter.apply(createIndex(
            List(prop("name")),
            Some(Right(parameter)),
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
          converter.apply(createIndex(
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

      test(s"CREATE INDEX FOR $pattern ON (v.name) OPTIONS {indexProvider : 'duff'}") {
        val error = intercept[IndexProviderNotFoundException] {
          converter.apply(createIndex(
            List(prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("duff")))
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
          test(s"CREATE INDEX FOR $pattern ON (v.name) OPTIONS {indexProvider : '$indexName'}") {
            val error = intercept[SchemaCommandReaderException] {
              converter.apply(createIndex(
                List(prop("name")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexProvider" -> literalString(indexName)))
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
          converter.apply(createIndex(
            List(prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexConfig" -> mapOf(
                "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
                "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
              )
            ))
          ))
        }
        error.getMessage should includeAllOf(
          "Could not create range",
          entityType,
          "property index with specified index config",
          "contains spatial config settings options"
        )
      }

      test(s"CREATE INDEX FOR $pattern ON (v.name) OPTIONS {duff : 13}") {
        val error = intercept[InvalidArgumentsException] {
          converter.apply(createIndex(
            List(prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "duff" -> literalInt(13)
            ))
          ))
        }
        error.getMessage should includeAllOf(
          "Failed to create range",
          entityType,
          "property index: Invalid option provided, valid options are `indexProvider` and `indexConfig`"
        )
      }

      Seq(
        "$$options",
        "{indexProvider: $providerParam}",
        "{indexConfig: $configMapParam}",
        "{indexConfig: {`index.setting.name`: $configSettingValueParam}}"
      ).foreach { optionsText =>
        test(s"CREATE INDEX FOR $pattern ON (v.name) OPTIONS $optionsText") {
          val error = intercept[SchemaCommandReaderException] {
            converter.apply(createIndex(
              List(prop("name")),
              None,
              ast.IfExistsThrowError,
              ast.OptionsParam(parameterMap)
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
          converter.apply(createIndex(
            List(prop("name")),
            Some(Right(parameter)),
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
          converter.apply(createIndex(
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
          converter.apply(createIndex(
            List(prop("name1"), prop("name2")),
            None,
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf("Expected only a single property")
      }

      test(s"CREATE POINT INDEX FOR $pattern ON (v.name) OPTIONS {indexProvider : 'duff'}") {
        val error = intercept[IndexProviderNotFoundException] {
          converter.apply(createIndex(
            List(prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("duff")))
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
          test(s"CREATE POINT INDEX FOR $pattern ON (v.name) OPTIONS {indexProvider : '$indexName'}") {
            val error = intercept[SchemaCommandReaderException] {
              converter.apply(createIndex(
                List(prop("name")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexProvider" -> literalString(indexName)))
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

      test(s"CREATE POINT INDEX FOR $pattern ON (v.name) OPTIONS {duff : 13}") {
        val error = intercept[InvalidArgumentsException] {
          converter.apply(createIndex(
            List(prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "duff" -> literalInt(13)
            ))
          ))
        }
        error.getMessage should includeAllOf(
          "Failed to create point index: Invalid option provided, valid options are `indexProvider` and `indexConfig`"
        )
      }

      Seq(
        "$$options",
        "{indexProvider: $providerParam}",
        "{indexConfig: $configMapParam}",
        "{indexConfig: {`index.setting.name`: $configSettingValueParam}}"
      ).foreach { optionsText =>
        test(s"CREATE POINT INDEX FOR $pattern ON (v.name) OPTIONS $optionsText") {
          val error = intercept[SchemaCommandReaderException] {
            converter.apply(createIndex(
              List(prop("name")),
              None,
              ast.IfExistsThrowError,
              ast.OptionsParam(parameterMap)
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
          converter.apply(createIndex(
            List(prop("name")),
            Some(Right(parameter)),
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
          converter.apply(createIndex(
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
          converter.apply(createIndex(
            List(prop("name1"), prop("name2")),
            None,
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf("Expected only a single property")
      }

      test(s"CREATE TEXT INDEX FOR $pattern ON (v.name) OPTIONS {indexProvider : 'duff'}") {
        val error = intercept[IndexProviderNotFoundException] {
          converter.apply(createIndex(
            List(prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("duff")))
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
          test(s"CREATE TEXT INDEX FOR $pattern ON (v.name) OPTIONS {indexProvider : '$indexName'}") {
            val error = intercept[SchemaCommandReaderException] {
              converter.apply(createIndex(
                List(prop("name")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexProvider" -> literalString(indexName)))
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

      test(s"CREATE TEXT INDEX FOR $pattern ON (v.name) OPTIONS {duff : 13}") {
        val error = intercept[InvalidArgumentsException] {
          converter.apply(createIndex(
            List(prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "duff" -> literalInt(13)
            ))
          ))
        }
        error.getMessage should includeAllOf(
          "Failed to create text index: Invalid option provided, valid options are `indexProvider` and `indexConfig`"
        )
      }

      Seq(
        "$$options",
        "{indexProvider: $providerParam}",
        "{indexConfig: $configMapParam}",
        "{indexConfig: {`index.setting.name`: $configSettingValueParam}}"
      ).foreach { optionsText =>
        test(s"CREATE TEXT INDEX FOR $pattern ON (v.name) OPTIONS $optionsText") {
          val error = intercept[SchemaCommandReaderException] {
            converter.apply(createIndex(
              List(prop("name")),
              None,
              ast.IfExistsThrowError,
              ast.OptionsParam(parameterMap)
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
          converter.apply(createIndex(
            List(prop("name")),
            Some(Right(parameter)),
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
          converter.apply(createIndex(
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
          converter.apply(createIndex(
            List(prop("name1"), prop("name2")),
            None,
            ast.IfExistsThrowError,
            ast.NoOptions
          ))
        }
        error.getMessage should includeAllOf("Expected only a single property")
      }

      test(s"CREATE VECTOR INDEX FOR $pattern ON (v.name) OPTIONS {indexProvider : 'duff'}") {
        val error = intercept[IndexProviderNotFoundException] {
          converter.apply(createIndex(
            List(prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("duff")))
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
          test(s"CREATE VECTOR INDEX FOR $pattern ON (v.name) OPTIONS {indexProvider : '$indexName'}") {
            val error = intercept[SchemaCommandReaderException] {
              converter.apply(createIndex(
                List(prop("name")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexProvider" -> literalString(indexName)))
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

      test(s"CREATE VECTOR INDEX FOR $pattern ON (v.name) OPTIONS {duff : 13}") {
        val error = intercept[InvalidArgumentsException] {
          converter.apply(createIndex(
            List(prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "duff" -> literalInt(13)
            ))
          ))
        }
        error.getMessage should includeAllOf(
          "Failed to create vector index: Invalid option provided, valid options are `indexProvider` and `indexConfig`"
        )
      }

      Seq(
        "$$options",
        "{indexProvider: $providerParam}",
        "{indexConfig: $configMapParam}",
        "{indexConfig: {`index.setting.name`: $configSettingValueParam}}"
      ).foreach { optionsText =>
        test(s"CREATE VECTOR INDEX FOR $pattern ON (v.name) OPTIONS $optionsText") {
          val error = intercept[SchemaCommandReaderException] {
            converter.apply(createIndex(
              List(prop("name")),
              None,
              ast.IfExistsThrowError,
              ast.OptionsParam(parameterMap)
            ))
          }
          error.getMessage should includeAllOf("Parameterised options are not allowed in import schema commands")
        }
      }

      test(
        s"CREATE VECTOR INDEX FOR $pattern ON (v.name) OPTIONS { indexConfig : {`vector.dimensions`: 50, `vector.quantization.enabled`: true }"
      ) {
        val error = intercept[InvalidArgumentsException] {
          v1VectorConverter.apply(createIndex(
            List(prop("name")),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "indexConfig" -> mapOf(
                "vector.dimensions" -> literalInt(1536),
                "vector.quantization.enabled" -> literalBoolean(true)
              )
            ))
          ))
        }
        error.getMessage should includeAllOf(
          "Could not create vector index with specified index config '{vector.dimensions: 1536, vector.quantization.enabled: true}'",
          "'vector.quantization.enabled' is an unrecognized setting. Supported: [vector.dimensions, vector.similarity_function]"
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
          converter.apply(createIndex(
            List(prop("name")),
            List(entity),
            Some(Right(parameter)),
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

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [v.name] OPTIONS {indexProvider : 'duff'}") {
        val error = intercept[IndexProviderNotFoundException] {
          converter.apply(createIndex(
            List(prop("name")),
            List(entity),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map("indexProvider" -> literalString("duff")))
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
          test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [v.name] OPTIONS {indexProvider : '$indexName'}") {
            val error = intercept[SchemaCommandReaderException] {
              converter.apply(createIndex(
                List(prop("name")),
                List(entity),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexProvider" -> literalString(indexName)))
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

      test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [v.name] OPTIONS {duff : 13}") {
        val error = intercept[InvalidArgumentsException] {
          converter.apply(createIndex(
            List(prop("name")),
            List(entity),
            None,
            ast.IfExistsThrowError,
            ast.OptionsMap(Map(
              "duff" -> literalInt(13)
            ))
          ))
        }
        error.getMessage should includeAllOf(
          "Failed to create fulltext index: Invalid option provided, valid options are `indexProvider` and `indexConfig`"
        )
      }

      Seq(
        "$$options",
        "{indexProvider: $providerParam}",
        "{indexConfig: $configMapParam}",
        "{indexConfig: {`index.setting.name`: $configSettingValueParam}}"
      ).foreach { optionsText =>
        test(s"CREATE FULLTEXT INDEX FOR $pattern ON EACH [v.name] OPTIONS $optionsText") {
          val error = intercept[SchemaCommandReaderException] {
            converter.apply(createIndex(
              List(prop("name")),
              List(entity),
              None,
              ast.IfExistsThrowError,
              ast.OptionsParam(parameterMap)
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
        assert(converter.apply(createConstraint(
          prop("name"),
          indexName(ixName),
          ast.IfExistsThrowError,
          ast.NoOptions
        )) == (constraintType match {
          case ConstraintType.UNIQUENESS => new NodeUniqueness(
              commandName(ixName),
              label.name,
              asList("name"),
              false,
              util.Optional.empty()
            )
          case ConstraintType.NODE_PROPERTY_EXISTENCE => new NodeExistence(
              commandName(ixName),
              label.name,
              "name",
              false
            )
          case ConstraintType.NODE_PROPERTY_TYPE => new NodePropertyType(
              commandName(ixName),
              label.name,
              "name",
              PropertyTypeSet.of(SchemaValueType.STRING),
              false
            )
          case ConstraintType.NODE_KEY => new NodeKey(
              commandName(ixName),
              label.name,
              asList("name"),
              false,
              util.Optional.empty()
            )
          case ConstraintType.RELATIONSHIP_UNIQUENESS => new RelationshipUniqueness(
              commandName(ixName),
              relType.name,
              asList("name"),
              false,
              util.Optional.empty()
            )
          case ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE => new RelationshipExistence(
              commandName(ixName),
              relType.name,
              "name",
              false
            )
          case ConstraintType.RELATIONSHIP_PROPERTY_TYPE => new RelationshipPropertyType(
              commandName(ixName),
              relType.name,
              "name",
              PropertyTypeSet.of(SchemaValueType.STRING),
              false
            )
          case ConstraintType.RELATIONSHIP_KEY => new RelationshipKey(
              commandName(ixName),
              relType.name,
              asList("name"),
              false,
              util.Optional.empty()
            )
        }))
      }

      test(s"CREATE CONSTRAINT $ixName IF NOT EXISTS FOR $pattern REQUIRE v.name IS $suffix") {
        assert(converter.apply(createConstraint(
          prop("name"),
          indexName(ixName),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )) == (constraintType match {
          case ConstraintType.UNIQUENESS => new NodeUniqueness(
              commandName(ixName),
              label.name,
              asList("name"),
              true,
              util.Optional.empty()
            )
          case ConstraintType.NODE_PROPERTY_EXISTENCE => new NodeExistence(
              commandName(ixName),
              label.name,
              "name",
              true
            )
          case ConstraintType.NODE_PROPERTY_TYPE => new NodePropertyType(
              commandName(ixName),
              label.name,
              "name",
              PropertyTypeSet.of(SchemaValueType.STRING),
              true
            )
          case ConstraintType.NODE_KEY => new NodeKey(
              commandName(ixName),
              label.name,
              asList("name"),
              true,
              util.Optional.empty()
            )
          case ConstraintType.RELATIONSHIP_UNIQUENESS => new RelationshipUniqueness(
              commandName(ixName),
              relType.name,
              asList("name"),
              true,
              util.Optional.empty()
            )
          case ConstraintType.RELATIONSHIP_PROPERTY_EXISTENCE => new RelationshipExistence(
              commandName(ixName),
              relType.name,
              "name",
              true
            )
          case ConstraintType.RELATIONSHIP_PROPERTY_TYPE => new RelationshipPropertyType(
              commandName(ixName),
              relType.name,
              "name",
              PropertyTypeSet.of(SchemaValueType.STRING),
              true
            )
          case ConstraintType.RELATIONSHIP_KEY => new RelationshipKey(
              commandName(ixName),
              relType.name,
              asList("name"),
              true,
              util.Optional.empty()
            )
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
          converter.apply(createConstraint(
            prop("name"),
            Some(Right(parameter)),
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
            converter.apply(createConstraint(
              prop("name"),
              indexName("my_index"),
              ast.IfExistsThrowError,
              ast.OptionsParam(parameterMap)
            ))
          }
          error.getMessage should includeAllOf("Parameterised options are not allowed in import schema commands")
        }
      }
  }

  type CreateConstraintFunction = (
    Property,
    Option[Either[String, Parameter]],
    ast.IfExistsDo,
    ast.Options
  ) => ast.CreateConstraint

  private def uniquenessNodeConstraint(
    prop: Property,
    name: Option[Either[String, Parameter]],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateConstraint =
    ast.CreateConstraint.createNodePropertyUniquenessConstraint(
      v,
      label,
      List(prop),
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def uniquenessRelConstraint(
    prop: Property,
    name: Option[Either[String, Parameter]],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateConstraint =
    ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
      v,
      relType,
      List(prop),
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def existenceNodeConstraint(
    prop: Property,
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateConstraint =
    ast.CreateConstraint.createNodeKeyConstraint(
      v,
      label,
      List(prop),
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def keyRelConstraint(
    prop: Property,
    name: Option[Either[String, Parameter]],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateConstraint =
    ast.CreateConstraint.createRelationshipKeyConstraint(
      v,
      relType,
      List(prop),
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  type CreateBTreeIndexFunction = (
    List[Property],
    Option[Either[String, Parameter]],
    ast.IfExistsDo,
    ast.Options
  ) => ast.CreateIndex

  private def btreeNodeIndex(
    props: List[Property],
    name: Option[Either[String, Parameter]],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createBtreeNodeIndex(
      v,
      label,
      props,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def btreeRelIndex(
    props: List[Property],
    name: Option[Either[String, Parameter]],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createBtreeRelationshipIndex(
      v,
      relType,
      props,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  type CreateIndexFunction = (
    List[Property],
    Option[Either[String, Parameter]],
    ast.IfExistsDo,
    ast.Options
  ) => ast.CreateIndex

  private def rangeNodeIndex(
    props: List[Property],
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    (Option[Either[String, Parameter]], ast.IfExistsDo, ast.Options) => ast.CreateIndex

  private def lookupNodeIndex(
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    Option[Either[String, Parameter]],
    ast.IfExistsDo,
    ast.Options
  ) => ast.CreateIndex

  private def fulltextNodeIndex(
    props: List[Property],
    labels: List[String],
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
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
    name: Option[Either[String, Parameter]],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createVectorNodeIndex(
      v,
      label,
      props,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def vectorRelIndex(
    props: List[Property],
    name: Option[Either[String, Parameter]],
    ifExistsDo: ast.IfExistsDo,
    options: ast.Options
  ): ast.CreateIndex =
    ast.CreateIndex.createVectorRelationshipIndex(
      v,
      relType,
      props,
      name,
      ifExistsDo,
      options
    )(InputPosition.NONE)

  private def indexName(name: String) = if (name.isBlank) None else Some(Left(name))

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
