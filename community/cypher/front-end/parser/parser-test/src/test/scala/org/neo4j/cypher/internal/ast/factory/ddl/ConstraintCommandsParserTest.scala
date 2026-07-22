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
package org.neo4j.cypher.internal.ast.factory.ddl

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.parser.common.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.parser.common.ast.factory.ConstraintType
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.DurationType
import org.neo4j.cypher.internal.util.symbols.Float32Type
import org.neo4j.cypher.internal.util.symbols.FloatType
import org.neo4j.cypher.internal.util.symbols.Integer16Type
import org.neo4j.cypher.internal.util.symbols.Integer32Type
import org.neo4j.cypher.internal.util.symbols.Integer8Type
import org.neo4j.cypher.internal.util.symbols.IntegerType
import org.neo4j.cypher.internal.util.symbols.ListType
import org.neo4j.cypher.internal.util.symbols.LocalDateTimeType
import org.neo4j.cypher.internal.util.symbols.LocalTimeType
import org.neo4j.cypher.internal.util.symbols.MapType
import org.neo4j.cypher.internal.util.symbols.NodeType
import org.neo4j.cypher.internal.util.symbols.NothingType
import org.neo4j.cypher.internal.util.symbols.NullType
import org.neo4j.cypher.internal.util.symbols.PathType
import org.neo4j.cypher.internal.util.symbols.PointType
import org.neo4j.cypher.internal.util.symbols.PropertyValueCypher5Type
import org.neo4j.cypher.internal.util.symbols.PropertyValueType
import org.neo4j.cypher.internal.util.symbols.RelationshipType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.VectorType
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType
import org.neo4j.cypher.internal.util.symbols.ZonedTimeType
import org.neo4j.cypher.internal.util.test_helpers.GqlExceptionMatchers.gqlStatus
import org.neo4j.exceptions.SyntaxException
import org.neo4j.gqlstatus.GqlStatusInfoCodes

/* Tests for creating and dropping constraints */
class ConstraintCommandsParserTest extends AdministrationAndSchemaCommandParserTestBase {

  override protected def ignorePrettifier: Boolean = true

  // Key and uniqueness

  Seq("NODE", "").foreach(nodeKeyword => {
    // Node key

    test(
      s"CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR (node:Label) REQUIRE node.prop IS $nodeKeyword KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          None,
          ast.IfExistsReplace,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT IF NOT EXISTS FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          None,
          ast.IfExistsDoNothing,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop1,node.prop2) IS $nodeKeyword KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop1"), prop("node", "prop2")),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS FOR (node:Label) REQUIRE (node.prop1,node.prop2) IS $nodeKeyword KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop1"), prop("node", "prop2")),
          None,
          ast.IfExistsInvalidSyntax,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword KEY OPTIONS {indexProvider : 'range-1.0'}"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("range-1.0")))(pos),
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword KEY OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}"
    ) {
      // will fail in options converter
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("native-btree-1.0"),
            "indexConfig" -> mapOf(
              "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
              "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
            )
          ))(pos),
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword KEY OPTIONS {indexConfig : {someConfig: 'toShowItCanBeParsed' }}"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf("someConfig" -> literalString("toShowItCanBeParsed"))))(pos),
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword KEY OPTIONS {nonValidOption : 42}"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("nonValidOption" -> literalInt(42)))(pos),
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword KEY OPTIONS {}"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)(pos),
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword KEY OPTIONS $$param"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.OptionsParam(parameter("param", CTMap))(pos),
          fromCypher5
        )(pos)
      )
    }

    test(
      s"USE neo4j CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5,
          Some(use(List("neo4j"), !fromCypher5))
        )(pos)
      )
    }

    test(
      s"USE neo4j CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          Some("my_constraint"),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions,
          fromCypher5,
          Some(use(List("neo4j"), !fromCypher5))
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE (node.prop1,node.prop2) IS $nodeKeyword KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop1"), prop("node", "prop2")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT my_constraint FOR (node:Label) REQUIRE (node.prop1,node.prop2) IS $nodeKeyword KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop1"), prop("node", "prop2")),
          Some("my_constraint"),
          ast.IfExistsReplace,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint IF NOT EXISTS FOR (node:Label) REQUIRE (node.prop1,node.prop2) IS $nodeKeyword KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop1"), prop("node", "prop2")),
          Some("my_constraint"),
          ast.IfExistsDoNothing,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword KEY OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}}"
    ) {
      // will fail in options converter
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodeKeyConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("native-btree-1.0"),
            "indexConfig" -> mapOf(
              "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
              "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
            )
          ))(pos),
          fromCypher5
        )(pos)
      )
    }

    // Node uniqueness

    test(
      s"CREATE CONSTRAINT FOR (node:Label) REQUIRE node.prop IS $nodeKeyword UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT FOR (node:Label) REQUIRE node.prop IS $nodeKeyword UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          None,
          ast.IfExistsReplace,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT IF NOT EXISTS FOR (node:Label) REQUIRE node.prop IS $nodeKeyword UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          None,
          ast.IfExistsDoNothing,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop1,node.prop2) IS $nodeKeyword UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop1"), prop("node", "prop2")),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS FOR (node:Label) REQUIRE (node.prop1,node.prop2) IS $nodeKeyword UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop1"), prop("node", "prop2")),
          None,
          ast.IfExistsInvalidSyntax,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword UNIQUE OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}, indexProvider : 'native-btree-1.0'}"
    ) {
      // will fail in options converter
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("native-btree-1.0"),
            "indexConfig" -> mapOf(
              "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
              "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
            )
          ))(pos),
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword UNIQUE OPTIONS $$options"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.OptionsParam(parameter("options", CTMap))(pos),
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE node.prop IS $nodeKeyword UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          Some("my_constraint"),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE (node.prop1,node.prop2) IS $nodeKeyword UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop1"), prop("node", "prop2")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT my_constraint FOR (node:Label) REQUIRE (node.prop1,node.prop2) IS $nodeKeyword UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop1"), prop("node", "prop2")),
          Some("my_constraint"),
          ast.IfExistsReplace,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint IF NOT EXISTS FOR (node:Label) REQUIRE (node.prop1,node.prop2) IS $nodeKeyword UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop1"), prop("node", "prop2")),
          Some("my_constraint"),
          ast.IfExistsDoNothing,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword UNIQUE OPTIONS {indexProvider : 'range-1.0'}"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexProvider" -> literalString("range-1.0")))(pos),
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword UNIQUE OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}"
    ) {
      // will fail in options converter
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map(
            "indexProvider" -> literalString("native-btree-1.0"),
            "indexConfig" -> mapOf(
              "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
              "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
            )
          ))(pos),
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword UNIQUE OPTIONS {indexConfig : {someConfig: 'toShowItCanBeParsed' }}"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("indexConfig" -> mapOf("someConfig" -> literalString("toShowItCanBeParsed"))))(pos),
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE (node.prop) IS $nodeKeyword UNIQUE OPTIONS {nonValidOption : 42}"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("nonValidOption" -> literalInt(42)))(pos),
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE (node.prop1,node.prop2) IS $nodeKeyword UNIQUE OPTIONS {}"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("node"),
          labelName("Label"),
          Seq(prop("node", "prop1"), prop("node", "prop2")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)(pos),
          fromCypher5
        )(pos)
      )
    }
  })

  test("CREATE CONSTRAINT $name FOR (n:L) REQUIRE n.prop IS NODE KEY") {
    assertAstVersionBased(fromCypher5 =>
      ast.CreateConstraint.createNodeKeyConstraint(
        varFor("n"),
        labelName("L"),
        Seq(prop("n", "prop")),
        Some(stringParam("name")),
        ast.IfExistsThrowError,
        ast.NoOptions,
        fromCypher5
      )(pos)
    )
  }

  test("CREATE CONSTRAINT $name FOR (n:L) REQUIRE n.prop IS UNIQUE") {
    assertAstVersionBased(fromCypher5 =>
      ast.CreateConstraint.createNodePropertyUniquenessConstraint(
        varFor("n"),
        labelName("L"),
        Seq(prop("n", "prop")),
        Some(stringParam("name")),
        ast.IfExistsThrowError,
        ast.NoOptions,
        fromCypher5
      )(pos)
    )
  }

  Seq("RELATIONSHIP", "REL", "").foreach(relKeyWord => {
    // Relationship key

    test(s"CREATE CONSTRAINT FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord KEY") {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipKeyConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR ()-[r1:R]->() REQUIRE (r2.prop1, r3.prop2) IS $relKeyWord KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipKeyConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop1"), prop("r3", "prop2")),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR ()<-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipKeyConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR ()<-[r1:R]->() REQUIRE (r2.prop) IS $relKeyWord KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipKeyConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipKeyConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          None,
          ast.IfExistsReplace,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipKeyConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          None,
          ast.IfExistsInvalidSyntax,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipKeyConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          None,
          ast.IfExistsDoNothing,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord KEY OPTIONS {key: 'value'}"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipKeyConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("key" -> literalString("value")))(pos),
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipKeyConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r1:R]->() REQUIRE (r2.prop1, r3.prop2) IS $relKeyWord KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipKeyConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop1"), prop("r3", "prop2")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()<-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipKeyConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()<-[r1:R]->() REQUIRE (r2.prop) IS $relKeyWord KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipKeyConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT my_constraint FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipKeyConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          Some("my_constraint"),
          ast.IfExistsReplace,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipKeyConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          Some("my_constraint"),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint IF NOT EXISTS FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord KEY"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipKeyConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          Some("my_constraint"),
          ast.IfExistsDoNothing,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord KEY OPTIONS {key: 'value'}"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipKeyConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("key" -> literalString("value")))(pos),
          fromCypher5
        )(pos)
      )
    }

    // Relationship uniqueness

    test(
      s"CREATE CONSTRAINT FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR ()-[r1:R]->() REQUIRE (r2.prop1, r3.prop2) IS $relKeyWord UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop1"), prop("r3", "prop2")),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR ()<-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR ()<-[r1:R]->() REQUIRE (r2.prop) IS $relKeyWord UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          None,
          ast.IfExistsReplace,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          None,
          ast.IfExistsInvalidSyntax,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          None,
          ast.IfExistsDoNothing,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord UNIQUE OPTIONS {key: 'value'}"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("key" -> literalString("value")))(pos),
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r1:R]->() REQUIRE (r2.prop1, r3.prop2) IS $relKeyWord UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop1"), prop("r3", "prop2")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()<-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()<-[r1:R]->() REQUIRE (r2.prop) IS $relKeyWord UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT my_constraint FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          Some("my_constraint"),
          ast.IfExistsReplace,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          Some("my_constraint"),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint IF NOT EXISTS FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord UNIQUE"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          Some("my_constraint"),
          ast.IfExistsDoNothing,
          ast.NoOptions,
          fromCypher5
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r1:R]-() REQUIRE (r2.prop) IS $relKeyWord UNIQUE OPTIONS {key: 'value'}"
    ) {
      assertAstVersionBased(fromCypher5 =>
        ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
          varFor("r1"),
          relTypeName("R"),
          Seq(prop("r2", "prop")),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map("key" -> literalString("value")))(pos),
          fromCypher5
        )(pos)
      )
    }
  })

  test(
    "CREATE CONSTRAINT $name FOR ()-[r:R]-() REQUIRE r.prop IS RELATIONSHIP KEY"
  ) {
    assertAstVersionBased(fromCypher5 =>
      ast.CreateConstraint.createRelationshipKeyConstraint(
        varFor("r"),
        relTypeName("R"),
        Seq(prop("r", "prop")),
        Some(stringParam("name")),
        ast.IfExistsThrowError,
        ast.NoOptions,
        fromCypher5
      )(pos)
    )
  }

  test("CREATE CONSTRAINT $name FOR ()-[r:R]-() REQUIRE r.prop IS UNIQUE") {
    assertAstVersionBased(fromCypher5 =>
      ast.CreateConstraint.createRelationshipPropertyUniquenessConstraint(
        varFor("r"),
        relTypeName("R"),
        Seq(prop("r", "prop")),
        Some(stringParam("name")),
        ast.IfExistsThrowError,
        ast.NoOptions,
        fromCypher5
      )(pos)
    )
  }

  // Node property existence

  test("CREATE CONSTRAINT FOR (node:Label) REQUIRE node.prop IS NOT NULL") {
    assertAst(
      ast.CreateConstraint.createNodePropertyExistenceConstraint(
        varFor("node"),
        labelName("Label"),
        prop("node", "prop"),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos)
    )
  }

  test("CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop) IS NOT NULL") {
    assertAst(
      ast.CreateConstraint.createNodePropertyExistenceConstraint(
        varFor("node"),
        labelName("Label"),
        prop("node", "prop"),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos)
    )
  }

  test(
    "CREATE OR REPLACE CONSTRAINT FOR (node:Label) REQUIRE node.prop IS NOT NULL"
  ) {
    assertAst(
      ast.CreateConstraint.createNodePropertyExistenceConstraint(
        varFor("node"),
        labelName("Label"),
        prop("node", "prop"),
        None,
        ast.IfExistsReplace,
        ast.NoOptions
      )(pos)
    )
  }

  test(
    "CREATE OR REPLACE CONSTRAINT IF NOT EXISTS FOR (node:Label) REQUIRE node.prop IS NOT NULL"
  ) {
    assertAst(
      ast.CreateConstraint.createNodePropertyExistenceConstraint(
        varFor("node"),
        labelName("Label"),
        prop("node", "prop"),
        None,
        ast.IfExistsInvalidSyntax,
        ast.NoOptions
      )(pos)
    )
  }

  test(
    "CREATE CONSTRAINT IF NOT EXISTS FOR (node:Label) REQUIRE (node.prop) IS NOT NULL"
  ) {
    assertAst(
      ast.CreateConstraint.createNodePropertyExistenceConstraint(
        varFor("node"),
        labelName("Label"),
        prop("node", "prop"),
        None,
        ast.IfExistsDoNothing,
        ast.NoOptions
      )(pos)
    )
  }

  test(
    "CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE (node.prop) IS NOT NULL"
  ) {
    assertAst(
      ast.CreateConstraint.createNodePropertyExistenceConstraint(
        varFor("node"),
        labelName("Label"),
        prop("node", "prop"),
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos)
    )
  }

  test(
    "CREATE CONSTRAINT property_exists FOR (n:address) REQUIRE n.id IS NOT NULL"
  ) {
    assertAst(
      ast.CreateConstraint.createNodePropertyExistenceConstraint(
        varFor("n"),
        labelName("address"),
        prop("n", "id"),
        Some("property_exists"),
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos)
    )
  }

  test(
    "CREATE OR REPLACE CONSTRAINT my_constraint FOR (node:Label) REQUIRE node.prop IS NOT NULL"
  ) {
    assertAst(
      ast.CreateConstraint.createNodePropertyExistenceConstraint(
        varFor("node"),
        labelName("Label"),
        prop("node", "prop"),
        Some("my_constraint"),
        ast.IfExistsReplace,
        ast.NoOptions
      )(pos)
    )
  }

  test(
    "CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS FOR (node:Label) REQUIRE node.prop IS NOT NULL"
  ) {
    assertAst(
      ast.CreateConstraint.createNodePropertyExistenceConstraint(
        varFor("node"),
        labelName("Label"),
        prop("node", "prop"),
        Some("my_constraint"),
        ast.IfExistsInvalidSyntax,
        ast.NoOptions
      )(pos)
    )
  }

  test(
    "CREATE CONSTRAINT my_constraint IF NOT EXISTS FOR (node:Label) REQUIRE node.prop IS NOT NULL"
  ) {
    assertAst(
      ast.CreateConstraint.createNodePropertyExistenceConstraint(
        varFor("node"),
        labelName("Label"),
        prop("node", "prop"),
        Some("my_constraint"),
        ast.IfExistsDoNothing,
        ast.NoOptions
      )(pos)
    )
  }

  test(
    "CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE node.prop IS NOT NULL OPTIONS {}"
  ) {
    assertAst(
      ast.CreateConstraint.createNodePropertyExistenceConstraint(
        varFor("node"),
        labelName("Label"),
        prop("node", "prop"),
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.OptionsMap(Map.empty)(pos)
      )(pos)
    )
  }

  test(
    "CREATE OR REPLACE CONSTRAINT my_constraint FOR (node:Label) REQUIRE (node.prop2, node.prop3) IS NOT NULL"
  ) {
    failsParsing[ast.Statements].withSyntaxErrorContaining(
      ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_IS_NOT_NULL),
      GqlStatusInfoCodes.STATUS_42N16,
      s"error: syntax error or access rule violation - unsupported index or constraint. Only single property '${ConstraintType.NODE_IS_NOT_NULL.description()}' constraints are supported."
    )
  }

  test("CREATE CONSTRAINT $name FOR (n:L) REQUIRE n.prop IS NOT NULL") {
    assertAst(
      ast.CreateConstraint.createNodePropertyExistenceConstraint(
        varFor("n"),
        labelName("L"),
        prop("n", "prop"),
        Some(stringParam("name")),
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos)
    )
  }

  // Relationship property existence

  test("CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE r.prop IS NOT NULL") {
    assertAst(
      ast.CreateConstraint.createRelationshipPropertyExistenceConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("r", "prop"),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos)
    )
  }

  test("CREATE CONSTRAINT FOR ()-[r:R]->() REQUIRE r.prop IS NOT NULL") {
    assertAst(
      ast.CreateConstraint.createRelationshipPropertyExistenceConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("r", "prop"),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos)
    )
  }

  test("CREATE CONSTRAINT FOR ()<-[r:R]-() REQUIRE (r.prop) IS NOT NULL") {
    assertAst(
      ast.CreateConstraint.createRelationshipPropertyExistenceConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("r", "prop"),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos)
    )
  }

  test("CREATE CONSTRAINT FOR ()<-[r:R]->() REQUIRE (r.prop) IS NOT NULL") {
    assertAst(
      ast.CreateConstraint.createRelationshipPropertyExistenceConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("r", "prop"),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos)
    )
  }

  test("CREATE OR REPLACE CONSTRAINT FOR ()<-[r:R]-() REQUIRE r.prop IS NOT NULL") {
    assertAst(
      ast.CreateConstraint.createRelationshipPropertyExistenceConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("r", "prop"),
        None,
        ast.IfExistsReplace,
        ast.NoOptions
      )(pos)
    )
  }

  test(
    "CREATE OR REPLACE CONSTRAINT IF NOT EXISTS FOR ()-[r:R]-() REQUIRE r.prop IS NOT NULL"
  ) {
    assertAst(
      ast.CreateConstraint.createRelationshipPropertyExistenceConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("r", "prop"),
        None,
        ast.IfExistsInvalidSyntax,
        ast.NoOptions
      )(pos)
    )
  }

  test(
    "CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:R]->() REQUIRE r.prop IS NOT NULL"
  ) {
    assertAst(
      ast.CreateConstraint.createRelationshipPropertyExistenceConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("r", "prop"),
        None,
        ast.IfExistsDoNothing,
        ast.NoOptions
      )(pos)
    )
  }

  test("CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.prop) IS NOT NULL OPTIONS {}") {
    assertAst(
      ast.CreateConstraint.createRelationshipPropertyExistenceConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("r", "prop"),
        None,
        ast.IfExistsThrowError,
        ast.OptionsMap(Map.empty)(pos)
      )(pos)
    )
  }

  test(
    "CREATE CONSTRAINT `$my_constraint` FOR ()-[r:R]-() REQUIRE r.prop IS NOT NULL"
  ) {
    assertAst(
      ast.CreateConstraint.createRelationshipPropertyExistenceConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("r", "prop"),
        Some("$my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos)
    )
  }

  test(
    "CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE (r.prop) IS NOT NULL"
  ) {
    assertAst(
      ast.CreateConstraint.createRelationshipPropertyExistenceConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("r", "prop"),
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos)
    )
  }

  test(
    "CREATE OR REPLACE CONSTRAINT `$my_constraint` FOR ()-[r:R]-() REQUIRE r.prop IS NOT NULL"
  ) {
    assertAst(
      ast.CreateConstraint.createRelationshipPropertyExistenceConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("r", "prop"),
        Some("$my_constraint"),
        ast.IfExistsReplace,
        ast.NoOptions
      )(pos)
    )
  }

  test(
    "CREATE OR REPLACE CONSTRAINT `$my_constraint` IF NOT EXISTS FOR ()-[r:R]->() REQUIRE (r.prop) IS NOT NULL"
  ) {
    assertAst(
      ast.CreateConstraint.createRelationshipPropertyExistenceConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("r", "prop"),
        Some("$my_constraint"),
        ast.IfExistsInvalidSyntax,
        ast.NoOptions
      )(pos)
    )
  }

  test(
    "CREATE CONSTRAINT `$my_constraint` IF NOT EXISTS FOR ()<-[r:R]-() REQUIRE r.prop IS NOT NULL"
  ) {
    assertAst(
      ast.CreateConstraint.createRelationshipPropertyExistenceConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("r", "prop"),
        Some("$my_constraint"),
        ast.IfExistsDoNothing,
        ast.NoOptions
      )(pos)
    )
  }

  test(
    "CREATE OR REPLACE CONSTRAINT my_constraint FOR ()-[r1:REL]-() REQUIRE (r2.prop2, r3.prop3) IS NOT NULL"
  ) {
    failsParsing[ast.Statements].withSyntaxErrorContaining(
      ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.REL_IS_NOT_NULL),
      GqlStatusInfoCodes.STATUS_42N16,
      s"error: syntax error or access rule violation - unsupported index or constraint. Only single property '${ConstraintType.REL_IS_NOT_NULL.description()}' constraints are supported."
    )
  }

  test("CREATE CONSTRAINT $name FOR ()-[r:R]-() REQUIRE r.prop IS NOT NULL") {
    assertAst(
      ast.CreateConstraint.createRelationshipPropertyExistenceConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("r", "prop"),
        Some(stringParam("name")),
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos)
    )
  }

  // Property type

  Seq("IS TYPED", "IS ::", "::").foreach(typeKeyword => {
    // Node property type

    test(
      s"CREATE CONSTRAINT FOR (node:Label) REQUIRE node.prop $typeKeyword BOOLEAN"
    ) {
      assertAst(
        ast.CreateConstraint.createNodePropertyTypeConstraint(
          varFor("node"),
          labelName("Label"),
          prop("node", "prop"),
          BooleanType(isNullable = true)(pos),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop) $typeKeyword BOOLEAN"
    ) {
      assertAst(
        ast.CreateConstraint.createNodePropertyTypeConstraint(
          varFor("node"),
          labelName("Label"),
          prop("node", "prop"),
          BooleanType(isNullable = true)(pos),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT FOR (node:Label) REQUIRE node.prop $typeKeyword BOOLEAN"
    ) {
      assertAst(
        ast.CreateConstraint.createNodePropertyTypeConstraint(
          varFor("node"),
          labelName("Label"),
          prop("node", "prop"),
          BooleanType(isNullable = true)(pos),
          None,
          ast.IfExistsReplace,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS FOR (node:Label) REQUIRE node.prop $typeKeyword BOOLEAN"
    ) {
      assertAst(
        ast.CreateConstraint.createNodePropertyTypeConstraint(
          varFor("node"),
          labelName("Label"),
          prop("node", "prop"),
          BooleanType(isNullable = true)(pos),
          None,
          ast.IfExistsInvalidSyntax,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT IF NOT EXISTS FOR (node:Label) REQUIRE (node.prop) $typeKeyword BOOLEAN"
    ) {
      assertAst(
        ast.CreateConstraint.createNodePropertyTypeConstraint(
          varFor("node"),
          labelName("Label"),
          prop("node", "prop"),
          BooleanType(isNullable = true)(pos),
          None,
          ast.IfExistsDoNothing,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE (node.prop) $typeKeyword STRING"
    ) {
      assertAst(
        ast.CreateConstraint.createNodePropertyTypeConstraint(
          varFor("node"),
          labelName("Label"),
          prop("node", "prop"),
          StringType(isNullable = true)(pos),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT my_constraint FOR (node:Label) REQUIRE node.prop $typeKeyword STRING"
    ) {
      assertAst(
        ast.CreateConstraint.createNodePropertyTypeConstraint(
          varFor("node"),
          labelName("Label"),
          prop("node", "prop"),
          StringType(isNullable = true)(pos),
          Some("my_constraint"),
          ast.IfExistsReplace,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS FOR (node:Label) REQUIRE node.prop $typeKeyword STRING"
    ) {
      assertAst(
        ast.CreateConstraint.createNodePropertyTypeConstraint(
          varFor("node"),
          labelName("Label"),
          prop("node", "prop"),
          StringType(isNullable = true)(pos),
          Some("my_constraint"),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint IF NOT EXISTS FOR (node:Label) REQUIRE node.prop $typeKeyword STRING"
    ) {
      assertAst(
        ast.CreateConstraint.createNodePropertyTypeConstraint(
          varFor("node"),
          labelName("Label"),
          prop("node", "prop"),
          StringType(isNullable = true)(pos),
          Some("my_constraint"),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE node.prop $typeKeyword STRING OPTIONS {}"
    ) {
      assertAst(
        ast.CreateConstraint.createNodePropertyTypeConstraint(
          varFor("node"),
          labelName("Label"),
          prop("node", "prop"),
          StringType(isNullable = true)(pos),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)(pos)
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT my_constraint FOR (node:Label) REQUIRE (node.prop2, node.prop3) $typeKeyword STRING"
    ) {
      failsParsing[ast.Statements].withSyntaxErrorContaining(
        ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_IS_TYPED),
        GqlStatusInfoCodes.STATUS_42N16,
        s"error: syntax error or access rule violation - unsupported index or constraint. Only single property '${ConstraintType.NODE_IS_TYPED.description()}' constraints are supported."
      )
    }

    // Relationship property type

    test(s"CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE r.prop $typeKeyword BOOLEAN") {
      assertAst(
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("r", "prop"),
          BooleanType(isNullable = true)(pos),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos)
      )
    }

    test(s"CREATE CONSTRAINT FOR ()-[r:R]->() REQUIRE r.prop $typeKeyword BOOLEAN") {
      assertAst(
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("r", "prop"),
          BooleanType(isNullable = true)(pos),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR ()<-[r:R]-() REQUIRE (r.prop) $typeKeyword BOOLEAN"
    ) {
      assertAst(
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("r", "prop"),
          BooleanType(isNullable = true)(pos),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR ()<-[r:R]->() REQUIRE (r.prop) $typeKeyword BOOLEAN"
    ) {
      assertAst(
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("r", "prop"),
          BooleanType(isNullable = true)(pos),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT FOR ()<-[r:R]-() REQUIRE r.prop $typeKeyword BOOLEAN"
    ) {
      assertAst(
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("r", "prop"),
          BooleanType(isNullable = true)(pos),
          None,
          ast.IfExistsReplace,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS FOR ()-[r:R]-() REQUIRE r.prop $typeKeyword BOOLEAN"
    ) {
      assertAst(
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("r", "prop"),
          BooleanType(isNullable = true)(pos),
          None,
          ast.IfExistsInvalidSyntax,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT IF NOT EXISTS FOR ()-[r:R]->() REQUIRE r.prop $typeKeyword BOOLEAN"
    ) {
      assertAst(
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("r", "prop"),
          BooleanType(isNullable = true)(pos),
          None,
          ast.IfExistsDoNothing,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.prop) $typeKeyword BOOLEAN OPTIONS {}"
    ) {
      assertAst(
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("r", "prop"),
          BooleanType(isNullable = true)(pos),
          None,
          ast.IfExistsThrowError,
          ast.OptionsMap(Map.empty)(pos)
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT `$$my_constraint` FOR ()-[r:R]-() REQUIRE r.prop $typeKeyword STRING"
    ) {
      assertAst(
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("r", "prop"),
          StringType(isNullable = true)(pos),
          Some("$my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE (r.prop) $typeKeyword STRING"
    ) {
      assertAst(
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("r", "prop"),
          StringType(isNullable = true)(pos),
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT `$$my_constraint` FOR ()-[r:R]-() REQUIRE r.prop $typeKeyword STRING"
    ) {
      assertAst(
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("r", "prop"),
          StringType(isNullable = true)(pos),
          Some("$my_constraint"),
          ast.IfExistsReplace,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT `$$my_constraint` IF NOT EXISTS FOR ()-[r:R]->() REQUIRE (r.prop) $typeKeyword STRING"
    ) {
      assertAst(
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("r", "prop"),
          StringType(isNullable = true)(pos),
          Some("$my_constraint"),
          ast.IfExistsInvalidSyntax,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE CONSTRAINT `$$my_constraint` IF NOT EXISTS FOR ()<-[r:R]-() REQUIRE r.prop $typeKeyword STRING"
    ) {
      assertAst(
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("r", "prop"),
          StringType(isNullable = true)(pos),
          Some("$my_constraint"),
          ast.IfExistsDoNothing,
          ast.NoOptions
        )(pos)
      )
    }

    test(
      s"CREATE OR REPLACE CONSTRAINT my_constraint FOR ()-[r1:REL]-() REQUIRE (r2.prop2, r3.prop3) $typeKeyword STRING"
    ) {
      failsParsing[ast.Statements].withSyntaxErrorContaining(
        ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.REL_IS_TYPED),
        GqlStatusInfoCodes.STATUS_42N16,
        s"error: syntax error or access rule violation - unsupported index or constraint. Only single property '${ConstraintType.REL_IS_TYPED.description()}' constraints are supported."
      )
    }
  })

  test("CREATE CONSTRAINT $name FOR (n:L) REQUIRE n.prop IS TYPED STRING") {
    assertAst(
      ast.CreateConstraint.createNodePropertyTypeConstraint(
        varFor("n"),
        labelName("L"),
        prop("n", "prop"),
        StringType(isNullable = true)(pos),
        Some(stringParam("name")),
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos)
    )
  }

  test("CREATE CONSTRAINT $name FOR ()-[r:R]-() REQUIRE r.prop IS TYPED STRING") {
    assertAst(
      ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("r", "prop"),
        StringType(isNullable = true)(pos),
        Some(stringParam("name")),
        ast.IfExistsThrowError,
        ast.NoOptions
      )(pos)
    )
  }

  // allowed single types
  private val allowedNonListSingleTypes = Seq(
    ("BOOL", BooleanType(isNullable = true)(pos)),
    ("BOOLEAN", BooleanType(isNullable = true)(pos)),
    ("VARCHAR", StringType(isNullable = true)(pos)),
    ("STRING", StringType(isNullable = true)(pos)),
    ("INTEGER", IntegerType(isNullable = true)(pos)),
    ("INTEGER64", IntegerType(isNullable = true)(pos)),
    ("INT", IntegerType(isNullable = true)(pos)),
    ("INT64", IntegerType(isNullable = true)(pos)),
    ("SIGNED INTEGER", IntegerType(isNullable = true)(pos)),
    ("FLOAT", FloatType(isNullable = true)(pos)),
    ("FLOAT64", FloatType(isNullable = true)(pos)),
    ("DATE", DateType(isNullable = true)(pos)),
    ("LOCAL TIME", LocalTimeType(isNullable = true)(pos)),
    ("TIME WITHOUT TIMEZONE", LocalTimeType(isNullable = true)(pos)),
    ("ZONED TIME", ZonedTimeType(isNullable = true)(pos)),
    ("TIME WITH TIMEZONE", ZonedTimeType(isNullable = true)(pos)),
    ("LOCAL DATETIME", LocalDateTimeType(isNullable = true)(pos)),
    ("TIMESTAMP WITHOUT TIMEZONE", LocalDateTimeType(isNullable = true)(pos)),
    ("ZONED DATETIME", ZonedDateTimeType(isNullable = true)(pos)),
    ("TIMESTAMP WITH TIMEZONE", ZonedDateTimeType(isNullable = true)(pos)),
    ("DURATION", DurationType(isNullable = true)(pos)),
    ("POINT", PointType(isNullable = true)(pos)),
    ("VECTOR<INT32>(3)", VectorType(Some(Integer32Type(isNullable = false)(pos)), Some(3), isNullable = true)(pos))
  )

  // coordinate types allowed inside vectors
  private val allowedInnerVectorTypes = Seq(
    ("INT8", Integer8Type(isNullable = false)(pos)),
    ("INTEGER8", Integer8Type(isNullable = false)(pos)),
    ("INT16", Integer16Type(isNullable = false)(pos)),
    ("INTEGER16", Integer16Type(isNullable = false)(pos)),
    ("INT32", Integer32Type(isNullable = false)(pos)),
    ("INTEGER32", Integer32Type(isNullable = false)(pos)),
    ("INT64", IntegerType(isNullable = false)(pos)),
    ("INTEGER64", IntegerType(isNullable = false)(pos)),
    ("INT", IntegerType(isNullable = false)(pos)),
    ("INTEGER", IntegerType(isNullable = false)(pos)),
    ("FLOAT32", Float32Type(isNullable = false)(pos)),
    ("FLOAT64", FloatType(isNullable = false)(pos)),
    ("FLOAT", FloatType(isNullable = false)(pos))
  )

  // disallowed single types (throws in semantic checking)
  private val disallowedNonListSingleTypes = Seq(
    ("NOTHING", NothingType()(pos)),
    ("NOTHING NOT NULL", NothingType()(pos)),
    ("NULL", NullType()(pos)),
    ("NULL NOT NULL", NothingType()(pos)),
    ("BOOL NOT NULL", BooleanType(isNullable = false)(pos)),
    ("BOOLEAN NOT NULL", BooleanType(isNullable = false)(pos)),
    ("VARCHAR NOT NULL", StringType(isNullable = false)(pos)),
    ("STRING NOT NULL", StringType(isNullable = false)(pos)),
    ("INTEGER NOT NULL", IntegerType(isNullable = false)(pos)),
    ("INT NOT NULL", IntegerType(isNullable = false)(pos)),
    ("SIGNED INTEGER NOT NULL", IntegerType(isNullable = false)(pos)),
    ("FLOAT NOT NULL", FloatType(isNullable = false)(pos)),
    ("DATE NOT NULL", DateType(isNullable = false)(pos)),
    ("LOCAL TIME NOT NULL", LocalTimeType(isNullable = false)(pos)),
    ("TIME WITHOUT TIMEZONE NOT NULL", LocalTimeType(isNullable = false)(pos)),
    ("ZONED TIME NOT NULL", ZonedTimeType(isNullable = false)(pos)),
    ("TIME WITH TIMEZONE NOT NULL", ZonedTimeType(isNullable = false)(pos)),
    ("LOCAL DATETIME NOT NULL", LocalDateTimeType(isNullable = false)(pos)),
    ("TIMESTAMP WITHOUT TIMEZONE NOT NULL", LocalDateTimeType(isNullable = false)(pos)),
    ("ZONED DATETIME NOT NULL", ZonedDateTimeType(isNullable = false)(pos)),
    ("TIMESTAMP WITH TIMEZONE NOT NULL", ZonedDateTimeType(isNullable = false)(pos)),
    ("DURATION NOT NULL", DurationType(isNullable = false)(pos)),
    ("POINT NOT NULL", PointType(isNullable = false)(pos)),
    // Missing dimension
    ("VECTOR<INTEGER64>", VectorType(Some(IntegerType(isNullable = false)(pos)), None, isNullable = true)(pos)),
    // Missing coordinate type
    ("VECTOR(77)", VectorType(None, Some(77), isNullable = true)(pos)),
    // Missing dimension and coordinate type
    ("VECTOR", VectorType(None, None, isNullable = true)(pos)),
    // Negative dimension
    (
      "VECTOR<INTEGER32>(-2)",
      VectorType(Some(Integer32Type(isNullable = false)(pos)), Some(-2), isNullable = true)(pos)
    ),
    // To small dimension
    ("VECTOR<INTEGER16>(0)", VectorType(Some(Integer16Type(isNullable = false)(pos)), Some(0), isNullable = true)(pos)),
    // Too large dimension
    (
      "VECTOR<FLOAT32>(4097)",
      VectorType(Some(Float32Type(isNullable = false)(pos)), Some(4097), isNullable = true)(pos)
    ),
    // Outer NOT NULL
    (
      "VECTOR<INT8>(1) NOT NULL",
      VectorType(Some(Integer8Type(isNullable = false)(pos)), Some(1), isNullable = false)(pos)
    ),
    ("NODE", NodeType(isNullable = true)(pos)),
    ("NODE NOT NULL", NodeType(isNullable = false)(pos)),
    ("ANY NODE", NodeType(isNullable = true)(pos)),
    ("ANY NODE NOT NULL", NodeType(isNullable = false)(pos)),
    ("VERTEX", NodeType(isNullable = true)(pos)),
    ("VERTEX NOT NULL", NodeType(isNullable = false)(pos)),
    ("ANY VERTEX", NodeType(isNullable = true)(pos)),
    ("ANY VERTEX NOT NULL", NodeType(isNullable = false)(pos)),
    ("RELATIONSHIP", RelationshipType(isNullable = true)(pos)),
    ("RELATIONSHIP NOT NULL", RelationshipType(isNullable = false)(pos)),
    ("ANY RELATIONSHIP", RelationshipType(isNullable = true)(pos)),
    ("ANY RELATIONSHIP NOT NULL", RelationshipType(isNullable = false)(pos)),
    ("EDGE", RelationshipType(isNullable = true)(pos)),
    ("EDGE NOT NULL", RelationshipType(isNullable = false)(pos)),
    ("ANY EDGE", RelationshipType(isNullable = true)(pos)),
    ("ANY EDGE NOT NULL", RelationshipType(isNullable = false)(pos)),
    ("MAP", MapType(isNullable = true)(pos)),
    ("MAP NOT NULL", MapType(isNullable = false)(pos)),
    ("ANY MAP", MapType(isNullable = true)(pos)),
    ("ANY MAP NOT NULL", MapType(isNullable = false)(pos)),
    ("PATH", PathType(isNullable = true)(pos)),
    ("PATH NOT NULL", PathType(isNullable = false)(pos)),
    ("ANY PROPERTY VALUE", PropertyValueType(isNullable = true)(pos)),
    ("ANY PROPERTY VALUE NOT NULL", PropertyValueType(isNullable = false)(pos)),
    ("PROPERTY VALUE", PropertyValueType(isNullable = true)(pos)),
    ("PROPERTY VALUE NOT NULL", PropertyValueType(isNullable = false)(pos)),
    ("ANY VALUE", AnyType(isNullable = true)(pos)),
    ("ANY VALUE NOT NULL", AnyType(isNullable = false)(pos)),
    ("ANY", AnyType(isNullable = true)(pos)),
    ("ANY NOT NULL", AnyType(isNullable = false)(pos))
  )

  // List of single types (mix of allowed and disallowed types)
  private val listSingleTypes = (allowedNonListSingleTypes ++ disallowedNonListSingleTypes)
    .flatMap { case (innerTypeString, innerTypeExpr: CypherType) =>
      Seq(
        // LIST<type>
        (s"LIST<$innerTypeString>", ListType(innerTypeExpr, isNullable = true)(pos)),
        (s"LIST<$innerTypeString> NOT NULL", ListType(innerTypeExpr, isNullable = false)(pos)),
        (s"ARRAY<$innerTypeString>", ListType(innerTypeExpr, isNullable = true)(pos)),
        (s"ARRAY<$innerTypeString> NOT NULL", ListType(innerTypeExpr, isNullable = false)(pos)),
        (s"$innerTypeString LIST", ListType(innerTypeExpr, isNullable = true)(pos)),
        (s"$innerTypeString LIST NOT NULL", ListType(innerTypeExpr, isNullable = false)(pos)),
        (s"$innerTypeString ARRAY", ListType(innerTypeExpr, isNullable = true)(pos)),
        (s"$innerTypeString ARRAY NOT NULL", ListType(innerTypeExpr, isNullable = false)(pos)),
        // LIST<LIST<type>>
        (
          s"LIST<LIST<$innerTypeString>>",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<LIST<$innerTypeString>> NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<LIST<$innerTypeString> NOT NULL>",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<LIST<$innerTypeString> NOT NULL> NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<ARRAY<$innerTypeString>>",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<ARRAY<$innerTypeString>> NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<ARRAY<$innerTypeString> NOT NULL>",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<ARRAY<$innerTypeString> NOT NULL> NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString LIST>",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString LIST> NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString LIST NOT NULL>",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString LIST NOT NULL> NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString ARRAY>",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString ARRAY> NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString ARRAY NOT NULL>",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString ARRAY NOT NULL> NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString> LIST",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString> LIST NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"LIST<$innerTypeString> NOT NULL LIST",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"LIST<$innerTypeString> NOT NULL LIST NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"ARRAY<$innerTypeString> LIST",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"ARRAY<$innerTypeString> LIST NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"ARRAY<$innerTypeString> NOT NULL LIST",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"ARRAY<$innerTypeString> NOT NULL LIST NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"$innerTypeString LIST LIST",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"$innerTypeString LIST LIST NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"$innerTypeString LIST NOT NULL LIST",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"$innerTypeString LIST NOT NULL LIST NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        (
          s"$innerTypeString ARRAY LIST",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = true)(pos)
        ),
        (
          s"$innerTypeString ARRAY LIST NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = true)(pos), isNullable = false)(pos)
        ),
        (
          s"$innerTypeString ARRAY NOT NULL LIST",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = true)(pos)
        ),
        (
          s"$innerTypeString ARRAY NOT NULL LIST NOT NULL",
          ListType(ListType(innerTypeExpr, isNullable = false)(pos), isNullable = false)(pos)
        ),
        // even more nesting lists
        (
          s"LIST<LIST<LIST<LIST<$innerTypeString>> NOT NULL> NOT NULL LIST NOT NULL>",
          ListType(
            ListType(
              ListType(
                ListType(
                  ListType(
                    innerTypeExpr,
                    isNullable = true
                  )(pos),
                  isNullable = false
                )(pos),
                isNullable = false
              )(pos),
              isNullable = false
            )(pos),
            isNullable = true
          )(pos)
        ),
        (
          s"$innerTypeString LIST NOT NULL LIST LIST NOT NULL LIST",
          ListType(
            ListType(
              ListType(
                ListType(
                  innerTypeExpr,
                  isNullable = false
                )(pos),
                isNullable = true
              )(pos),
              isNullable = false
            )(pos),
            isNullable = true
          )(pos)
        )
      )
    }

  // Union types or types involving unions (mix of allowed and disallowed types)
  private val unionTypes = Seq(
    // unions of single types and lists of unions
    (
      "ANY<DURATION>",
      DurationType(isNullable = true)(pos)
    ),
    (
      "ANY VALUE < VARCHAR NOT NULL >",
      StringType(isNullable = false)(pos)
    ),
    (
      "BOOL | BOOLEAN",
      BooleanType(isNullable = true)(pos)
    ),
    (
      "ANY<FLOAT | FLOAT>",
      FloatType(isNullable = true)(pos)
    ),
    (
      "LIST<DURATION | DATE | PATH>",
      ListType(
        ClosedDynamicUnionType(Set(
          DateType(isNullable = true)(pos),
          DurationType(isNullable = true)(pos),
          PathType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    ),
    (
      "ARRAY < ANY < VARCHAR NOT NULL | INT NOT NULL> | ANY VALUE < INT | BOOL > > NOT NULL",
      ListType(
        ClosedDynamicUnionType(Set(
          BooleanType(isNullable = true)(pos),
          StringType(isNullable = false)(pos),
          IntegerType(isNullable = false)(pos),
          IntegerType(isNullable = true)(pos)
        ))(pos),
        isNullable = false
      )(pos)
    )
  ) ++ Seq(
    // unions of multiple types
    (
      "STRING",
      StringType(isNullable = true)(pos),
      "INT NOT NULL",
      IntegerType(isNullable = false)(pos)
    ),
    (
      "FLOAT",
      FloatType(isNullable = true)(pos),
      "DATE",
      DateType(isNullable = true)(pos)
    ),
    (
      "LOCAL DATETIME NOT NULL",
      LocalDateTimeType(isNullable = false)(pos),
      "DURATION",
      DurationType(isNullable = true)(pos)
    ),
    (
      "NULL",
      NullType()(pos),
      "NODE",
      NodeType(isNullable = true)(pos)
    ),
    (
      "ANY EDGE NOT NULL",
      RelationshipType(isNullable = false)(pos),
      "MAP NOT NULL",
      MapType(isNullable = false)(pos)
    ),
    (
      "ANY VALUE",
      AnyType(isNullable = true)(pos),
      "PROPERTY VALUE",
      PropertyValueType(isNullable = true)(pos)
    ),
    (
      "LIST<BOOL>",
      ListType(BooleanType(isNullable = true)(pos), isNullable = true)(pos),
      "FLOAT ARRAY",
      ListType(FloatType(isNullable = true)(pos), isNullable = true)(pos)
    ),
    (
      "LIST<NOTHING>",
      ListType(NothingType()(pos), isNullable = true)(pos),
      "VARCHAR",
      StringType(isNullable = true)(pos)
    ),
    (
      "TIME WITH TIMEZONE",
      ZonedTimeType(isNullable = true)(pos),
      "LIST<SIGNED INTEGER NOT NULL>",
      ListType(IntegerType(isNullable = false)(pos), isNullable = true)(pos)
    ),
    (
      "LIST<PATH | BOOL> NOT NULL",
      ListType(
        ClosedDynamicUnionType(Set(
          BooleanType(isNullable = true)(pos),
          PathType(isNullable = true)(pos)
        ))(pos),
        isNullable = false
      )(pos),
      "FLOAT NOT NULL ARRAY NOT NULL",
      ListType(FloatType(isNullable = false)(pos), isNullable = false)(pos)
    ),
    (
      "LIST<ANY<NOTHING | STRING | BOOLEAN | NULL>>",
      ListType(
        ClosedDynamicUnionType(Set(
          NothingType()(pos),
          NullType()(pos),
          BooleanType(isNullable = true)(pos),
          StringType(isNullable = true)(pos)
        ))(pos),
        isNullable = true
      )(pos),
      "VARCHAR",
      StringType(isNullable = true)(pos)
    ),
    (
      "TIME WITH TIMEZONE",
      ZonedTimeType(isNullable = true)(pos),
      "LIST < ANY VALUE < SIGNED INTEGER NOT NULL | INT > | DURATION NOT NULL >",
      ListType(
        ClosedDynamicUnionType(Set(
          IntegerType(isNullable = false)(pos),
          IntegerType(isNullable = true)(pos),
          DurationType(isNullable = false)(pos)
        ))(pos),
        isNullable = true
      )(pos)
    )
  ).flatMap { case (typeString1, typeExpr1, typeString2, typeExpr2) =>
    Seq(
      (s"ANY<$typeString1 | $typeString2>", ClosedDynamicUnionType(Set(typeExpr1, typeExpr2))(pos)),
      (s"ANY VALUE<$typeString1 | $typeString2>", ClosedDynamicUnionType(Set(typeExpr1, typeExpr2))(pos)),
      (s"$typeString1 | $typeString2", ClosedDynamicUnionType(Set(typeExpr1, typeExpr2))(pos)),
      (
        s"ANY<$typeString1 | $typeString2 | MAP>",
        ClosedDynamicUnionType(Set(typeExpr1, typeExpr2, MapType(isNullable = true)(pos)))(pos)
      ),
      (
        s"ANY VALUE < LIST < NULL NOT NULL > NOT NULL | $typeString1 | POINT NOT NULL | $typeString2 >",
        ClosedDynamicUnionType(Set(
          ListType(NothingType()(pos), isNullable = false)(pos),
          typeExpr1,
          PointType(isNullable = false)(pos),
          typeExpr2
        ))(pos)
      ),
      (
        s"$typeString1|ANY<INT>|$typeString2|ANY<VARCHAR|BOOL>|NODE NOT NULL",
        ClosedDynamicUnionType(Set(
          typeExpr1,
          IntegerType(isNullable = true)(pos),
          typeExpr2,
          StringType(isNullable = true)(pos),
          BooleanType(isNullable = true)(pos),
          NodeType(isNullable = false)(pos)
        ))(pos)
      )
    )
  } ++ Seq(
    // a big union of all allowed (non-list) single types
    (
      allowedNonListSingleTypes.map(_._1).mkString("|"),
      ClosedDynamicUnionType(allowedNonListSingleTypes.map(_._2).toSet)(pos)
    ),
    (
      allowedNonListSingleTypes.map(_._1).mkString("ANY<", " | ", ">"),
      ClosedDynamicUnionType(allowedNonListSingleTypes.map(_._2).toSet)(pos)
    )
  )

  val cypher25OnlyTypes: Seq[String] = Seq(
    "UUID",
    "INT64",
    "INTEGER64",
    "FLOAT64",
    "VECTOR"
  )

  allowedNonListSingleTypes.foreach { case (typeString, typeExpr: CypherType) =>
    test(s"CREATE CONSTRAINT FOR (n:Label) REQUIRE r.prop IS TYPED $typeString") {
      assertAst(
        ast.CreateConstraint.createNodePropertyTypeConstraint(
          varFor("n"),
          labelName("Label"),
          prop("r", "prop"),
          typeExpr,
          None,
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos),
        supportedInCypher5 = !cypher25OnlyTypes.exists(typeString.contains(_))
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR (n:Label) REQUIRE r.prop IS TYPED ${typeString.toLowerCase}"
    ) {
      assertAst(
        ast.CreateConstraint.createNodePropertyTypeConstraint(
          varFor("n"),
          labelName("Label"),
          prop("r", "prop"),
          typeExpr,
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos),
        supportedInCypher5 = !cypher25OnlyTypes.exists(typeString.contains(_))
      )
    }

    test(s"CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE n.prop IS TYPED ${typeString.toLowerCase}") {
      assertAst(
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("n", "prop"),
          typeExpr,
          None,
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos),
        supportedInCypher5 = !cypher25OnlyTypes.exists(typeString.contains(_))
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE n.prop IS TYPED $typeString"
    ) {
      assertAst(
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("n", "prop"),
          typeExpr,
          Some("my_constraint"),
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos),
        supportedInCypher5 = !cypher25OnlyTypes.exists(typeString.contains(_))
      )
    }
  }

  allowedInnerVectorTypes.foreach { case (coordinateType, typeExpr: CypherType) =>
    // Node property type constraint, Cypher style syntax
    test(s"CREATE CONSTRAINT name FOR (n:Label) REQUIRE n.prop IS TYPED VECTOR<$coordinateType>(2)") {
      assertAst(
        ast.CreateConstraint.createNodePropertyTypeConstraint(
          varFor("n"),
          labelName("Label"),
          prop("n", "prop"),
          VectorType(Some(typeExpr), Some(2), isNullable = true)(pos),
          Some("name"),
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos),
        supportedInCypher5 = false
      )
    }

    // Node property type constraint, GQL style syntax and explicit inner NOT NULL
    test(s"CREATE CONSTRAINT name FOR (n:Label) REQUIRE n.prop IS TYPED VECTOR(512, $coordinateType NOT NULL)") {
      assertAst(
        ast.CreateConstraint.createNodePropertyTypeConstraint(
          varFor("n"),
          labelName("Label"),
          prop("n", "prop"),
          VectorType(Some(typeExpr), Some(512), isNullable = true)(pos),
          Some("name"),
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos),
        supportedInCypher5 = false
      )
    }

    // Relationship property type constraint, Cypher style syntax and explicit inner NOT NULL
    test(s"CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE r.prop IS :: VECTOR<$coordinateType NOT NULL>(1032)") {
      assertAst(
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("r", "prop"),
          VectorType(Some(typeExpr), Some(1032), isNullable = true)(pos),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos),
        supportedInCypher5 = false
      )
    }

    // Relationship property type constraint, GQL style syntax
    test(s"CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE r.prop IS :: VECTOR(5, $coordinateType)") {
      assertAst(
        ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
          varFor("r"),
          relTypeName("R"),
          prop("r", "prop"),
          VectorType(Some(typeExpr), Some(5), isNullable = true)(pos),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos),
        supportedInCypher5 = false
      )
    }
  }

  // In Cypher 5 the property set is different (doesn't contain vector type), so this updates
  // the types accordingly
  def getCorrectCypherVersionOfType(fromCypher5: Boolean, typeExpr: CypherType): CypherType = {
    if (fromCypher5) {
      typeExpr match {
        case _: PropertyValueType =>
          PropertyValueCypher5Type(isNullable = typeExpr.isNullable)(typeExpr.position)
        case listType: ListType => ListType(
            getCorrectCypherVersionOfType(fromCypher5, listType.innerType),
            listType.isNullable
          )(listType.position)
        case unionType: ClosedDynamicUnionType => ClosedDynamicUnionType(unionType.innerTypes.map(innerType =>
            getCorrectCypherVersionOfType(fromCypher5, innerType)
          ))(unionType.position)
        case _ => typeExpr
      }
    } else typeExpr
  }

  disallowedNonListSingleTypes.foreach { case (typeString, typeExpr: CypherType) =>
    test(
      s"CREATE CONSTRAINT my_constraint FOR (n:Label) REQUIRE r.prop IS TYPED ${typeString.toLowerCase}"
    ) {
      assertAstVersionBased(
        fromCypher5 =>
          ast.CreateConstraint.createNodePropertyTypeConstraint(
            varFor("n"),
            labelName("Label"),
            prop("r", "prop"),
            getCorrectCypherVersionOfType(fromCypher5, typeExpr),
            Some("my_constraint"),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
        supportedInCypher5 = !cypher25OnlyTypes.exists(typeString.contains(_))
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE n.prop IS TYPED $typeString"
    ) {
      assertAstVersionBased(
        fromCypher5 =>
          ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
            varFor("r"),
            relTypeName("R"),
            prop("n", "prop"),
            getCorrectCypherVersionOfType(fromCypher5, typeExpr),
            Some("my_constraint"),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos),
        supportedInCypher5 = !cypher25OnlyTypes.exists(typeString.contains(_))
      )
    }
  }

  listSingleTypes.foreach { case (listTypeString, listTypeExpr: CypherType) =>
    test(
      s"CREATE CONSTRAINT my_constraint FOR (n:Label) REQUIRE r.prop IS TYPED ${listTypeString.toLowerCase}"
    ) {
      assertAstVersionBased(
        fromCypher5 =>
          ast.Statements(Seq(ast.CreateConstraint.createNodePropertyTypeConstraint(
            varFor("n"),
            labelName("Label"),
            prop("r", "prop"),
            getCorrectCypherVersionOfType(fromCypher5, listTypeExpr),
            Some("my_constraint"),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos))),
        comparePosition = false,
        supportedInCypher5 = !cypher25OnlyTypes.exists(listTypeString.contains(_))
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE n.prop IS TYPED $listTypeString"
    ) {
      assertAstVersionBased(
        fromCypher5 =>
          ast.Statements(Seq(ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
            varFor("r"),
            relTypeName("R"),
            prop("n", "prop"),
            getCorrectCypherVersionOfType(fromCypher5, listTypeExpr),
            Some("my_constraint"),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos))),
        comparePosition = false,
        supportedInCypher5 = !cypher25OnlyTypes.exists(listTypeString.contains(_))
      )
    }
  }

  unionTypes.foreach { case (unionTypeString, unionTypeExpr: CypherType) =>
    test(
      s"CREATE CONSTRAINT my_constraint FOR (n:Label) REQUIRE r.prop IS TYPED ${unionTypeString.toLowerCase}"
    ) {
      assertAstVersionBased(
        fromCypher5 =>
          ast.Statements(Seq(ast.CreateConstraint.createNodePropertyTypeConstraint(
            varFor("n"),
            labelName("Label"),
            prop("r", "prop"),
            getCorrectCypherVersionOfType(fromCypher5, unionTypeExpr),
            Some("my_constraint"),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos))),
        comparePosition = false,
        supportedInCypher5 = !cypher25OnlyTypes.exists(unionTypeString.contains(_))
      )
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE n.prop IS TYPED $unionTypeString"
    ) {
      assertAstVersionBased(
        fromCypher5 =>
          ast.Statements(Seq(ast.CreateConstraint.createRelationshipPropertyTypeConstraint(
            varFor("r"),
            relTypeName("R"),
            prop("n", "prop"),
            getCorrectCypherVersionOfType(fromCypher5, unionTypeExpr),
            Some("my_constraint"),
            ast.IfExistsThrowError,
            ast.NoOptions
          )(pos))),
        comparePosition = false,
        supportedInCypher5 = !cypher25OnlyTypes.exists(unionTypeString.contains(_))
      )
    }
  }

  // Edge case tests

  test(
    "CREATE CONSTRAINT my_constraint FOR (n:Person) REQUIRE n.prop IS NOT NULL OPTIONS {indexProvider : 'range-1.0'};"
  ) {
    assertAst(
      ast.CreateConstraint.createNodePropertyExistenceConstraint(
        varFor("n"),
        labelName("Person"),
        prop("n", "prop"),
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.OptionsMap(Map("indexProvider" -> literalString("range-1.0")))(pos)
      )(pos)
    )
  }

  test(
    "CREATE CONSTRAINT FOR (n:Person) REQUIRE n.prop IS NOT NULL; CREATE CONSTRAINT FOR (n:User) REQUIRE n.prop IS UNIQUE"
  ) {
    assertAstVersionBased(fromCypher5 =>
      ast.Statements(Seq(
        ast.CreateConstraint.createNodePropertyExistenceConstraint(
          varFor("n"),
          labelName("Person"),
          prop("n", "prop"),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions
        )(pos),
        ast.CreateConstraint.createNodePropertyUniquenessConstraint(
          varFor("n"),
          labelName("User"),
          Seq(prop("n", "prop")),
          None,
          ast.IfExistsThrowError,
          ast.NoOptions,
          fromCypher5
        )(pos)
      ))
    )
  }

  test("CREATE CONSTRAINT FOR FOR (node:Label) REQUIRE (node.prop) IS NODE KEY") {
    assertAstVersionBased(fromCypher5 =>
      ast.CreateConstraint.createNodeKeyConstraint(
        varFor("node", (1, 28, 27)),
        labelName("Label", (1, 33, 32)),
        Seq(prop("node", "prop", (1, 49, 48))),
        Some("FOR"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        fromCypher5
      )(defaultPos)
    )
  }

  test("CREATE CONSTRAINT FOR FOR (node:Label) REQUIRE (node.prop) IS UNIQUE") {
    assertAstVersionBased(fromCypher5 =>
      ast.CreateConstraint.createNodePropertyUniquenessConstraint(
        varFor("node", (1, 28, 27)),
        labelName("Label", (1, 33, 32)),
        Seq(prop("node", "prop", (1, 49, 48))),
        Some("FOR"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        fromCypher5
      )(defaultPos)
    )
  }

  test("CREATE CONSTRAINT FOR FOR (node:Label) REQUIRE node.prop IS NOT NULL") {
    assertAst(
      ast.CreateConstraint.createNodePropertyExistenceConstraint(
        varFor("node", (1, 28, 27)),
        labelName("Label", (1, 33, 32)),
        prop("node", "prop", (1, 48, 47)),
        Some("FOR"),
        ast.IfExistsThrowError,
        ast.NoOptions
      )(defaultPos)
    )
  }

  test("CREATE CONSTRAINT FOR FOR ()-[r:R]-() REQUIRE (r.prop) IS NOT NULL") {
    assertAst(
      ast.CreateConstraint.createRelationshipPropertyExistenceConstraint(
        varFor("r", (1, 31, 30)),
        relTypeName("R", (1, 33, 32)),
        prop("r", "prop", (1, 48, 47)),
        Some("FOR"),
        ast.IfExistsThrowError,
        ast.NoOptions
      )(defaultPos)
    )
  }

  // Negative tests

  test(
    "CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop) IS NODE KEY {indexProvider : 'range-1.0'}"
  ) {
    failsParsing[ast.Statements].withSyntaxErrorContaining("Invalid input '{': expected 'OPTIONS' or <EOF>")
  }

  test(
    "CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop) IS NODE KEY OPTIONS"
  ) {
    failsParsing[ast.Statements].withSyntaxErrorContaining("Invalid input '': expected a parameter or '{'")

  }

  test("CREATE CONSTRAINT FOR (node:Label) REQUIRE node.prop.part IS UNIQUE") {
    failsParsing[ast.Statements].withSyntaxErrorContaining("Invalid input '.': expected '::' or 'IS'")
  }

  test("CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop.part) IS UNIQUE") {
    failsParsing[ast.Statements].withSyntaxErrorContaining("Invalid input '.': expected ')' or ','")
  }

  test(
    "CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop) IS UNIQUE {indexProvider : 'range-1.0'}"
  ) {
    failsParsing[ast.Statements].withSyntaxErrorContaining(
      "Invalid input '{': expected 'OPTIONS' or <EOF>"
    )
  }

  test(
    "CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop1, node.prop2) IS UNIQUE OPTIONS"
  ) {
    failsParsing[ast.Statements].withSyntaxErrorContaining(
      "Invalid input '': expected a parameter or '{'"
    )
  }

  test(
    "CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop1, node.prop2) IS NOT NULL"
  ) {
    failsParsing[ast.Statements].withSyntaxErrorContaining(
      "Constraint type 'IS NOT NULL' does not allow multiple properties",
      GqlStatusInfoCodes.STATUS_42N16,
      "error: syntax error or access rule violation - unsupported index or constraint. Only single property 'IS NOT NULL' constraints are supported."
    )
  }

  test(
    "CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.prop1, r.prop2) IS NOT NULL"
  ) {
    failsParsing[ast.Statements].withSyntaxErrorContaining(
      "Constraint type 'IS NOT NULL' does not allow multiple properties",
      GqlStatusInfoCodes.STATUS_42N16,
      "error: syntax error or access rule violation - unsupported index or constraint. Only single property 'IS NOT NULL' constraints are supported."
    )
  }

  test(
    "CREATE CONSTRAINT FOR (node:Label) REQUIRE (node.prop1, node.prop2) IS TYPED BOOLEAN"
  ) {
    failsParsing[ast.Statements].withSyntaxErrorContaining(
      "Constraint type 'IS TYPED' does not allow multiple properties",
      GqlStatusInfoCodes.STATUS_42N16,
      "error: syntax error or access rule violation - unsupported index or constraint. Only single property 'IS TYPED' constraints are supported."
    )
  }

  test(
    "CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE (r.prop1, r.prop2) IS TYPED INTEGER"
  ) {
    failsParsing[ast.Statements].withSyntaxErrorContaining(
      "Constraint type 'IS TYPED' does not allow multiple properties",
      GqlStatusInfoCodes.STATUS_42N16,
      "error: syntax error or access rule violation - unsupported index or constraint. Only single property 'IS TYPED' constraints are supported."
    )
  }

  test("CREATE CONSTRAINT FOR ()-[r1:REL]-() REQUIRE (r2.prop) IS NODE KEY") {
    failsParsing[ast.Statements]
      .withMessageStart(ASTExceptionFactory.relationshipPatternNotAllowed(ConstraintType.NODE_KEY))
      .withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_22N04,
        "error: data exception - invalid input value. Invalid input 'relationship pattern' for IS NODE KEY. Expected 'node patterns'."
      ))
      .throws[SyntaxException]
  }

  test("CREATE CONSTRAINT FOR ()-[r1:REL]-() REQUIRE (r2.prop) IS NODE UNIQUE") {
    failsParsing[ast.Statements]
      .withMessageStart(ASTExceptionFactory.relationshipPatternNotAllowed(ConstraintType.NODE_UNIQUE))
      .withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_22N04,
        "error: data exception - invalid input value. Invalid input 'relationship pattern' for IS NODE UNIQUE. Expected 'node patterns'."
      ))
      .throws[SyntaxException]
  }

  test("CREATE CONSTRAINT FOR (node:Label) REQUIRE (r.prop) IS RELATIONSHIP KEY") {
    failsParsing[ast.Statements]
      .withMessageStart(ASTExceptionFactory.nodePatternNotAllowed(ConstraintType.REL_KEY))
      .withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_22N04,
        "error: data exception - invalid input value. Invalid input 'node pattern' for IS RELATIONSHIP KEY. Expected 'relationship patterns'."
      ))
      .throws[SyntaxException]
  }

  test("CREATE CONSTRAINT FOR (node:Label) REQUIRE (r.prop) IS REL KEY") {
    failsParsing[ast.Statements]
      .withMessageStart(ASTExceptionFactory.nodePatternNotAllowed(ConstraintType.REL_KEY))
      .withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_22N04,
        "error: data exception - invalid input value. Invalid input 'node pattern' for IS RELATIONSHIP KEY. Expected 'relationship patterns'."
      ))
      .throws[SyntaxException]
  }

  test(
    "CREATE CONSTRAINT FOR (node:Label) REQUIRE (r.prop) IS RELATIONSHIP UNIQUE"
  ) {
    failsParsing[ast.Statements]
      .withMessageStart(ASTExceptionFactory.nodePatternNotAllowed(ConstraintType.REL_UNIQUE))
      .withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_22N04,
        "error: data exception - invalid input value. Invalid input 'node pattern' for IS RELATIONSHIP UNIQUE. Expected 'relationship patterns'."
      ))
      .throws[SyntaxException]
  }

  test("CREATE CONSTRAINT FOR (node:Label) REQUIRE (r.prop) IS REL UNIQUE") {
    failsParsing[ast.Statements]
      .withMessageStart(ASTExceptionFactory.nodePatternNotAllowed(ConstraintType.REL_UNIQUE))
      .withSyntaxErrorGqlStatus(gqlStatus(
        GqlStatusInfoCodes.STATUS_22N04,
        "error: data exception - invalid input value. Invalid input 'node pattern' for IS RELATIONSHIP UNIQUE. Expected 'relationship patterns'."
      ))
      .throws[SyntaxException]
  }

  test(
    "CREATE CONSTRAINT FOR (node:Label) REQUIRE node.prop IS UNIQUENESS"
  ) {
    failsParsing[ast.Statements].withMessageStart("Invalid input 'UNIQUENESS'")
  }

  test(
    "CREATE CONSTRAINT FOR (node:Label) REQUIRE node.prop IS NODE UNIQUENESS"
  ) {
    failsParsing[ast.Statements].withMessageStart("Invalid input 'UNIQUENESS'")
  }

  test(
    "CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE r.prop IS UNIQUENESS"
  ) {
    failsParsing[ast.Statements].withMessageStart("Invalid input 'UNIQUENESS'")
  }

  test(
    "CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE r.prop IS RELATIONSHIP UNIQUENESS"
  ) {
    failsParsing[ast.Statements].withMessageStart("Invalid input 'UNIQUENESS'")
  }

  test(
    "CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE r.prop IS REL UNIQUENESS"
  ) {
    failsParsing[ast.Statements].withMessageStart("Invalid input 'UNIQUENESS'")
  }

  test("CREATE CONSTRAINT my_constraint FOR (n:L) REQUIRE n.p IS :: ANY<BOOLEAN | STRING> NOT NULL") {
    failsParsing[ast.Statements]
      .withSyntaxErrorContaining(
        "Closed Dynamic Union Types can not be appended with `NOT NULL`, specify `NOT NULL` on all inner types instead.",
        GqlStatusInfoCodes.STATUS_42I33,
        "error: syntax error or access rule violation - invalid use of NOT NULL. Closed Dynamic Union types cannot be appended with 'NOT NULL', specify 'NOT NULL' on inner types instead."
      )
  }

  test("CREATE CONSTRAINT my_constraint FOR (n:L) REQUIRE n.p IS :: BOOLEAN LIST NOT NULL NOT NULL") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'NOT': expected 'ARRAY', 'LIST', 'OPTIONS', '|' or <EOF> (line 1, column 83 (offset: 82))
        |"CREATE CONSTRAINT my_constraint FOR (n:L) REQUIRE n.p IS :: BOOLEAN LIST NOT NULL NOT NULL"
        |                                                                                   ^""".stripMargin
    )
  }

  test("CREATE CONSTRAINT FOR (:A)-[n1:R]-() REQUIRE (n2.name) IS RELATIONSHIP KEY") {
    // label on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ':': expected a variable name or ')' (line 1, column 24 (offset: 23))
        |"CREATE CONSTRAINT FOR (:A)-[n1:R]-() REQUIRE (n2.name) IS RELATIONSHIP KEY"
        |                        ^""".stripMargin
    )
  }

  test("CREATE CONSTRAINT FOR ()-[n1:R]-(:A) REQUIRE (n2.name) IS UNIQUE") {
    // label on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ':': expected ')' (line 1, column 34 (offset: 33))
        |"CREATE CONSTRAINT FOR ()-[n1:R]-(:A) REQUIRE (n2.name) IS UNIQUE"
        |                                  ^""".stripMargin
    )
  }

  test("CREATE CONSTRAINT FOR (n2)-[n1:R]-() REQUIRE (n2.name) IS NOT NULL") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input ')': expected ':' (line 1, column 26 (offset: 25))
        |"CREATE CONSTRAINT FOR (n2)-[n1:R]-() REQUIRE (n2.name) IS NOT NULL"
        |                          ^""".stripMargin
    )
  }

  test("CREATE CONSTRAINT FOR ()-[n1:R]-(n2) REQUIRE (n2.name) IS :: STRING") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'n2': expected ')' (line 1, column 34 (offset: 33))
        |"CREATE CONSTRAINT FOR ()-[n1:R]-(n2) REQUIRE (n2.name) IS :: STRING"
        |                                  ^""".stripMargin
    )
  }

  test("CREATE CONSTRAINT FOR (n2:A)-[n1:R]-() REQUIRE (n2.name) IS KEY") {
    // variable on node
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '-': expected 'ASSERT' or 'REQUIRE' (line 1, column 29 (offset: 28))
            |"CREATE CONSTRAINT FOR (n2:A)-[n1:R]-() REQUIRE (n2.name) IS KEY"
            |                             ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '-': expected 'REQUIRE' (line 1, column 29 (offset: 28))
            |"CREATE CONSTRAINT FOR (n2:A)-[n1:R]-() REQUIRE (n2.name) IS KEY"
            |                             ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR ()-[n1:R]-(n2:A) REQUIRE (n2.name) IS RELATIONSHIP UNIQUE") {
    // variable on node
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'n2': expected ')' (line 1, column 34 (offset: 33))
        |"CREATE CONSTRAINT FOR ()-[n1:R]-(n2:A) REQUIRE (n2.name) IS RELATIONSHIP UNIQUE"
        |                                  ^""".stripMargin
    )
  }

  test("CREATE CONSTRAINT FOR (n:Label) REQUIRE (n.prop)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '': expected '::' or 'IS' (line 1, column 49 (offset: 48))
        |"CREATE CONSTRAINT FOR (n:Label) REQUIRE (n.prop)"
        |                                                 ^""".stripMargin
    )
  }

  test("CREATE CONSTRAINT FOR (node:Label) REQUIRE EXISTS (node.prop)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '(': expected '.' (line 1, column 51 (offset: 50))
        |"CREATE CONSTRAINT FOR (node:Label) REQUIRE EXISTS (node.prop)"
        |                                                   ^""".stripMargin
    )
  }

  test("CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE EXISTS (r.prop)") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '(': expected '.' (line 1, column 50 (offset: 49))
        |"CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE EXISTS (r.prop)"
        |                                                  ^""".stripMargin
    )
  }

  test(s"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE r.prop IS NULL") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'NULL': expected '::', 'KEY', 'NODE', 'NOT NULL', 'REL', 'RELATIONSHIP', 'TYPED' or 'UNIQUE' (line 1, column 67 (offset: 66))
        |"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE r.prop IS NULL"
        |                                                                   ^""".stripMargin
    )
  }

  test(s"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE node.prop IS NULL") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'NULL': expected '::', 'KEY', 'NODE', 'NOT NULL', 'REL', 'RELATIONSHIP', 'TYPED' or 'UNIQUE' (line 1, column 71 (offset: 70))
        |"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE node.prop IS NULL"
        |                                                                       ^""".stripMargin
    )
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS TYPED") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          """Invalid input '': expected 'ANY', 'ARRAY', 'BOOL', 'BOOLEAN', 'DATE', 'DURATION', 'EDGE', 'FLOAT', 'INT', 'INTEGER', 'LIST', 'LOCAL', 'MAP', 'NODE', 'NOTHING', 'NULL', 'PATH', 'PATHS', 'POINT', 'RELATIONSHIP', 'SIGNED', 'STRING', 'TIME', 'TIMESTAMP', 'PROPERTY VALUE', 'VARCHAR', 'VERTEX' or 'ZONED' (line 1, column 49 (offset: 48))
            |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS TYPED"
            |                                                 ^""".stripMargin
        )
      case _ => _.withSyntaxErrorContaining(
          """Invalid input '': expected 'ANY', 'ARRAY', 'BOOL', 'BOOLEAN', 'DATE', 'DURATION', 'EDGE', 'FLOAT', 'FLOAT64', 'INT', 'INT64', 'INTEGER', 'INTEGER64', 'LIST', 'LOCAL', 'MAP', 'NODE', 'NOTHING', 'NULL', 'PATH', 'PATHS', 'POINT', 'RELATIONSHIP', 'SIGNED', 'STRING', 'TIME', 'TIMESTAMP', 'UUID', 'PROPERTY VALUE', 'VARCHAR', 'VECTOR', 'VERTEX' or 'ZONED' (line 1, column 49 (offset: 48))
            |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS TYPED"
            |                                                 ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS ::") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          """Invalid input '': expected 'ANY', 'ARRAY', 'BOOL', 'BOOLEAN', 'DATE', 'DURATION', 'EDGE', 'FLOAT', 'INT', 'INTEGER', 'LIST', 'LOCAL', 'MAP', 'NODE', 'NOTHING', 'NULL', 'PATH', 'PATHS', 'POINT', 'RELATIONSHIP', 'SIGNED', 'STRING', 'TIME', 'TIMESTAMP', 'PROPERTY VALUE', 'VARCHAR', 'VERTEX' or 'ZONED' (line 1, column 46 (offset: 45))
            |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS ::"
            |                                              ^""".stripMargin
        )
      case _ => _.withSyntaxErrorContaining(
          """Invalid input '': expected 'ANY', 'ARRAY', 'BOOL', 'BOOLEAN', 'DATE', 'DURATION', 'EDGE', 'FLOAT', 'FLOAT64', 'INT', 'INT64', 'INTEGER', 'INTEGER64', 'LIST', 'LOCAL', 'MAP', 'NODE', 'NOTHING', 'NULL', 'PATH', 'PATHS', 'POINT', 'RELATIONSHIP', 'SIGNED', 'STRING', 'TIME', 'TIMESTAMP', 'UUID', 'PROPERTY VALUE', 'VARCHAR', 'VECTOR', 'VERTEX' or 'ZONED' (line 1, column 46 (offset: 45))
            |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS ::"
            |                                              ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p ::") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          """Invalid input '': expected 'ANY', 'ARRAY', 'BOOL', 'BOOLEAN', 'DATE', 'DURATION', 'EDGE', 'FLOAT', 'INT', 'INTEGER', 'LIST', 'LOCAL', 'MAP', 'NODE', 'NOTHING', 'NULL', 'PATH', 'PATHS', 'POINT', 'RELATIONSHIP', 'SIGNED', 'STRING', 'TIME', 'TIMESTAMP', 'PROPERTY VALUE', 'VARCHAR', 'VERTEX' or 'ZONED' (line 1, column 43 (offset: 42))
            |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p ::"
            |                                           ^""".stripMargin
        )
      case _ => _.withSyntaxErrorContaining(
          """Invalid input '': expected 'ANY', 'ARRAY', 'BOOL', 'BOOLEAN', 'DATE', 'DURATION', 'EDGE', 'FLOAT', 'FLOAT64', 'INT', 'INT64', 'INTEGER', 'INTEGER64', 'LIST', 'LOCAL', 'MAP', 'NODE', 'NOTHING', 'NULL', 'PATH', 'PATHS', 'POINT', 'RELATIONSHIP', 'SIGNED', 'STRING', 'TIME', 'TIMESTAMP', 'UUID', 'PROPERTY VALUE', 'VARCHAR', 'VECTOR', 'VERTEX' or 'ZONED' (line 1, column 43 (offset: 42))
            |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p ::"
            |                                           ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p :: TYPED") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          """Invalid input 'TYPED': expected 'ANY', 'ARRAY', 'BOOL', 'BOOLEAN', 'DATE', 'DURATION', 'EDGE', 'FLOAT', 'INT', 'INTEGER', 'LIST', 'LOCAL', 'MAP', 'NODE', 'NOTHING', 'NULL', 'PATH', 'PATHS', 'POINT', 'RELATIONSHIP', 'SIGNED', 'STRING', 'TIME', 'TIMESTAMP', 'PROPERTY VALUE', 'VARCHAR', 'VERTEX' or 'ZONED' (line 1, column 44 (offset: 43))
            |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p :: TYPED"
            |                                            ^""".stripMargin
        )
      case _ => _.withSyntaxErrorContaining(
          """Invalid input 'TYPED': expected 'ANY', 'ARRAY', 'BOOL', 'BOOLEAN', 'DATE', 'DURATION', 'EDGE', 'FLOAT', 'FLOAT64', 'INT', 'INT64', 'INTEGER', 'INTEGER64', 'LIST', 'LOCAL', 'MAP', 'NODE', 'NOTHING', 'NULL', 'PATH', 'PATHS', 'POINT', 'RELATIONSHIP', 'SIGNED', 'STRING', 'TIME', 'TIMESTAMP', 'UUID', 'PROPERTY VALUE', 'VARCHAR', 'VECTOR', 'VERTEX' or 'ZONED' (line 1, column 44 (offset: 43))
            |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p :: TYPED"
            |                                            ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p :: UNIQUE") {

    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          """Invalid input 'UNIQUE': expected 'ANY', 'ARRAY', 'BOOL', 'BOOLEAN', 'DATE', 'DURATION', 'EDGE', 'FLOAT', 'INT', 'INTEGER', 'LIST', 'LOCAL', 'MAP', 'NODE', 'NOTHING', 'NULL', 'PATH', 'PATHS', 'POINT', 'RELATIONSHIP', 'SIGNED', 'STRING', 'TIME', 'TIMESTAMP', 'PROPERTY VALUE', 'VARCHAR', 'VERTEX' or 'ZONED' (line 1, column 44 (offset: 43))
            |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p :: UNIQUE"
            |                                            ^""".stripMargin
        )
      case _ => _.withSyntaxErrorContaining(
          """Invalid input 'UNIQUE': expected 'ANY', 'ARRAY', 'BOOL', 'BOOLEAN', 'DATE', 'DURATION', 'EDGE', 'FLOAT', 'FLOAT64', 'INT', 'INT64', 'INTEGER', 'INTEGER64', 'LIST', 'LOCAL', 'MAP', 'NODE', 'NOTHING', 'NULL', 'PATH', 'PATHS', 'POINT', 'RELATIONSHIP', 'SIGNED', 'STRING', 'TIME', 'TIMESTAMP', 'UUID', 'PROPERTY VALUE', 'VARCHAR', 'VECTOR', 'VERTEX' or 'ZONED' (line 1, column 44 (offset: 43))
            |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p :: UNIQUE"
            |                                            ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p :: BOOLEAN UNIQUE") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'UNIQUE': expected '!', 'ARRAY', 'LIST', 'NOT NULL', 'OPTIONS', '|' or <EOF> (line 1, column 52 (offset: 51))
        |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p :: BOOLEAN UNIQUE"
        |                                                    ^""".stripMargin
    )
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: BOOL EAN") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'EAN': expected '!', 'ARRAY', 'LIST', 'NOT NULL', 'OPTIONS', '|' or <EOF> (line 1, column 52 (offset: 51))
        |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: BOOL EAN"
        |                                                    ^""".stripMargin
    )
  }

  test("CREATE CONSTRAINT typeConstraint FOR (n:Label) REQUIRE n.prop IS TYPED VECTOR<STRING>(2)") {
    // Invalid inner type
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'VECTOR'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input 'VECTOR'",
          fuzzyStatusDescr = true
        )
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '<'",
          GqlStatusInfoCodes.STATUS_42I06,
          "error: syntax error or access rule violation - invalid input. Invalid input '<'",
          fuzzyStatusDescr = true
        )
    }
  }

  test("CREATE CONSTRAINT typeConstraint FOR (n:Label) REQUIRE n.prop IS :: INTEGER32") {
    // Vector-only numeric coordinate type
    failsParsing[ast.Statements].withSyntaxErrorContaining(
      "Invalid input 'INTEGER32'",
      GqlStatusInfoCodes.STATUS_42I06,
      "error: syntax error or access rule violation - invalid input. Invalid input 'INTEGER32'",
      fuzzyStatusDescr = true
    )
  }

  // ON/ASSERT/EXISTS

  // Error messages for mixing old and new constraint syntax
  private val errorMessageOnRequire =
    "Invalid constraint syntax, ON should not be used in combination with REQUIRE. Replace ON with FOR."

  private val errorMessageForAssert =
    "Invalid constraint syntax, FOR should not be used in combination with ASSERT. Replace ASSERT with REQUIRE."

  private val errorMessageForAssertExists =
    "Invalid constraint syntax, FOR should not be used in combination with ASSERT EXISTS. Replace ASSERT EXISTS with REQUIRE ... IS NOT NULL."

  private val errorMessageOnAssert =
    "Invalid constraint syntax, ON and ASSERT should not be used. Replace ON with FOR and ASSERT with REQUIRE."

  private val errorMessageOnAssertExists =
    "Invalid constraint syntax, ON and ASSERT EXISTS should not be used. Replace ON with FOR and ASSERT EXISTS with REQUIRE ... IS NOT NULL."

  test(
    "CREATE CONSTRAINT ON (node:Label) REQUIRE (node.prop) IS NODE KEY"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnRequire, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON (node:Label) REQUIRE (node.prop) IS NODE KEY"
            |                      ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT FOR (node:Label) ASSERT (node.prop) IS KEY"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssert, testName, InputPosition(35, 1, 36))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 36 (offset: 35))
            |"CREATE CONSTRAINT FOR (node:Label) ASSERT (node.prop) IS KEY"
            |                                    ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssert, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY"
            |                      ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint ON (node:Label) REQUIRE (node.prop1,node.prop2) IS KEY"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnRequire, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON (node:Label) REQUIRE (node.prop1,node.prop2) IS KEY"
            |                                 ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint FOR (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssert, testName, InputPosition(49, 1, 50))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 50 (offset: 49))
            |"CREATE CONSTRAINT my_constraint FOR (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY"
            |                                                  ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS KEY"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssert, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS KEY"
            |                                 ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT ON ()-[r1:R]-() REQUIRE (r2.prop) IS KEY") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnRequire, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON ()-[r1:R]-() REQUIRE (r2.prop) IS KEY"
            |                      ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR ()-[r1:R]-() ASSERT (r2.prop) IS REL KEY") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssert, testName, InputPosition(35, 1, 36))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 36 (offset: 35))
            |"CREATE CONSTRAINT FOR ()-[r1:R]-() ASSERT (r2.prop) IS REL KEY"
            |                                    ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT ON ()-[r1:R]-() ASSERT (r2.prop) IS RELATIONSHIP KEY") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssert, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON ()-[r1:R]-() ASSERT (r2.prop) IS RELATIONSHIP KEY"
            |                      ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint ON ()<-[r1:R]-() REQUIRE (r2.prop) IS REL KEY"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnRequire, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON ()<-[r1:R]-() REQUIRE (r2.prop) IS REL KEY"
            |                                 ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint FOR ()<-[r1:R]-() ASSERT (r2.prop) IS RELATIONSHIP KEY"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssert, testName, InputPosition(50, 1, 51))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 51 (offset: 50))
            |"CREATE CONSTRAINT my_constraint FOR ()<-[r1:R]-() ASSERT (r2.prop) IS RELATIONSHIP KEY"
            |                                                   ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint ON ()<-[r1:R]-() ASSERT (r2.prop) IS KEY"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssert, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON ()<-[r1:R]-() ASSERT (r2.prop) IS KEY"
            |                                 ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT ON (node:Label) REQUIRE node.prop IS NODE UNIQUE"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnRequire, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON (node:Label) REQUIRE node.prop IS NODE UNIQUE"
            |                      ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT FOR (node:Label) ASSERT node.prop IS NODE UNIQUE"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssert, testName, InputPosition(35, 1, 36))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 36 (offset: 35))
            |"CREATE CONSTRAINT FOR (node:Label) ASSERT node.prop IS NODE UNIQUE"
            |                                    ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssert, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE"
            |                      ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint ON (node:Label) REQUIRE node.prop IS UNIQUE"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnRequire, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON (node:Label) REQUIRE node.prop IS UNIQUE"
            |                                 ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint FOR (node:Label) ASSERT node.prop IS NODE UNIQUE"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssert, testName, InputPosition(49, 1, 50))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 50 (offset: 49))
            |"CREATE CONSTRAINT my_constraint FOR (node:Label) ASSERT node.prop IS NODE UNIQUE"
            |                                                  ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT node.prop IS NODE UNIQUE"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssert, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT node.prop IS NODE UNIQUE"
            |                                 ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT ON ()-[r1:R]->() REQUIRE (r2.prop1, r3.prop2) IS RELATIONSHIP UNIQUE"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnRequire, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON ()-[r1:R]->() REQUIRE (r2.prop1, r3.prop2) IS RELATIONSHIP UNIQUE"
            |                      ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT FOR ()-[r1:R]->() ASSERT (r2.prop1, r3.prop2) IS REL UNIQUE"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssert, testName, InputPosition(36, 1, 37))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 37 (offset: 36))
            |"CREATE CONSTRAINT FOR ()-[r1:R]->() ASSERT (r2.prop1, r3.prop2) IS REL UNIQUE"
            |                                     ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT ON ()-[r1:R]->() ASSERT (r2.prop1, r3.prop2) IS UNIQUE"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssert, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON ()-[r1:R]->() ASSERT (r2.prop1, r3.prop2) IS UNIQUE"
            |                      ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint IF NOT EXISTS ON ()-[r1:R]-() REQUIRE (r2.prop) IS REL UNIQUE"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnRequire, testName, InputPosition(46, 1, 47))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'FOR' (line 1, column 47 (offset: 46))
            |"CREATE CONSTRAINT my_constraint IF NOT EXISTS ON ()-[r1:R]-() REQUIRE (r2.prop) IS REL UNIQUE"
            |                                               ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint IF NOT EXISTS FOR ()-[r1:R]-() ASSERT (r2.prop) IS UNIQUE"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssert, testName, InputPosition(63, 1, 64))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 64 (offset: 63))
            |"CREATE CONSTRAINT my_constraint IF NOT EXISTS FOR ()-[r1:R]-() ASSERT (r2.prop) IS UNIQUE"
            |                                                                ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint IF NOT EXISTS ON ()-[r1:R]-() ASSERT (r2.prop) IS RELATIONSHIP UNIQUE"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssert, testName, InputPosition(46, 1, 47))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'FOR' (line 1, column 47 (offset: 46))
            |"CREATE CONSTRAINT my_constraint IF NOT EXISTS ON ()-[r1:R]-() ASSERT (r2.prop) IS RELATIONSHIP UNIQUE"
            |                                               ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT ON (node:Label) REQUIRE node.prop IS NOT NULL") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnRequire, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON (node:Label) REQUIRE node.prop IS NOT NULL"
            |                      ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR (node:Label) ASSERT node.prop IS NOT NULL") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssert, testName, InputPosition(35, 1, 36))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 36 (offset: 35))
            |"CREATE CONSTRAINT FOR (node:Label) ASSERT node.prop IS NOT NULL"
            |                                    ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS NOT NULL") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssert, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS NOT NULL"
            |                      ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint ON (node:Label) REQUIRE (node.prop) IS NOT NULL"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnRequire, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON (node:Label) REQUIRE (node.prop) IS NOT NULL"
            |                                 ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint FOR (node:Label) ASSERT (node.prop) IS NOT NULL"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssert, testName, InputPosition(49, 1, 50))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 50 (offset: 49))
            |"CREATE CONSTRAINT my_constraint FOR (node:Label) ASSERT (node.prop) IS NOT NULL"
            |                                                  ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop) IS NOT NULL"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssert, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop) IS NOT NULL"
            |                                 ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT ON ()-[r:R]-() REQUIRE r.prop IS NOT NULL") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnRequire, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON ()-[r:R]-() REQUIRE r.prop IS NOT NULL"
            |                      ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR ()-[r:R]-() ASSERT r.prop IS NOT NULL") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssert, testName, InputPosition(34, 1, 35))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 35 (offset: 34))
            |"CREATE CONSTRAINT FOR ()-[r:R]-() ASSERT r.prop IS NOT NULL"
            |                                   ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT ON ()-[r:R]-() ASSERT r.prop IS NOT NULL") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssert, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON ()-[r:R]-() ASSERT r.prop IS NOT NULL"
            |                      ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT `$my_constraint` ON ()-[r:R]-() REQUIRE r.prop IS NOT NULL"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnRequire, testName, InputPosition(35, 1, 36))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 36 (offset: 35))
            |"CREATE CONSTRAINT `$my_constraint` ON ()-[r:R]-() REQUIRE r.prop IS NOT NULL"
            |                                    ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT `$my_constraint` FOR ()-[r:R]-() ASSERT r.prop IS NOT NULL"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssert, testName, InputPosition(51, 1, 52))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 52 (offset: 51))
            |"CREATE CONSTRAINT `$my_constraint` FOR ()-[r:R]-() ASSERT r.prop IS NOT NULL"
            |                                                    ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT `$my_constraint` ON ()-[r:R]-() ASSERT r.prop IS NOT NULL"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssert, testName, InputPosition(35, 1, 36))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 36 (offset: 35))
            |"CREATE CONSTRAINT `$my_constraint` ON ()-[r:R]-() ASSERT r.prop IS NOT NULL"
            |                                    ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT ON (node:Label) REQUIRE node.prop :: BOOLEAN"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnRequire, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON (node:Label) REQUIRE node.prop :: BOOLEAN"
            |                      ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT FOR (node:Label) ASSERT node.prop IS :: BOOLEAN"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssert, testName, InputPosition(35, 1, 36))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 36 (offset: 35))
            |"CREATE CONSTRAINT FOR (node:Label) ASSERT node.prop IS :: BOOLEAN"
            |                                    ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS TYPED BOOLEAN"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssert, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS TYPED BOOLEAN"
            |                      ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint ON (node:Label) REQUIRE (node.prop) IS :: STRING"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnRequire, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON (node:Label) REQUIRE (node.prop) IS :: STRING"
            |                                 ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint FOR (node:Label) ASSERT (node.prop) IS TYPED STRING"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssert, testName, InputPosition(49, 1, 50))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 50 (offset: 49))
            |"CREATE CONSTRAINT my_constraint FOR (node:Label) ASSERT (node.prop) IS TYPED STRING"
            |                                                  ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop) :: STRING"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssert, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop) :: STRING"
            |                                 ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT ON ()-[r:R]->() REQUIRE r.prop IS TYPED BOOLEAN") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnRequire, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON ()-[r:R]->() REQUIRE r.prop IS TYPED BOOLEAN"
            |                      ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR ()-[r:R]->() ASSERT r.prop :: BOOLEAN") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssert, testName, InputPosition(35, 1, 36))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 36 (offset: 35))
            |"CREATE CONSTRAINT FOR ()-[r:R]->() ASSERT r.prop :: BOOLEAN"
            |                                    ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT ON ()-[r:R]->() ASSERT r.prop IS :: BOOLEAN") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssert, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON ()-[r:R]->() ASSERT r.prop IS :: BOOLEAN"
            |                      ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint ON ()-[r:R]-() REQUIRE (r.prop) :: STRING"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnRequire, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON ()-[r:R]-() REQUIRE (r.prop) :: STRING"
            |                                 ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() ASSERT (r.prop) IS TYPED STRING"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssert, testName, InputPosition(48, 1, 49))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 49 (offset: 48))
            |"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() ASSERT (r.prop) IS TYPED STRING"
            |                                                 ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT (r.prop) IS :: STRING"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssert, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT (r.prop) IS :: STRING"
            |                                 ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR (node:Label) ASSERT EXISTS (node.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssertExists, testName, InputPosition(35, 1, 36))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 36 (offset: 35))
            |"CREATE CONSTRAINT FOR (node:Label) ASSERT EXISTS (node.prop)"
            |                                    ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT my_constraint FOR (node:Label) ASSERT EXISTS (node.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssertExists, testName, InputPosition(49, 1, 50))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 50 (offset: 49))
            |"CREATE CONSTRAINT my_constraint FOR (node:Label) ASSERT EXISTS (node.prop)"
            |                                                  ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssertExists, testName, InputPosition(34, 1, 35))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 35 (offset: 34))
            |"CREATE CONSTRAINT FOR ()-[r:R]-() ASSERT EXISTS (r.prop)"
            |                                   ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT `$my_constraint` FOR ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageForAssertExists, testName, InputPosition(51, 1, 52))
      case _ => _.withSyntaxError(
          """Invalid input 'ASSERT': expected 'REQUIRE' (line 1, column 52 (offset: 51))
            |"CREATE CONSTRAINT `$my_constraint` FOR ()-[r:R]-() ASSERT EXISTS (r.prop)"
            |                                                    ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)"
            |                      ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT ON (node1:Label) ASSERT EXISTS node2.prop") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON (node1:Label) ASSERT EXISTS node2.prop"
            |                      ^""".stripMargin
        )
    }
  }

  test("CREATE OR REPLACE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(29, 1, 30))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE OR REPLACE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)"
            |                                 ^""".stripMargin
        )
    }
  }

  test("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(43, 1, 44))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'FOR' (line 1, column 44 (offset: 43))
            |"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)"
            |                                            ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)"
            |                                 ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop) OPTIONS {}") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop) OPTIONS {}"
            |                      ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT ON (node1:Label) ASSERT EXISTS (node2.prop1, node3.prop2)") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withSyntaxErrorContaining(
          ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_EXISTS),
          GqlStatusInfoCodes.STATUS_42N16,
          s"error: syntax error or access rule violation - unsupported index or constraint. Only single property '${ConstraintType.NODE_EXISTS.description()}' constraints are supported."
        )
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON (node1:Label) ASSERT EXISTS (node2.prop1, node3.prop2)"
            |                      ^""".stripMargin,
          GqlStatusInfoCodes.STATUS_42I06,
          s"error: syntax error or access rule violation - invalid input. Invalid input '(', expected: 'IF NOT EXISTS' or 'FOR'."
        )
    }
  }

  test("CREATE CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop)"
            |                      ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON ()-[r:R]->() ASSERT EXISTS (r.prop)"
            |                      ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON ()<-[r:R]-() ASSERT EXISTS (r.prop)"
            |                      ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT ON ()-[r1:R]-() ASSERT EXISTS r2.prop") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(18, 1, 19))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON ()-[r1:R]-() ASSERT EXISTS r2.prop"
            |                      ^""".stripMargin
        )
    }
  }

  test("CREATE OR REPLACE CONSTRAINT ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(29, 1, 30))
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE OR REPLACE CONSTRAINT ON ()<-[r:R]-() ASSERT EXISTS (r.prop)"
            |                                 ^""".stripMargin
        )
    }
  }

  test("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(43, 1, 44))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'FOR' (line 1, column 44 (offset: 43))
            |"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON ()-[r:R]-() ASSERT EXISTS (r.prop)"
            |                                            ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:R]->() ASSERT EXISTS (r.prop)"
            |                                 ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT ON ()-[r1:REL]-() ASSERT EXISTS (r2.prop1, r3.prop2)") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withSyntaxErrorContaining(
          ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.REL_EXISTS),
          GqlStatusInfoCodes.STATUS_42N16,
          s"error: syntax error or access rule violation - unsupported index or constraint. Only single property '${ConstraintType.REL_EXISTS.description()}' constraints are supported."
        )
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON ()-[r1:REL]-() ASSERT EXISTS (r2.prop1, r3.prop2)"
            |                      ^""".stripMargin,
          GqlStatusInfoCodes.STATUS_42I06,
          s"error: syntax error or access rule violation - invalid input. Invalid input '(', expected: 'IF NOT EXISTS' or 'FOR'."
        )
    }
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop)"
            |                                 ^""".stripMargin
        )
    }
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(43, 1, 44))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 44 (offset: 43))
            |"CREATE OR REPLACE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop)"
            |                                            ^""".stripMargin
        )
    }
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(57, 1, 58))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'FOR' (line 1, column 58 (offset: 57))
            |"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)"
            |                                                          ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(46, 1, 47))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'FOR' (line 1, column 47 (offset: 46))
            |"CREATE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)"
            |                                               ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT `$my_constraint` ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(35, 1, 36))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 36 (offset: 35))
            |"CREATE CONSTRAINT `$my_constraint` ON ()-[r:R]-() ASSERT EXISTS (r.prop)"
            |                                    ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop) OPTIONS {}") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop) OPTIONS {}"
            |                                 ^""".stripMargin
        )
    }
  }

  test("CREATE OR REPLACE CONSTRAINT `$my_constraint` ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(46, 1, 47))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 47 (offset: 46))
            |"CREATE OR REPLACE CONSTRAINT `$my_constraint` ON ()-[r:R]-() ASSERT EXISTS (r.prop)"
            |                                               ^""".stripMargin
        )
    }
  }

  test("CREATE OR REPLACE CONSTRAINT `$my_constraint` IF NOT EXISTS ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(60, 1, 61))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'FOR' (line 1, column 61 (offset: 60))
            |"CREATE OR REPLACE CONSTRAINT `$my_constraint` IF NOT EXISTS ON ()-[r:R]->() ASSERT EXISTS (r.prop)"
            |                                                             ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT `$my_constraint` IF NOT EXISTS ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(49, 1, 50))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'FOR' (line 1, column 50 (offset: 49))
            |"CREATE CONSTRAINT `$my_constraint` IF NOT EXISTS ON ()<-[r:R]-() ASSERT EXISTS (r.prop)"
            |                                                  ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop1, node.prop2)"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Constraint type 'EXISTS' does not allow multiple properties (line",
          GqlStatusInfoCodes.STATUS_42N16,
          "error: syntax error or access rule violation - unsupported index or constraint. Only single property 'EXISTS' constraints are supported."
        )
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop1, node.prop2)"
            |                      ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop1, r.prop2)"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Constraint type 'EXISTS' does not allow multiple properties (line",
          GqlStatusInfoCodes.STATUS_42N16,
          "error: syntax error or access rule violation - unsupported index or constraint. Only single property 'EXISTS' constraints are supported."
        )
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop1, r.prop2)"
            |                      ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop1, r.prop2) IS NOT NULL"
  ) {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Constraint type 'EXISTS' does not allow multiple properties (line",
          GqlStatusInfoCodes.STATUS_42N16,
          "error: syntax error or access rule violation - unsupported index or constraint. Only single property 'EXISTS' constraints are supported."
        )
      case _ => // parses ON as constraint name
        _.withSyntaxError(
          """Invalid input '(': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 22 (offset: 21))
            |"CREATE CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop1, r.prop2) IS NOT NULL"
            |                      ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT $my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(33, 1, 34))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 34 (offset: 33))
            |"CREATE CONSTRAINT $my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop)"
            |                                  ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop) IS NOT NULL") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop) IS NOT NULL"
            |                                 ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop) IS NOT NULL") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntaxWithPosition(errorMessageOnAssertExists, testName, InputPosition(32, 1, 33))
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop) IS NOT NULL"
            |                                 ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT r.prop IS NULL") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'NULL': expected '::', 'KEY', 'NODE', 'NOT NULL', 'REL', 'RELATIONSHIP', 'TYPED' or 'UNIQUE' (line 1, column 65 (offset: 64))
            |"CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT r.prop IS NULL"
            |                                                                 ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT r.prop IS NULL"
            |                                 ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT node.prop IS NULL") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input 'NULL': expected '::', 'KEY', 'NODE', 'NOT NULL', 'REL', 'RELATIONSHIP', 'TYPED' or 'UNIQUE' (line 1, column 69 (offset: 68))
            |"CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT node.prop IS NULL"
            |                                                                     ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF NOT EXISTS' or 'FOR' (line 1, column 33 (offset: 32))
            |"CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT node.prop IS NULL"
            |                                 ^""".stripMargin
        )
    }
  }

  test(
    "CREATE CONSTRAINT FOR (node:$(Label)) REQUIRE (node.prop) IS NODE KEY"
  ) {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '$': expected an identifier (line 1, column 29 (offset: 28))
        |"CREATE CONSTRAINT FOR (node:$(Label)) REQUIRE (node.prop) IS NODE KEY"
        |                             ^""".stripMargin
    )
  }

  test(
    "CREATE CONSTRAINT $name FOR ()-[r:$(R)]-() REQUIRE r.prop IS NOT NULL"
  ) {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input '$': expected an identifier (line 1, column 35 (offset: 34))
        |"CREATE CONSTRAINT $name FOR ()-[r:$(R)]-() REQUIRE r.prop IS NOT NULL"
        |                                   ^""".stripMargin
    )
  }

  // Drop constraint by schema (throws in parsing)

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withOldSyntax(ASTExceptionFactory.invalidDropConstraint(ConstraintType.NODE_KEY, false))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (node1:Label) ASSERT node2.prop IS NODE KEY") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withOldSyntax(ASTExceptionFactory.invalidDropConstraint(ConstraintType.NODE_KEY, false))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withOldSyntax(ASTExceptionFactory.invalidDropConstraint(ConstraintType.NODE_KEY, false))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withOldSyntax(ASTExceptionFactory.invalidDropConstraint(ConstraintType.NODE_UNIQUE, false))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop) IS UNIQUE") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withOldSyntax(ASTExceptionFactory.invalidDropConstraint(ConstraintType.NODE_UNIQUE, false))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withOldSyntax(ASTExceptionFactory.invalidDropConstraint(ConstraintType.NODE_UNIQUE, false))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withOldSyntax(ASTExceptionFactory.invalidDropConstraint(ConstraintType.NODE_EXISTS, false))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (node1:Label) ASSERT EXISTS node2.prop") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withOldSyntax(ASTExceptionFactory.invalidDropConstraint(ConstraintType.NODE_EXISTS, false))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withOldSyntax(ASTExceptionFactory.invalidDropConstraint(ConstraintType.REL_EXISTS, false))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withOldSyntax(ASTExceptionFactory.invalidDropConstraint(ConstraintType.REL_EXISTS, false))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withOldSyntax(ASTExceptionFactory.invalidDropConstraint(ConstraintType.REL_EXISTS, false))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON ()-[r1:R]-() ASSERT EXISTS r2.prop") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withOldSyntax(ASTExceptionFactory.invalidDropConstraint(ConstraintType.REL_EXISTS, false))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.EXISTS) IS NODE KEY") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withOldSyntax(ASTExceptionFactory.invalidDropConstraint(ConstraintType.NODE_KEY, false))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.EXISTS) IS UNIQUE") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withOldSyntax(ASTExceptionFactory.invalidDropConstraint(ConstraintType.NODE_UNIQUE, false))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT EXISTS (node.EXISTS)") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withOldSyntax(ASTExceptionFactory.invalidDropConstraint(ConstraintType.NODE_EXISTS, false))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.EXISTS)") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withOldSyntax(ASTExceptionFactory.invalidDropConstraint(ConstraintType.REL_EXISTS, false))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON ()-[r1:R]-() ASSERT r2.prop IS NODE KEY") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withSyntaxErrorContaining(ASTExceptionFactory.relationshipPatternNotAllowed(ConstraintType.NODE_KEY))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON ()-[r1:R]-() ASSERT r2.prop IS UNIQUE") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withSyntaxErrorContaining(ASTExceptionFactory.relationshipPatternNotAllowed(ConstraintType.NODE_UNIQUE))
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT EXISTS (n.p1, n.p2)") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withSyntaxErrorContaining(
          ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_EXISTS),
          GqlStatusInfoCodes.STATUS_42N16,
          s"error: syntax error or access rule violation - unsupported index or constraint. Only single property '${ConstraintType.NODE_EXISTS.description()}' constraints are supported."
        )
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '(': expected 'IF EXISTS' or <EOF> (line",
          GqlStatusInfoCodes.STATUS_42I06,
          s"error: syntax error or access rule violation - invalid input. Invalid input '(', expected: 'IF EXISTS' or <EOF>."
        )
    }
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.p1, r.p2)") {
    failsParsing[ast.Statements].in {
      case Cypher5 =>
        _.withSyntaxErrorContaining(
          ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.REL_EXISTS),
          GqlStatusInfoCodes.STATUS_42N16,
          s"error: syntax error or access rule violation - unsupported index or constraint. Only single property '${ConstraintType.REL_EXISTS.description()}' constraints are supported."
        )
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '(': expected 'IF EXISTS' or <EOF> (line",
          GqlStatusInfoCodes.STATUS_42I06,
          s"error: syntax error or access rule violation - invalid input. Invalid input '(', expected: 'IF EXISTS' or <EOF>."
        )
    }
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NOT NULL") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(ASTExceptionFactory.invalidDropCommand)
      case _       => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS NOT NULL") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(ASTExceptionFactory.invalidDropCommand)
      case _       => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT (r.prop) IS NOT NULL") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(ASTExceptionFactory.invalidDropCommand)
      case _       => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.prop IS NOT NULL") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(ASTExceptionFactory.invalidDropCommand)
      case _       => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT (n.p1, n.p2) IS NOT NULL") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(ASTExceptionFactory.invalidDropCommand)
      case _       => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT (r.p1, r.p2) IS NOT NULL") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withOldSyntax(ASTExceptionFactory.invalidDropCommand)
      case _       => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT FOR (n:L) REQUIRE n.p IS NODE KEY") {
    // Parses FOR as constraint name
    failsParsing[ast.Statements].withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
  }

  test("DROP CONSTRAINT FOR (n:L) ASSERT n.p IS NODE KEY") {
    // Parses FOR as constraint name
    failsParsing[ast.Statements].withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
  }

  test("DROP CONSTRAINT ON (n:L) REQUIRE n.p IS NODE KEY") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'REQUIRE': expected 'ASSERT' (line")
      case _       => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT FOR (n:L) REQUIRE n.p IS UNIQUE") {
    // Parses FOR as constraint name
    failsParsing[ast.Statements].withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
  }

  test("DROP CONSTRAINT FOR (n:L) ASSERT n.p IS UNIQUE") {
    // Parses FOR as constraint name
    failsParsing[ast.Statements].withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
  }

  test("DROP CONSTRAINT ON (n:L) REQUIRE n.p IS UNIQUE") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'REQUIRE': expected 'ASSERT' (line")
      case _       => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT FOR (n:L) REQUIRE EXISTS n.p") {
    // Parses FOR as constraint name
    failsParsing[ast.Statements].withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
  }

  test("DROP CONSTRAINT FOR (n:L) ASSERT EXISTS n.p") {
    // Parses FOR as constraint name
    failsParsing[ast.Statements].withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
  }

  test("DROP CONSTRAINT ON (n:L) REQUIRE EXISTS n.p") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'REQUIRE': expected 'ASSERT' (line")
      case _       => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT n.p IS REL KEY") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'REL': expected 'NODE KEY', 'NOT NULL' or 'UNIQUE' (line"
        )
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT n.p IS RELATIONSHIP KEY") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'RELATIONSHIP': expected 'NODE KEY', 'NOT NULL' or 'UNIQUE' (line"
        )
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT n.p IS REL UNIQUE") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'REL': expected 'NODE KEY', 'NOT NULL' or 'UNIQUE' (line"
        )
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT n.p IS RELATIONSHIP UNIQUE") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'RELATIONSHIP': expected 'NODE KEY', 'NOT NULL' or 'UNIQUE' (line"
        )
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT n.p IS NODE UNIQUE") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input 'UNIQUE': expected 'KEY' (line")
      case _       => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT n.p IS TYPED INT") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'TYPED': expected 'NODE KEY', 'NOT NULL' or 'UNIQUE' (line"
        )
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.p IS TYPED STRING") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input 'TYPED': expected 'NODE KEY', 'NOT NULL' or 'UNIQUE' (line"
        )
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT n.p IS :: LIST<FLOAT>") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input '::': expected 'NODE KEY', 'NOT NULL' or 'UNIQUE' (line"
        )
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.p IS :: BOOLEAN") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining(
          "Invalid input '::': expected 'NODE KEY', 'NOT NULL' or 'UNIQUE' (line"
        )
      case _ => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT n.p :: ZONED DATETIME") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input '::': expected 'IS' (line")
      case _       => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.p :: LOCAL TIME") {
    failsParsing[ast.Statements].in {
      case Cypher5 => _.withSyntaxErrorContaining("Invalid input '::': expected 'IS' (line")
      case _       => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  // Drop constraint by name

  test("DROP CONSTRAINT my_constraint") {
    assertAst(ast.DropConstraintOnName("my_constraint", ifExists = false)(pos))
  }

  test("DROP CONSTRAINT `$my_constraint`") {
    assertAst(ast.DropConstraintOnName("$my_constraint", ifExists = false)(pos))
  }

  test("DROP CONSTRAINT my_constraint IF EXISTS") {
    assertAst(ast.DropConstraintOnName("my_constraint", ifExists = true)(pos))
  }

  test("DROP CONSTRAINT $my_constraint") {
    assertAst(
      ast.DropConstraintOnName(stringParam("my_constraint"), ifExists = false)(pos)
    )
  }

  test("DROP CONSTRAINT my_constraint IF EXISTS;") {
    assertAst(ast.DropConstraintOnName("my_constraint", ifExists = true)(pos))
  }

  test("DROP CONSTRAINT my_constraint; DROP CONSTRAINT my_constraint2;") {
    // kept the version based as it takes in Statements instead of single Statement
    assertAstVersionBased(_ =>
      ast.Statements(Seq(
        ast.DropConstraintOnName("my_constraint", ifExists = false)(pos),
        ast.DropConstraintOnName("my_constraint2", ifExists = false)(pos)
      ))
    )
  }

  test("DROP CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    failsParsing[ast.Statements].withSyntaxError(
      """Invalid input 'ON': expected 'IF EXISTS' or <EOF> (line 1, column 31 (offset: 30))
        |"DROP CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY"
        |                               ^""".stripMargin
    )
  }
}
