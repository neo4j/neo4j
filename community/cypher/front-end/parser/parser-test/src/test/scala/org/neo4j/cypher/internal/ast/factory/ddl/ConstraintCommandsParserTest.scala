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
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.neo4j.Neo4jASTConstructionException
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.parser.common.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.parser.common.ast.factory.ConstraintType
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.symbols.AnyType
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.ClosedDynamicUnionType
import org.neo4j.cypher.internal.util.symbols.CypherType
import org.neo4j.cypher.internal.util.symbols.DateType
import org.neo4j.cypher.internal.util.symbols.DurationType
import org.neo4j.cypher.internal.util.symbols.FloatType
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
import org.neo4j.cypher.internal.util.symbols.PropertyValueType
import org.neo4j.cypher.internal.util.symbols.RelationshipType
import org.neo4j.cypher.internal.util.symbols.StringType
import org.neo4j.cypher.internal.util.symbols.ZonedDateTimeType
import org.neo4j.cypher.internal.util.symbols.ZonedTimeType
import org.neo4j.exceptions.SyntaxException

/* Tests for creating and dropping constraints */
class ConstraintCommandsParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq("ON", "FOR")
    .foreach { forOrOnString =>
      Seq("ASSERT", "REQUIRE")
        .foreach { requireOrAssertString =>
          val containsOn = forOrOnString == "ON"
          val constraintVersion =
            if (requireOrAssertString == "REQUIRE") ast.ConstraintVersion2 else ast.ConstraintVersion0
          val constraintVersionOneOrTwo =
            if (requireOrAssertString == "REQUIRE") ast.ConstraintVersion2 else ast.ConstraintVersion1

          // Create constraint: Without name

          Seq("NODE", "").foreach(nodeKeyword => {
            // Node key

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY"
            ) {
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS $nodeKeyword KEY"
            ) {
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY"
            ) {
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY"
            ) {
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword KEY"
            ) {
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword KEY"
            ) {
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                None,
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY OPTIONS {indexProvider : 'range-1.0'}"
            ) {
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexProvider" -> literalString("range-1.0"))),
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}"
            ) {
              // will fail in options converter
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
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
                )),
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY OPTIONS {indexConfig : {someConfig: 'toShowItCanBeParsed' }}"
            ) {
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexConfig" -> mapOf("someConfig" -> literalString("toShowItCanBeParsed")))),
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY OPTIONS {nonValidOption : 42}"
            ) {
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("nonValidOption" -> literalInt(42))),
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY OPTIONS {}"
            ) {
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map.empty),
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY OPTIONS $$param"
            ) {
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsParam(parameter("param", CTMap)),
                containsOn,
                constraintVersion
              )(pos))
            }

            // Node uniqueness

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS $nodeKeyword UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS $nodeKeyword UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop IS $nodeKeyword UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                None,
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}, indexProvider : 'native-btree-1.0'}"
            ) {
              // will fail in options converter
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
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
                )),
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE OPTIONS $$options"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsParam(parameter("options", CTMap)),
                containsOn,
                constraintVersion
              )(pos))
            }
          })

          Seq("RELATIONSHIP", "REL", "").foreach(relKeyWord => {
            // Relationship key

            test(s"CREATE CONSTRAINT $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY") {
              parsesTo[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()-[r1:R]->() $requireOrAssertString (r2.prop1, r3.prop2) IS $relKeyWord KEY"
            ) {
              parsesTo[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop1"), prop("r3", "prop2")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()<-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              parsesTo[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()<-[r1:R]->() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              parsesTo[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              parsesTo[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              parsesTo[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              parsesTo[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY OPTIONS {key: 'value'}"
            ) {
              parsesTo[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("key" -> literalString("value"))),
                containsOn,
                constraintVersion
              )(pos))
            }

            // Relationship uniqueness

            test(
              s"CREATE CONSTRAINT $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()-[r1:R]->() $requireOrAssertString (r2.prop1, r3.prop2) IS $relKeyWord UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop1"), prop("r3", "prop2")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()<-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()<-[r1:R]->() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE OPTIONS {key: 'value'}"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("key" -> literalString("value"))),
                containsOn,
                constraintVersion
              )(pos))
            }
          })

          // Node property existence

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL") {
            parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              None,
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NOT NULL") {
            parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              None,
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL"
          ) {
            parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              None,
              ast.IfExistsReplace,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL"
          ) {
            parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              None,
              ast.IfExistsInvalidSyntax,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(
            s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NOT NULL"
          ) {
            parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              None,
              ast.IfExistsDoNothing,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          // Relationship property existence

          test(s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS NOT NULL") {
            parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              None,
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()-[r:R]->() $requireOrAssertString r.prop IS NOT NULL") {
            parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              None,
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()<-[r:R]-() $requireOrAssertString (r.prop) IS NOT NULL") {
            parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              None,
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()<-[r:R]->() $requireOrAssertString (r.prop) IS NOT NULL") {
            parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              None,
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(s"CREATE OR REPLACE CONSTRAINT $forOrOnString ()<-[r:R]-() $requireOrAssertString r.prop IS NOT NULL") {
            parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              None,
              ast.IfExistsReplace,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS NOT NULL"
          ) {
            parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              None,
              ast.IfExistsInvalidSyntax,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(
            s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r:R]->() $requireOrAssertString r.prop IS NOT NULL"
          ) {
            parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              None,
              ast.IfExistsDoNothing,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString (r.prop) IS NOT NULL OPTIONS {}") {
            parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              None,
              ast.IfExistsThrowError,
              ast.OptionsMap(Map.empty),
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          Seq("IS TYPED", "IS ::", "::").foreach(typeKeyword => {
            // Node property type

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop $typeKeyword BOOLEAN"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) $typeKeyword BOOLEAN"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop $typeKeyword BOOLEAN"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop $typeKeyword BOOLEAN"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop) $typeKeyword BOOLEAN"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            // Relationship property type

            test(s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop $typeKeyword BOOLEAN") {
              parsesTo[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(s"CREATE CONSTRAINT $forOrOnString ()-[r:R]->() $requireOrAssertString r.prop $typeKeyword BOOLEAN") {
              parsesTo[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()<-[r:R]-() $requireOrAssertString (r.prop) $typeKeyword BOOLEAN"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()<-[r:R]->() $requireOrAssertString (r.prop) $typeKeyword BOOLEAN"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT $forOrOnString ()<-[r:R]-() $requireOrAssertString r.prop $typeKeyword BOOLEAN"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop $typeKeyword BOOLEAN"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r:R]->() $requireOrAssertString r.prop $typeKeyword BOOLEAN"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString (r.prop) $typeKeyword BOOLEAN OPTIONS {}"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map.empty),
                containsOn,
                constraintVersion
              )(pos))
            }
          })

          // Create constraint: With name

          Seq("NODE", "").foreach(nodeKeyword => {
            // Node key
            test(
              s"USE neo4j CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY"
            ) {
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion,
                Some(use(List("neo4j")))
              )(pos))
            }

            test(
              s"USE neo4j CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY"
            ) {
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                Some("my_constraint"),
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion,
                Some(use(List("neo4j")))
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword KEY"
            ) {
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword KEY"
            ) {
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                Some("my_constraint"),
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword KEY"
            ) {
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                Some("my_constraint"),
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}}"
            ) {
              // will fail in options converter
              parsesTo[Statements](ast.CreateNodeKeyConstraint(
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
                )),
                containsOn,
                constraintVersion
              )(pos))
            }

            // Node uniqueness

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString node.prop IS $nodeKeyword UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                Some("my_constraint"),
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                Some("my_constraint"),
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                Some("my_constraint"),
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE OPTIONS {indexProvider : 'range-1.0'}"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexProvider" -> literalString("range-1.0"))),
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}"
            ) {
              // will fail in options converter
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
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
                )),
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE OPTIONS {indexConfig : {someConfig: 'toShowItCanBeParsed' }}"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexConfig" -> mapOf("someConfig" -> literalString("toShowItCanBeParsed")))),
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE OPTIONS {nonValidOption : 42}"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("nonValidOption" -> literalInt(42))),
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword UNIQUE OPTIONS {}"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.OptionsMap(Map.empty),
                containsOn,
                constraintVersion
              )(pos))
            }
          })

          Seq("RELATIONSHIP", "REL", "").foreach(relKeyWord => {
            // Relationship key

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              parsesTo[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()-[r1:R]->() $requireOrAssertString (r2.prop1, r3.prop2) IS $relKeyWord KEY"
            ) {
              parsesTo[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop1"), prop("r3", "prop2")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()<-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              parsesTo[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()<-[r1:R]->() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              parsesTo[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              parsesTo[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              parsesTo[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              parsesTo[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY OPTIONS {key: 'value'}"
            ) {
              parsesTo[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("key" -> literalString("value"))),
                containsOn,
                constraintVersion
              )(pos))
            }

            // Relationship uniqueness

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()-[r1:R]->() $requireOrAssertString (r2.prop1, r3.prop2) IS $relKeyWord UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop1"), prop("r3", "prop2")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()<-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()<-[r1:R]->() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE OPTIONS {key: 'value'}"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("key" -> literalString("value"))),
                containsOn,
                constraintVersion
              )(pos))
            }
          })

          // Node property existence

          test(
            s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NOT NULL"
          ) {
            parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              Some("my_constraint"),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL"
          ) {
            parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              Some("my_constraint"),
              ast.IfExistsReplace,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL"
          ) {
            parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              Some("my_constraint"),
              ast.IfExistsInvalidSyntax,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(
            s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL"
          ) {
            parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              Some("my_constraint"),
              ast.IfExistsDoNothing,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(
            s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL OPTIONS {}"
          ) {
            parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              Some("my_constraint"),
              ast.IfExistsThrowError,
              ast.OptionsMap(Map.empty),
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop2, node.prop3) IS NOT NULL"
          ) {
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.throws[Neo4jASTConstructionException].withMessage(
                  ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_IS_NOT_NULL)
                )
              case _ => _.withSyntaxErrorContaining(
                  ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_IS_NOT_NULL)
                )
            }
          }

          // Relationship property existence

          test(
            s"CREATE CONSTRAINT `$$my_constraint` $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS NOT NULL"
          ) {
            parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              Some("$my_constraint"),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(
            s"CREATE CONSTRAINT my_constraint $forOrOnString ()-[r:R]-() $requireOrAssertString (r.prop) IS NOT NULL"
          ) {
            parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              Some("my_constraint"),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT `$$my_constraint` $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS NOT NULL"
          ) {
            parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              Some("$my_constraint"),
              ast.IfExistsReplace,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT `$$my_constraint` IF NOT EXISTS $forOrOnString ()-[r:R]->() $requireOrAssertString (r.prop) IS NOT NULL"
          ) {
            parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              Some("$my_constraint"),
              ast.IfExistsInvalidSyntax,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(
            s"CREATE CONSTRAINT `$$my_constraint` IF NOT EXISTS $forOrOnString ()<-[r:R]-() $requireOrAssertString r.prop IS NOT NULL"
          ) {
            parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              Some("$my_constraint"),
              ast.IfExistsDoNothing,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString ()-[r1:REL]-() $requireOrAssertString (r2.prop2, r3.prop3) IS NOT NULL"
          ) {
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.throws[Neo4jASTConstructionException].withMessage(
                  ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.REL_IS_NOT_NULL)
                )
              case _ => _.withSyntaxErrorContaining(
                  ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.REL_IS_NOT_NULL)
                )
            }
          }

          Seq("IS TYPED", "IS ::", "::").foreach(typeKeyword => {
            // Node property type

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) $typeKeyword STRING"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                StringType(isNullable = true)(pos),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString node.prop $typeKeyword STRING"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                StringType(isNullable = true)(pos),
                Some("my_constraint"),
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop $typeKeyword STRING"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                StringType(isNullable = true)(pos),
                Some("my_constraint"),
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop $typeKeyword STRING"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                StringType(isNullable = true)(pos),
                Some("my_constraint"),
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString node.prop $typeKeyword STRING OPTIONS {}"
            ) {
              parsesTo[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                StringType(isNullable = true)(pos),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.OptionsMap(Map.empty),
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop2, node.prop3) $typeKeyword STRING"
            ) {
              failsParsing[Statements].in {
                case Cypher5JavaCc => _.throws[Neo4jASTConstructionException].withMessage(
                    ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_IS_TYPED)
                  )
                case _ => _.withSyntaxErrorContaining(
                    ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_IS_TYPED)
                  )
              }
            }

            // Relationship property type

            test(
              s"CREATE CONSTRAINT `$$my_constraint` $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop $typeKeyword STRING"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                StringType(isNullable = true)(pos),
                Some("$my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()-[r:R]-() $requireOrAssertString (r.prop) $typeKeyword STRING"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                StringType(isNullable = true)(pos),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT `$$my_constraint` $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop $typeKeyword STRING"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                StringType(isNullable = true)(pos),
                Some("$my_constraint"),
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT `$$my_constraint` IF NOT EXISTS $forOrOnString ()-[r:R]->() $requireOrAssertString (r.prop) $typeKeyword STRING"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                StringType(isNullable = true)(pos),
                Some("$my_constraint"),
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE CONSTRAINT `$$my_constraint` IF NOT EXISTS $forOrOnString ()<-[r:R]-() $requireOrAssertString r.prop $typeKeyword STRING"
            ) {
              parsesTo[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                StringType(isNullable = true)(pos),
                Some("$my_constraint"),
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              )(pos))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString ()-[r1:REL]-() $requireOrAssertString (r2.prop2, r3.prop3) $typeKeyword STRING"
            ) {
              failsParsing[Statements].in {
                case Cypher5JavaCc => _.throws[Neo4jASTConstructionException].withMessage(
                    ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.REL_IS_TYPED)
                  )
                case _ => _.withSyntaxErrorContaining(
                    ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.REL_IS_TYPED)
                  )
              }
            }
          })

          // Negative tests

          test(
            s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY {indexProvider : 'range-1.0'}"
          ) {
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart("Invalid input '{': expected \"OPTIONS\" or <EOF>")
              case _ =>
                _.withSyntaxErrorContaining("Invalid input '{': expected 'OPTIONS' or <EOF>")
            }
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY OPTIONS"
          ) {
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart("Invalid input '': expected \"{\" or a parameter")
              case _ =>
                _.withSyntaxErrorContaining("Invalid input '': expected a parameter or '{'")

            }
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop.part IS UNIQUE") {
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart("Invalid input '.': expected \"::\" or \"IS\"")
              case _             => _.withSyntaxErrorContaining("Invalid input '.': expected '::' or 'IS'")
            }
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop.part) IS UNIQUE") {
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart("Invalid input '.': expected \")\" or \",\"")
              case _             => _.withSyntaxErrorContaining("Invalid input '.': expected ')' or ','")
            }
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS UNIQUE {indexProvider : 'range-1.0'}"
          ) {
            failsParsing[Statements].in {
              case Cypher5JavaCc =>
                _.withMessageStart("Invalid input '{': expected \"OPTIONS\" or <EOF>")
              case _ => _.withSyntaxErrorContaining(
                  "Invalid input '{': expected 'OPTIONS' or <EOF>"
                )
            }
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop1, node.prop2) IS UNIQUE OPTIONS"
          ) {
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessageStart("Invalid input '': expected \"{\" or a parameter")
              case _ => _.withSyntaxErrorContaining(
                  "Invalid input '': expected a parameter or '{'"
                )
            }
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop1, node.prop2) IS NOT NULL"
          ) {
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessage("Constraint type 'IS NOT NULL' does not allow multiple properties")
              case _ => _.withSyntaxErrorContaining(
                  "Constraint type 'IS NOT NULL' does not allow multiple properties"
                )
            }
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString (r.prop1, r.prop2) IS NOT NULL"
          ) {
            failsParsing[Statements].in {
              case Cypher5JavaCc => _.withMessage("Constraint type 'IS NOT NULL' does not allow multiple properties")
              case _ => _.withSyntaxErrorContaining(
                  "Constraint type 'IS NOT NULL' does not allow multiple properties"
                )
            }
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()-[r1:REL]-() $requireOrAssertString (r2.prop) IS NODE KEY") {
            failsParsing[Statements]
              .withMessageStart(ASTExceptionFactory.relationshipPatternNotAllowed(ConstraintType.NODE_KEY))
              .in {
                case Cypher5JavaCc => _.throws[OpenCypherExceptionFactory.SyntaxException]
                case _             => _.throws[SyntaxException]
              }
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()-[r1:REL]-() $requireOrAssertString (r2.prop) IS NODE UNIQUE") {
            failsParsing[Statements]
              .withMessageStart(ASTExceptionFactory.relationshipPatternNotAllowed(ConstraintType.NODE_UNIQUE))
              .in {
                case Cypher5JavaCc => _.throws[OpenCypherExceptionFactory.SyntaxException]
                case _             => _.throws[SyntaxException]
              }
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (r.prop) IS RELATIONSHIP KEY") {
            failsParsing[Statements]
              .withMessageStart(ASTExceptionFactory.nodePatternNotAllowed(ConstraintType.REL_KEY))
              .in {
                case Cypher5JavaCc => _.throws[OpenCypherExceptionFactory.SyntaxException]
                case _             => _.throws[SyntaxException]
              }
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (r.prop) IS REL KEY") {
            failsParsing[Statements]
              .withMessageStart(ASTExceptionFactory.nodePatternNotAllowed(ConstraintType.REL_KEY))
              .in {
                case Cypher5JavaCc => _.throws[OpenCypherExceptionFactory.SyntaxException]
                case _             => _.throws[SyntaxException]
              }
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (r.prop) IS RELATIONSHIP UNIQUE"
          ) {
            failsParsing[Statements]
              .withMessageStart(ASTExceptionFactory.nodePatternNotAllowed(ConstraintType.REL_UNIQUE))
              .in {
                case Cypher5JavaCc => _.throws[OpenCypherExceptionFactory.SyntaxException]
                case _             => _.throws[SyntaxException]
              }
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (r.prop) IS REL UNIQUE") {
            failsParsing[Statements]
              .withMessageStart(ASTExceptionFactory.nodePatternNotAllowed(ConstraintType.REL_UNIQUE))
              .in {
                case Cypher5JavaCc => _.throws[OpenCypherExceptionFactory.SyntaxException]
                case _             => _.throws[SyntaxException]
              }
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS UNIQUENESS"
          ) {
            failsParsing[Statements].withMessageStart("Invalid input 'UNIQUENESS'")
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS NODE UNIQUENESS"
          ) {
            failsParsing[Statements].withMessageStart("Invalid input 'UNIQUENESS'")
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS UNIQUENESS"
          ) {
            failsParsing[Statements].withMessageStart("Invalid input 'UNIQUENESS'")
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS RELATIONSHIP UNIQUENESS"
          ) {
            failsParsing[Statements].withMessageStart("Invalid input 'UNIQUENESS'")
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS REL UNIQUENESS"
          ) {
            failsParsing[Statements].withMessageStart("Invalid input 'UNIQUENESS'")
          }

          // constraint name parameter

          test(s"CREATE CONSTRAINT $$name $forOrOnString (n:L) $requireOrAssertString n.prop IS NODE KEY") {
            parsesTo[Statements](ast.CreateNodeKeyConstraint(
              varFor("n"),
              labelName("L"),
              Seq(prop("n", "prop")),
              Some(Right(stringParam("name"))),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersion
            )(pos))
          }

          test(
            s"CREATE CONSTRAINT $$name $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS RELATIONSHIP KEY"
          ) {
            parsesTo[Statements](ast.CreateRelationshipKeyConstraint(
              varFor("r"),
              relTypeName("R"),
              Seq(prop("r", "prop")),
              Some(Right(stringParam("name"))),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersion
            )(pos))
          }

          test(s"CREATE CONSTRAINT $$name $forOrOnString (n:L) $requireOrAssertString n.prop IS UNIQUE") {
            parsesTo[Statements](ast.CreateNodePropertyUniquenessConstraint(
              varFor("n"),
              labelName("L"),
              Seq(prop("n", "prop")),
              Some(Right(stringParam("name"))),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersion
            )(pos))
          }

          test(s"CREATE CONSTRAINT $$name $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS UNIQUE") {
            parsesTo[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
              varFor("r"),
              relTypeName("R"),
              Seq(prop("r", "prop")),
              Some(Right(stringParam("name"))),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersion
            )(pos))
          }

          test(s"CREATE CONSTRAINT $$name $forOrOnString (n:L) $requireOrAssertString n.prop IS NOT NULL") {
            parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("n"),
              labelName("L"),
              prop("n", "prop"),
              Some(Right(stringParam("name"))),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(s"CREATE CONSTRAINT $$name $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS NOT NULL") {
            parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              Some(Right(stringParam("name"))),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            )(pos))
          }

          test(s"CREATE CONSTRAINT $$name $forOrOnString (n:L) $requireOrAssertString n.prop IS TYPED STRING") {
            parsesTo[Statements](ast.CreateNodePropertyTypeConstraint(
              varFor("n"),
              labelName("L"),
              prop("n", "prop"),
              StringType(isNullable = true)(pos),
              Some(Right(stringParam("name"))),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersion
            )(pos))
          }

          test(s"CREATE CONSTRAINT $$name $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS TYPED STRING") {
            parsesTo[Statements](ast.CreateRelationshipPropertyTypeConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              StringType(isNullable = true)(pos),
              Some(Right(stringParam("name"))),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersion
            )(pos))
          }
        }
    }

  // Property types

  // allowed single types
  private val allowedNonListSingleTypes = Seq(
    ("BOOL", BooleanType(isNullable = true)(pos)),
    ("BOOLEAN", BooleanType(isNullable = true)(pos)),
    ("VARCHAR", StringType(isNullable = true)(pos)),
    ("STRING", StringType(isNullable = true)(pos)),
    ("INTEGER", IntegerType(isNullable = true)(pos)),
    ("INT", IntegerType(isNullable = true)(pos)),
    ("SIGNED INTEGER", IntegerType(isNullable = true)(pos)),
    ("FLOAT", FloatType(isNullable = true)(pos)),
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
    ("POINT", PointType(isNullable = true)(pos))
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

  allowedNonListSingleTypes.foreach { case (typeString, typeExpr: CypherType) =>
    test(s"CREATE CONSTRAINT FOR (n:Label) REQUIRE r.prop IS TYPED $typeString") {
      parsesTo[Statements](ast.CreateNodePropertyTypeConstraint(
        varFor("n"),
        labelName("Label"),
        prop("r", "prop"),
        typeExpr,
        None,
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      )(pos))
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR (n:Label) REQUIRE r.prop IS TYPED ${typeString.toLowerCase}"
    ) {
      parsesTo[Statements](ast.CreateNodePropertyTypeConstraint(
        varFor("n"),
        labelName("Label"),
        prop("r", "prop"),
        typeExpr,
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      )(pos))
    }

    test(s"CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE n.prop IS TYPED ${typeString.toLowerCase}") {
      parsesTo[Statements](ast.CreateRelationshipPropertyTypeConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("n", "prop"),
        typeExpr,
        None,
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      )(pos))
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE n.prop IS TYPED $typeString"
    ) {
      parsesTo[Statements](ast.CreateRelationshipPropertyTypeConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("n", "prop"),
        typeExpr,
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      )(pos))
    }
  }

  disallowedNonListSingleTypes.foreach { case (typeString, typeExpr: CypherType) =>
    test(
      s"CREATE CONSTRAINT my_constraint FOR (n:Label) REQUIRE r.prop IS TYPED ${typeString.toLowerCase}"
    ) {
      parsesTo[Statements](ast.CreateNodePropertyTypeConstraint(
        varFor("n"),
        labelName("Label"),
        prop("r", "prop"),
        typeExpr,
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      )(pos))
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE n.prop IS TYPED $typeString"
    ) {
      parsesTo[Statements](ast.CreateRelationshipPropertyTypeConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("n", "prop"),
        typeExpr,
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      )(pos))
    }
  }

  // Observe list types have buggy positioning in JavaCc
  listSingleTypes.foreach { case (listTypeString, listTypeExpr: CypherType) =>
    test(
      s"CREATE CONSTRAINT my_constraint FOR (n:Label) REQUIRE r.prop IS TYPED ${listTypeString.toLowerCase}"
    ) {
      parses[Statements].toAstIgnorePos(Statements(Seq(ast.CreateNodePropertyTypeConstraint(
        varFor("n"),
        labelName("Label"),
        prop("r", "prop"),
        listTypeExpr,
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      )(pos))))
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE n.prop IS TYPED $listTypeString"
    ) {
      parses[Statements].toAstIgnorePos(Statements(Seq(ast.CreateRelationshipPropertyTypeConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("n", "prop"),
        listTypeExpr,
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      )(pos))))
    }
  }

  unionTypes.foreach { case (unionTypeString, unionTypeExpr: CypherType) =>
    test(
      s"CREATE CONSTRAINT my_constraint FOR (n:Label) REQUIRE r.prop IS TYPED ${unionTypeString.toLowerCase}"
    ) {
      parses[Statements].toAstIgnorePos(Statements(Seq(ast.CreateNodePropertyTypeConstraint(
        varFor("n"),
        labelName("Label"),
        prop("r", "prop"),
        unionTypeExpr,
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      )(pos))))
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE n.prop IS TYPED $unionTypeString"
    ) {
      parses[Statements].toAstIgnorePos(Statements(Seq(ast.CreateRelationshipPropertyTypeConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("n", "prop"),
        unionTypeExpr,
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      )(pos))))
    }
  }

  test("CREATE CONSTRAINT my_constraint FOR (n:L) REQUIRE n.p IS :: ANY<BOOLEAN | STRING> NOT NULL") {
    failsParsing[Statements]
      .withMessageStart(
        "Closed Dynamic Union Types can not be appended with `NOT NULL`, specify `NOT NULL` on all inner types instead."
      )
      .in {
        case Cypher5JavaCc => _.throws[OpenCypherExceptionFactory.SyntaxException]
        case _             => _.throws[SyntaxException]
      }
  }

  test("CREATE CONSTRAINT my_constraint FOR (n:L) REQUIRE n.p IS :: BOOLEAN LIST NOT NULL NOT NULL") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          "Invalid input 'NOT': expected \"ARRAY\", \"LIST\", \"OPTIONS\" or <EOF> (line 1, column 83 (offset: 82))"
        )
      case _ => _.withSyntaxError(
          """Invalid input 'NOT': expected 'ARRAY', 'LIST', 'OPTIONS', '|' or <EOF> (line 1, column 83 (offset: 82))
            |"CREATE CONSTRAINT my_constraint FOR (n:L) REQUIRE n.p IS :: BOOLEAN LIST NOT NULL NOT NULL"
            |                                                                                   ^""".stripMargin
        )
    }
  }

  // ASSERT EXISTS

  test("CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)") {
    parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE CONSTRAINT ON (node1:Label) ASSERT EXISTS node2.prop") {
    assertAst(ast.CreateNodePropertyExistenceConstraint(
      varFor("node1", (1, 23, 22)),
      labelName("Label", (1, 29, 28)),
      Property(varFor("node2", (1, 50, 49)), PropertyKeyName("prop")((1, 56, 55)))((1, 50, 49)),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0,
      None
    )(defaultPos))
  }

  test("CREATE OR REPLACE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)") {
    parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      None,
      ast.IfExistsReplace,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      None,
      ast.IfExistsInvalidSyntax,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      None,
      ast.IfExistsDoNothing,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop) OPTIONS {}") {
    parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      None,
      ast.IfExistsThrowError,
      ast.OptionsMap(Map.empty),
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE CONSTRAINT ON (node1:Label) ASSERT EXISTS (node2.prop1, node3.prop2)") {
    failsParsing[Statements]
      .withMessageStart(ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_EXISTS))
      .in {
        case Cypher5JavaCc => _.throws[Neo4jASTConstructionException]
        case _             => _.throws[SyntaxException]
      }
  }

  test("CREATE CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE CONSTRAINT ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE CONSTRAINT ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE CONSTRAINT ON ()-[r1:R]-() ASSERT EXISTS r2.prop") {
    assertAst(ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r1", (1, 26, 25)),
      relTypeName("R", (1, 29, 28)),
      Property(varFor("r2", (1, 49, 48)), PropertyKeyName("prop")((1, 52, 51)))((1, 49, 48)),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0,
      None
    )(defaultPos))
  }

  test("CREATE OR REPLACE CONSTRAINT ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      None,
      ast.IfExistsReplace,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      None,
      ast.IfExistsInvalidSyntax,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      None,
      ast.IfExistsDoNothing,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE CONSTRAINT ON ()-[r1:REL]-() ASSERT EXISTS (r2.prop1, r3.prop2)") {
    failsParsing[Statements]
      .withMessageStart(ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.REL_EXISTS))
      .in {
        case Cypher5JavaCc => _.throws[Neo4jASTConstructionException]
        case _             => _.throws[SyntaxException]
      }
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop)") {
    parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      Some("my_constraint"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop)") {
    parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      Some("my_constraint"),
      ast.IfExistsReplace,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      Some("my_constraint"),
      ast.IfExistsInvalidSyntax,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      Some("my_constraint"),
      ast.IfExistsDoNothing,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE CONSTRAINT `$my_constraint` ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      Some("$my_constraint"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop) OPTIONS {}") {
    parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      Some("my_constraint"),
      ast.IfExistsThrowError,
      ast.OptionsMap(Map.empty),
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE OR REPLACE CONSTRAINT `$my_constraint` ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      Some("$my_constraint"),
      ast.IfExistsReplace,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE OR REPLACE CONSTRAINT `$my_constraint` IF NOT EXISTS ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      Some("$my_constraint"),
      ast.IfExistsInvalidSyntax,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test("CREATE CONSTRAINT `$my_constraint` IF NOT EXISTS ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      Some("$my_constraint"),
      ast.IfExistsDoNothing,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  test(
    s"CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop1, node.prop2)"
  ) {
    failsParsing[Statements]
      .withMessageStart("Constraint type 'EXISTS' does not allow multiple properties")
      .in {
        case Cypher5JavaCc => _.throws[Neo4jASTConstructionException]
        case _             => _.throws[SyntaxException]
      }
  }

  test(
    s"CREATE CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop1, r.prop2)"
  ) {
    failsParsing[Statements]
      .withMessageStart("Constraint type 'EXISTS' does not allow multiple properties")
      .in {
        case Cypher5JavaCc => _.throws[Neo4jASTConstructionException]
        case _             => _.throws[SyntaxException]
      }
  }

  test("CREATE CONSTRAINT $my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    parsesTo[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      Some(Right(stringParam("my_constraint"))),
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    )(pos))
  }

  // Edge case tests

  test(
    "CREATE CONSTRAINT my_constraint FOR (n:Person) REQUIRE n.prop IS NOT NULL OPTIONS {indexProvider : 'range-1.0'};"
  ) {
    parsesTo[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("n"),
      labelName("Person"),
      prop("n", "prop"),
      Some("my_constraint"),
      ast.IfExistsThrowError,
      ast.OptionsMap(Map("indexProvider" -> literalString("range-1.0"))),
      containsOn = false,
      constraintVersion = ast.ConstraintVersion2
    )(pos))
  }

  test(
    "CREATE CONSTRAINT FOR (n:Person) REQUIRE n.prop IS NOT NULL; CREATE CONSTRAINT FOR (n:User) REQUIRE n.prop IS UNIQUE"
  ) {
    // The test setup does 'fromParser(_.Statements().get(0)', so only the first statement is yielded.
    // The purpose of the test is to make sure the parser does not throw an error on the semicolon, which was an issue before.
    // If we want to test that both statements are parsed, the test framework needs to be extended.
    parses[Statements].withAstLike { statements =>
      statements.get(0) shouldBe ast.CreateNodePropertyExistenceConstraint(
        varFor("n"),
        labelName("Person"),
        prop("n", "prop"),
        None,
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        constraintVersion = ast.ConstraintVersion2
      )(pos)
    }
  }

  test("CREATE CONSTRAINT FOR FOR (node:Label) REQUIRE (node.prop) IS NODE KEY") {
    assertAst(ast.CreateNodeKeyConstraint(
      varFor("node", (1, 28, 27)),
      labelName("Label", (1, 33, 32)),
      Seq(prop("node", "prop", (1, 49, 48))),
      Some("FOR"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = false,
      ast.ConstraintVersion2
    )(defaultPos))
  }

  test("CREATE CONSTRAINT FOR FOR (node:Label) REQUIRE (node.prop) IS UNIQUE") {
    assertAst(ast.CreateNodePropertyUniquenessConstraint(
      varFor("node", (1, 28, 27)),
      labelName("Label", (1, 33, 32)),
      Seq(prop("node", "prop", (1, 49, 48))),
      Some("FOR"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = false,
      ast.ConstraintVersion2
    )(defaultPos))
  }

  test("CREATE CONSTRAINT FOR FOR (node:Label) REQUIRE node.prop IS NOT NULL") {
    assertAst(ast.CreateNodePropertyExistenceConstraint(
      varFor("node", (1, 28, 27)),
      labelName("Label", (1, 33, 32)),
      prop("node", "prop", (1, 48, 47)),
      Some("FOR"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = false,
      ast.ConstraintVersion2
    )(defaultPos))
  }

  test("CREATE CONSTRAINT FOR FOR ()-[r:R]-() REQUIRE (r.prop) IS NOT NULL") {
    assertAst(ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r", (1, 31, 30)),
      relTypeName("R", (1, 33, 32)),
      prop("r", "prop", (1, 48, 47)),
      Some("FOR"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = false,
      ast.ConstraintVersion2
    )(defaultPos))
  }

  // Negative tests

  test("CREATE CONSTRAINT FOR (:A)-[n1:R]-() REQUIRE (n2.name) IS RELATIONSHIP KEY") {
    // label on node
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input ':': expected ")" or an identifier""")
      case _ => _.withSyntaxError(
          """Invalid input ':': expected a variable name or ')' (line 1, column 24 (offset: 23))
            |"CREATE CONSTRAINT FOR (:A)-[n1:R]-() REQUIRE (n2.name) IS RELATIONSHIP KEY"
            |                        ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR ()-[n1:R]-(:A) REQUIRE (n2.name) IS UNIQUE") {
    // label on node
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input ':': expected ")"""")
      case _ => _.withSyntaxError(
          """Invalid input ':': expected ')' (line 1, column 34 (offset: 33))
            |"CREATE CONSTRAINT FOR ()-[n1:R]-(:A) REQUIRE (n2.name) IS UNIQUE"
            |                                  ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR (n2)-[n1:R]-() REQUIRE (n2.name) IS NOT NULL") {
    // variable on node
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input ')': expected ":"""")
      case _ => _.withSyntaxError(
          """Invalid input ')': expected ':' (line 1, column 26 (offset: 25))
            |"CREATE CONSTRAINT FOR (n2)-[n1:R]-() REQUIRE (n2.name) IS NOT NULL"
            |                          ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR ()-[n1:R]-(n2) REQUIRE (n2.name) IS :: STRING") {
    // variable on node
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'n2': expected ")"""")
      case _ => _.withSyntaxError(
          """Invalid input 'n2': expected ')' (line 1, column 34 (offset: 33))
            |"CREATE CONSTRAINT FOR ()-[n1:R]-(n2) REQUIRE (n2.name) IS :: STRING"
            |                                  ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR (n2:A)-[n1:R]-() REQUIRE (n2.name) IS KEY") {
    // variable on node
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input '-': expected "ASSERT" or "REQUIRE"""")
      case _ => _.withSyntaxError(
          """Invalid input '-': expected 'ASSERT' or 'REQUIRE' (line 1, column 29 (offset: 28))
            |"CREATE CONSTRAINT FOR (n2:A)-[n1:R]-() REQUIRE (n2.name) IS KEY"
            |                             ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR ()-[n1:R]-(n2:A) REQUIRE (n2.name) IS RELATIONSHIP UNIQUE") {
    // variable on node
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'n2': expected ")"""")
      case _ => _.withSyntaxError(
          """Invalid input 'n2': expected ')' (line 1, column 34 (offset: 33))
            |"CREATE CONSTRAINT FOR ()-[n1:R]-(n2:A) REQUIRE (n2.name) IS RELATIONSHIP UNIQUE"
            |                                  ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop) IS NOT NULL") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'IS': expected "OPTIONS" or <EOF> (line 1, column 75 (offset: 74))""")
      case _ => _.withSyntaxError(
          """Invalid input 'IS': expected 'OPTIONS' or <EOF> (line 1, column 75 (offset: 74))
            |"CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop) IS NOT NULL"
            |                                                                           ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop) IS NOT NULL") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'IS': expected \"OPTIONS\" or <EOF> (line 1, column 71 (offset: 70))")
      case _ => _.withSyntaxError(
          """Invalid input 'IS': expected 'OPTIONS' or <EOF> (line 1, column 71 (offset: 70))
            |"CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop) IS NOT NULL"
            |                                                                       ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR (n:Label) REQUIRE (n.prop)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input '': expected \"::\" or \"IS\" (line 1, column 49 (offset: 48))")
      case _ => _.withSyntaxError(
          """Invalid input '': expected '::' or 'IS' (line 1, column 49 (offset: 48))
            |"CREATE CONSTRAINT FOR (n:Label) REQUIRE (n.prop)"
            |                                                 ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR (node:Label) REQUIRE EXISTS (node.prop)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input '(': expected \".\" (line 1, column 51 (offset: 50))")
      case _ => _.withSyntaxError(
          """Invalid input '(': expected '.' (line 1, column 51 (offset: 50))
            |"CREATE CONSTRAINT FOR (node:Label) REQUIRE EXISTS (node.prop)"
            |                                                   ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE EXISTS (r.prop)") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input '(': expected \".\" (line 1, column 50 (offset: 49))")
      case _ => _.withSyntaxError(
          """Invalid input '(': expected '.' (line 1, column 50 (offset: 49))
            |"CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE EXISTS (r.prop)"
            |                                                  ^""".stripMargin
        )
    }
  }

  test(s"CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT r.prop IS NULL") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'NULL': expected
            |  "::"
            |  "KEY"
            |  "NODE"
            |  "NOT"
            |  "REL"
            |  "RELATIONSHIP"
            |  "TYPED"
            |  "UNIQUE" (line 1, column 65 (offset: 64))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'NULL': expected '::', 'KEY', 'NODE', 'NOT NULL', 'REL', 'RELATIONSHIP', 'TYPED' or 'UNIQUE' (line 1, column 65 (offset: 64))
            |"CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT r.prop IS NULL"
            |                                                                 ^""".stripMargin
        )
    }
  }

  test(s"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE r.prop IS NULL") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'NULL': expected
            |  "::"
            |  "KEY"
            |  "NODE"
            |  "NOT"
            |  "REL"
            |  "RELATIONSHIP"
            |  "TYPED"
            |  "UNIQUE" (line 1, column 67 (offset: 66))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'NULL': expected '::', 'KEY', 'NODE', 'NOT NULL', 'REL', 'RELATIONSHIP', 'TYPED' or 'UNIQUE' (line 1, column 67 (offset: 66))
            |"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE r.prop IS NULL"
            |                                                                   ^""".stripMargin
        )
    }
  }

  test(s"CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT node.prop IS NULL") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'NULL': expected
            |  "::"
            |  "KEY"
            |  "NODE"
            |  "NOT"
            |  "REL"
            |  "RELATIONSHIP"
            |  "TYPED"
            |  "UNIQUE" (line 1, column 69 (offset: 68))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'NULL': expected '::', 'KEY', 'NODE', 'NOT NULL', 'REL', 'RELATIONSHIP', 'TYPED' or 'UNIQUE' (line 1, column 69 (offset: 68))
            |"CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT node.prop IS NULL"
            |                                                                     ^""".stripMargin
        )
    }
  }

  test(s"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE node.prop IS NULL") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'NULL': expected
                                                 |  "::"
                                                 |  "KEY"
                                                 |  "NODE"
                                                 |  "NOT"
                                                 |  "REL"
                                                 |  "RELATIONSHIP"
                                                 |  "TYPED"
                                                 |  "UNIQUE" (line 1, column 71 (offset: 70))""".stripMargin)
      case _ => _.withSyntaxError(
          """Invalid input 'NULL': expected '::', 'KEY', 'NODE', 'NOT NULL', 'REL', 'RELATIONSHIP', 'TYPED' or 'UNIQUE' (line 1, column 71 (offset: 70))
            |"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE node.prop IS NULL"
            |                                                                       ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS TYPED") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input '': expected
            |  "ANY"
            |  "ARRAY"
            |  "BOOL"
            |  "BOOLEAN"
            |  "DATE"
            |  "DURATION"
            |  "EDGE"
            |  "FLOAT"
            |  "INT"
            |  "INTEGER"
            |  "LIST"
            |  "LOCAL"
            |  "MAP"
            |  "NODE"
            |  "NOTHING"
            |  "PATH"
            |  "POINT"
            |  "PROPERTY"
            |  "RELATIONSHIP"
            |  "SIGNED"
            |  "STRING"
            |  "TIME"
            |  "TIMESTAMP"
            |  "VARCHAR"
            |  "VERTEX"
            |  "ZONED"
            |  "null"""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'ARRAY', 'LIST', 'ANY', 'BOOL', 'BOOLEAN', 'DATE', 'DURATION', 'EDGE', 'FLOAT', 'INT', 'INTEGER', 'LOCAL', 'MAP', 'NODE', 'NOTHING', 'NULL', 'PATH', 'PATHS', 'POINT', 'RELATIONSHIP', 'SIGNED', 'STRING', 'TIME', 'TIMESTAMP', 'PROPERTY VALUE', 'VARCHAR', 'VERTEX' or 'ZONED' (line 1, column 49 (offset: 48))
            |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS TYPED"
            |                                                 ^""".stripMargin
        )
    }

  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS ::") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input '': expected
            |  "ANY"
            |  "ARRAY"
            |  "BOOL"
            |  "BOOLEAN"
            |  "DATE"
            |  "DURATION"
            |  "EDGE"
            |  "FLOAT"
            |  "INT"
            |  "INTEGER"
            |  "LIST"
            |  "LOCAL"
            |  "MAP"
            |  "NODE"
            |  "NOTHING"
            |  "PATH"
            |  "POINT"
            |  "PROPERTY"
            |  "RELATIONSHIP"
            |  "SIGNED"
            |  "STRING"
            |  "TIME"
            |  "TIMESTAMP"
            |  "VARCHAR"
            |  "VERTEX"
            |  "ZONED"
            |  "null"""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'ARRAY', 'LIST', 'ANY', 'BOOL', 'BOOLEAN', 'DATE', 'DURATION', 'EDGE', 'FLOAT', 'INT', 'INTEGER', 'LOCAL', 'MAP', 'NODE', 'NOTHING', 'NULL', 'PATH', 'PATHS', 'POINT', 'RELATIONSHIP', 'SIGNED', 'STRING', 'TIME', 'TIMESTAMP', 'PROPERTY VALUE', 'VARCHAR', 'VERTEX' or 'ZONED' (line 1, column 46 (offset: 45))
            |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS ::"
            |                                              ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p ::") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input '': expected
            |  "ANY"
            |  "ARRAY"
            |  "BOOL"
            |  "BOOLEAN"
            |  "DATE"
            |  "DURATION"
            |  "EDGE"
            |  "FLOAT"
            |  "INT"
            |  "INTEGER"
            |  "LIST"
            |  "LOCAL"
            |  "MAP"
            |  "NODE"
            |  "NOTHING"
            |  "PATH"
            |  "POINT"
            |  "PROPERTY"
            |  "RELATIONSHIP"
            |  "SIGNED"
            |  "STRING"
            |  "TIME"
            |  "TIMESTAMP"
            |  "VARCHAR"
            |  "VERTEX"
            |  "ZONED"
            |  "null"""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected 'ARRAY', 'LIST', 'ANY', 'BOOL', 'BOOLEAN', 'DATE', 'DURATION', 'EDGE', 'FLOAT', 'INT', 'INTEGER', 'LOCAL', 'MAP', 'NODE', 'NOTHING', 'NULL', 'PATH', 'PATHS', 'POINT', 'RELATIONSHIP', 'SIGNED', 'STRING', 'TIME', 'TIMESTAMP', 'PROPERTY VALUE', 'VARCHAR', 'VERTEX' or 'ZONED' (line 1, column 43 (offset: 42))
            |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p ::"
            |                                           ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p :: TYPED") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'TYPED': expected
            |  "ANY"
            |  "ARRAY"
            |  "BOOL"
            |  "BOOLEAN"
            |  "DATE"
            |  "DURATION"
            |  "EDGE"
            |  "FLOAT"
            |  "INT"
            |  "INTEGER"
            |  "LIST"
            |  "LOCAL"
            |  "MAP"
            |  "NODE"
            |  "NOTHING"
            |  "PATH"
            |  "POINT"
            |  "PROPERTY"
            |  "RELATIONSHIP"
            |  "SIGNED"
            |  "STRING"
            |  "TIME"
            |  "TIMESTAMP"
            |  "VARCHAR"
            |  "VERTEX"
            |  "ZONED"
            |  "null" (line 1, column 44 (offset: 43))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'TYPED': expected 'ARRAY', 'LIST', 'ANY', 'BOOL', 'BOOLEAN', 'DATE', 'DURATION', 'EDGE', 'FLOAT', 'INT', 'INTEGER', 'LOCAL', 'MAP', 'NODE', 'NOTHING', 'NULL', 'PATH', 'PATHS', 'POINT', 'RELATIONSHIP', 'SIGNED', 'STRING', 'TIME', 'TIMESTAMP', 'PROPERTY VALUE', 'VARCHAR', 'VERTEX' or 'ZONED' (line 1, column 44 (offset: 43))
            |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p :: TYPED"
            |                                            ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p :: UNIQUE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'UNIQUE': expected
            |  "ANY"
            |  "ARRAY"
            |  "BOOL"
            |  "BOOLEAN"
            |  "DATE"
            |  "DURATION"
            |  "EDGE"
            |  "FLOAT"
            |  "INT"
            |  "INTEGER"
            |  "LIST"
            |  "LOCAL"
            |  "MAP"
            |  "NODE"
            |  "NOTHING"
            |  "PATH"
            |  "POINT"
            |  "PROPERTY"
            |  "RELATIONSHIP"
            |  "SIGNED"
            |  "STRING"
            |  "TIME"
            |  "TIMESTAMP"
            |  "VARCHAR"
            |  "VERTEX"
            |  "ZONED"
            |  "null" (line 1, column 44 (offset: 43))""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'UNIQUE': expected 'ARRAY', 'LIST', 'ANY', 'BOOL', 'BOOLEAN', 'DATE', 'DURATION', 'EDGE', 'FLOAT', 'INT', 'INTEGER', 'LOCAL', 'MAP', 'NODE', 'NOTHING', 'NULL', 'PATH', 'PATHS', 'POINT', 'RELATIONSHIP', 'SIGNED', 'STRING', 'TIME', 'TIMESTAMP', 'PROPERTY VALUE', 'VARCHAR', 'VERTEX' or 'ZONED' (line 1, column 44 (offset: 43))
            |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p :: UNIQUE"
            |                                            ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p :: BOOLEAN UNIQUE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'UNIQUE': expected
            |  "!"
            |  "ARRAY"
            |  "LIST"
            |  "NOT"
            |  "OPTIONS"
            |  <EOF>""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'UNIQUE': expected '!', 'ARRAY', 'LIST', 'NOT NULL', 'OPTIONS', '|' or <EOF> (line 1, column 52 (offset: 51))
            |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p :: BOOLEAN UNIQUE"
            |                                                    ^""".stripMargin
        )
    }
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: BOOL EAN") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart(
          """Invalid input 'EAN': expected
            |  "!"
            |  "ARRAY"
            |  "LIST"
            |  "NOT"
            |  "OPTIONS"
            |  <EOF>""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input 'EAN': expected '!', 'ARRAY', 'LIST', 'NOT NULL', 'OPTIONS', '|' or <EOF> (line 1, column 52 (offset: 51))
            |"CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: BOOL EAN"
            |                                                    ^""".stripMargin
        )
    }
  }

  // Drop constraint by schema (throws either in parsing, ast generation or semantic checking)

  //   Throws in semantic checking

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    parsesTo[Statements](ast.DropNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")))(pos))
  }

  test("DROP CONSTRAINT ON (node1:Label) ASSERT node2.prop IS NODE KEY") {
    assertAst(
      ast.DropNodeKeyConstraint(
        varFor("node1", (1, 21, 20)),
        labelName("Label", (1, 27, 26)),
        Seq(Property(varFor("node2", (1, 41, 40)), PropertyKeyName("prop")((1, 47, 46)))((1, 42, 40))),
        None
      )(defaultPos)
    )
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    parsesTo[Statements](ast.DropNodeKeyConstraint(
      varFor("node"),
      labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2"))
    )(pos))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE") {
    parsesTo[Statements](ast.DropPropertyUniquenessConstraint(
      varFor("node"),
      labelName("Label"),
      Seq(prop("node", "prop"))
    )(pos))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop) IS UNIQUE") {
    parsesTo[Statements](ast.DropPropertyUniquenessConstraint(
      varFor("node"),
      labelName("Label"),
      Seq(prop("node", "prop"))
    )(pos))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE") {
    parsesTo[Statements](ast.DropPropertyUniquenessConstraint(
      varFor("node"),
      labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2"))
    )(pos))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)") {
    parsesTo[Statements](ast.DropNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop")
    )(pos))
  }

  test("DROP CONSTRAINT ON (node1:Label) ASSERT EXISTS node2.prop") {
    assertAst(
      ast.DropNodePropertyExistenceConstraint(
        varFor("node1", (1, 21, 20)),
        labelName("Label", (1, 27, 26)),
        Property(varFor("node2", (1, 48, 47)), PropertyKeyName("prop")((1, 54, 53)))((1, 48, 47)),
        None
      )(defaultPos)
    )
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    parsesTo[Statements](ast.DropRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop")
    )(pos))
  }

  test("DROP CONSTRAINT ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    parsesTo[Statements](ast.DropRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop")
    )(pos))
  }

  test("DROP CONSTRAINT ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    parsesTo[Statements](ast.DropRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop")
    )(pos))
  }

  test("DROP CONSTRAINT ON ()-[r1:R]-() ASSERT EXISTS r2.prop") {
    assertAst(
      ast.DropRelationshipPropertyExistenceConstraint(
        varFor("r1", (1, 24, 23)),
        relTypeName("R", (1, 27, 26)),
        Property(varFor("r2", (1, 47, 46)), PropertyKeyName("prop")((1, 50, 49)))((1, 47, 46)),
        None
      )(defaultPos)
    )
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.EXISTS) IS NODE KEY") {
    parsesTo[Statements](
      ast.DropNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "EXISTS")))(pos)
    )
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.EXISTS) IS UNIQUE") {
    parsesTo[Statements](ast.DropPropertyUniquenessConstraint(
      varFor("node"),
      labelName("Label"),
      Seq(prop("node", "EXISTS"))
    )(pos))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT EXISTS (node.EXISTS)") {
    parsesTo[Statements](ast.DropNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "EXISTS")
    )(pos))
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.EXISTS)") {
    parsesTo[Statements](ast.DropRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "EXISTS")
    )(pos))
  }

  //   Throws in ast generation/parsing

  test("DROP CONSTRAINT ON ()-[r1:R]-() ASSERT r2.prop IS NODE KEY") {
    failsParsing[Statements]
      .withMessageStart(ASTExceptionFactory.relationshipPatternNotAllowed(ConstraintType.NODE_KEY))
      .in {
        case Cypher5JavaCc => _.throws[OpenCypherExceptionFactory.SyntaxException]
        case _             => _.throws[SyntaxException]
      }
  }

  test("DROP CONSTRAINT ON ()-[r1:R]-() ASSERT r2.prop IS UNIQUE") {
    failsParsing[Statements]
      .withMessageStart(ASTExceptionFactory.relationshipPatternNotAllowed(ConstraintType.NODE_UNIQUE))
      .in {
        case Cypher5JavaCc => _.throws[OpenCypherExceptionFactory.SyntaxException]
        case _             => _.throws[SyntaxException]
      }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT EXISTS (n.p1, n.p2)") {
    failsParsing[Statements]
      .withMessageStart(ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_EXISTS))
      .in {
        case Cypher5JavaCc => _.throws[Neo4jASTConstructionException]
        case _             => _.throws[SyntaxException]
      }
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.p1, r.p2)") {
    failsParsing[Statements]
      .withMessageStart(ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.REL_EXISTS))
      .in {
        case Cypher5JavaCc => _.throws[Neo4jASTConstructionException]
        case _             => _.throws[SyntaxException]
      }
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NOT NULL") {
    failsParsing[Statements]
      .withMessageStart(ASTExceptionFactory.invalidDropCommand)
      .in {
        case Cypher5JavaCc => _.throws[Neo4jASTConstructionException]
        case _             => _.throws[SyntaxException]
      }
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS NOT NULL") {
    failsParsing[Statements]
      .withMessageStart(ASTExceptionFactory.invalidDropCommand)
      .in {
        case Cypher5JavaCc => _.throws[Neo4jASTConstructionException]
        case _             => _.throws[SyntaxException]
      }

  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT (r.prop) IS NOT NULL") {
    failsParsing[Statements]
      .withMessageStart(ASTExceptionFactory.invalidDropCommand)
      .in {
        case Cypher5JavaCc => _.throws[Neo4jASTConstructionException]
        case _             => _.throws[SyntaxException]
      }

  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.prop IS NOT NULL") {
    failsParsing[Statements]
      .withMessageStart(ASTExceptionFactory.invalidDropCommand)
      .in {
        case Cypher5JavaCc => _.throws[Neo4jASTConstructionException]
        case _             => _.throws[SyntaxException]
      }

  }

  test("DROP CONSTRAINT ON (n:L) ASSERT (n.p1, n.p2) IS NOT NULL") {
    failsParsing[Statements]
      .withMessageStart(ASTExceptionFactory.invalidDropCommand)
      .in {
        case Cypher5JavaCc => _.throws[Neo4jASTConstructionException]
        case _             => _.throws[SyntaxException]
      }

  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT (r.p1, r.p2) IS NOT NULL") {
    failsParsing[Statements]
      .withMessageStart(ASTExceptionFactory.invalidDropCommand)
      .in {
        case Cypher5JavaCc => _.throws[Neo4jASTConstructionException]
        case _             => _.throws[SyntaxException]
      }

  }

  test("DROP CONSTRAINT FOR (n:L) REQUIRE n.p IS NODE KEY") {
    // Parses FOR as constraint name
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '(': expected "IF" or <EOF> (line""")
      case _ =>
        _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT FOR (n:L) ASSERT n.p IS NODE KEY") {
    // Parses FOR as constraint name
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '(': expected "IF" or <EOF> (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (n:L) REQUIRE n.p IS NODE KEY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'REQUIRE': expected "ASSERT" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'REQUIRE': expected 'ASSERT' (line")
    }
  }

  test("DROP CONSTRAINT FOR (n:L) REQUIRE n.p IS UNIQUE") {
    // Parses FOR as constraint name
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '(': expected "IF" or <EOF> (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT FOR (n:L) ASSERT n.p IS UNIQUE") {
    // Parses FOR as constraint name
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '(': expected "IF" or <EOF> (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (n:L) REQUIRE n.p IS UNIQUE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'REQUIRE': expected "ASSERT" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'REQUIRE': expected 'ASSERT' (line")
    }
  }

  test("DROP CONSTRAINT FOR (n:L) REQUIRE EXISTS n.p") {
    // Parses FOR as constraint name
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '(': expected "IF" or <EOF> (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT FOR (n:L) ASSERT EXISTS n.p") {
    // Parses FOR as constraint name
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '(': expected "IF" or <EOF> (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input '(': expected 'IF EXISTS' or <EOF> (line")
    }
  }

  test("DROP CONSTRAINT ON (n:L) REQUIRE EXISTS n.p") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'REQUIRE': expected "ASSERT" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'REQUIRE': expected 'ASSERT' (line")
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT n.p IS REL KEY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'REL': expected "NODE", "NOT" or "UNIQUE" (line""")
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'REL': expected 'NODE KEY', 'NOT NULL' or 'UNIQUE' (line"
        )
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT n.p IS RELATIONSHIP KEY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'RELATIONSHIP': expected "NODE", "NOT" or "UNIQUE" (line""")
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'RELATIONSHIP': expected 'NODE KEY', 'NOT NULL' or 'UNIQUE' (line"
        )
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT n.p IS REL UNIQUE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'REL': expected "NODE", "NOT" or "UNIQUE" (line""")
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'REL': expected 'NODE KEY', 'NOT NULL' or 'UNIQUE' (line"
        )
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT n.p IS RELATIONSHIP UNIQUE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("""Invalid input 'RELATIONSHIP': expected "NODE", "NOT" or "UNIQUE" (line""")
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'RELATIONSHIP': expected 'NODE KEY', 'NOT NULL' or 'UNIQUE' (line"
        )
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT n.p IS NODE UNIQUE") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'UNIQUE': expected "KEY" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input 'UNIQUE': expected 'KEY' (line")
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT n.p IS TYPED INT") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'TYPED': expected "NODE", "NOT" or "UNIQUE" (line""")
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'TYPED': expected 'NODE KEY', 'NOT NULL' or 'UNIQUE' (line"
        )
    }
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.p IS TYPED STRING") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input 'TYPED': expected "NODE", "NOT" or "UNIQUE" (line""")
      case _ => _.withSyntaxErrorContaining(
          "Invalid input 'TYPED': expected 'NODE KEY', 'NOT NULL' or 'UNIQUE' (line"
        )
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT n.p IS :: LIST<FLOAT>") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '::': expected "NODE", "NOT" or "UNIQUE" (line""")
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '::': expected 'NODE KEY', 'NOT NULL' or 'UNIQUE' (line"
        )
    }
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.p IS :: BOOLEAN") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '::': expected "NODE", "NOT" or "UNIQUE" (line""")
      case _ => _.withSyntaxErrorContaining(
          "Invalid input '::': expected 'NODE KEY', 'NOT NULL' or 'UNIQUE' (line"
        )
    }
  }

  test("DROP CONSTRAINT ON (n:L) ASSERT n.p :: ZONED DATETIME") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '::': expected "IS" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input '::': expected 'IS' (line")
    }
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.p :: LOCAL TIME") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("""Invalid input '::': expected "IS" (line""")
      case _             => _.withSyntaxErrorContaining("Invalid input '::': expected 'IS' (line")
    }
  }

  // Drop constraint by name

  test("DROP CONSTRAINT my_constraint") {
    parsesTo[Statements](ast.DropConstraintOnName("my_constraint", ifExists = false)(pos))
  }

  test("DROP CONSTRAINT `$my_constraint`") {
    parsesTo[Statements](ast.DropConstraintOnName("$my_constraint", ifExists = false)(pos))
  }

  test("DROP CONSTRAINT my_constraint IF EXISTS") {
    parsesTo[Statements](ast.DropConstraintOnName("my_constraint", ifExists = true)(pos))
  }

  test("DROP CONSTRAINT $my_constraint") {
    parsesTo[Statements](ast.DropConstraintOnName(Right(stringParam("my_constraint")), ifExists = false)(pos))
  }

  test("DROP CONSTRAINT my_constraint IF EXISTS;") {
    parsesTo[Statements](ast.DropConstraintOnName("my_constraint", ifExists = true)(pos))
  }

  test("DROP CONSTRAINT my_constraint; DROP CONSTRAINT my_constraint2;") {
    // The test setup does 'fromParser(_.Statements().get(0)', so only the first statement is yielded.
    // The purpose of the test is to make sure the parser does not throw an error on the semicolon, which was an issue before.
    // If we want to test that both statements are parsed, the test framework needs to be extended.
    parses[Statements].withAstLike { statements =>
      statements.get(0) shouldBe ast.DropConstraintOnName("my_constraint", ifExists = false)(pos)
    }
  }

  test("DROP CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    failsParsing[Statements].in {
      case Cypher5JavaCc =>
        _.withMessageStart("Invalid input 'ON': expected \"IF\" or <EOF> (line 1, column 31 (offset: 30))")
      case _ => _.withSyntaxError(
          """Invalid input 'ON': expected 'IF EXISTS' or <EOF> (line 1, column 31 (offset: 30))
            |"DROP CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY"
            |                               ^""".stripMargin
        )
    }
  }
}
