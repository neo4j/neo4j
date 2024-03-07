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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.ast.factory.ConstraintType
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.ParserSupport.NotAnyAntlr
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
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
              yields[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS $nodeKeyword KEY"
            ) {
              yields[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY"
            ) {
              yields[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY"
            ) {
              yields[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword KEY"
            ) {
              yields[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword KEY"
            ) {
              yields[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                None,
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY OPTIONS {indexProvider : 'range-1.0'}"
            ) {
              yields[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexProvider" -> literalString("range-1.0"))),
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}"
            ) {
              // will fail in options converter
              yields[Statements](ast.CreateNodeKeyConstraint(
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
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY OPTIONS {indexConfig : {someConfig: 'toShowItCanBeParsed' }}"
            ) {
              yields[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexConfig" -> mapOf("someConfig" -> literalString("toShowItCanBeParsed")))),
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY OPTIONS {nonValidOption : 42}"
            ) {
              yields[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("nonValidOption" -> literalInt(42))),
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY OPTIONS {}"
            ) {
              yields[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map.empty),
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY OPTIONS $$param"
            ) {
              yields[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsParam(parameter("param", CTMap)),
                containsOn,
                constraintVersion
              ))
            }

            // Node uniqueness

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS $nodeKeyword UNIQUE"
            ) {
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS $nodeKeyword UNIQUE"
            ) {
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop IS $nodeKeyword UNIQUE"
            ) {
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE"
            ) {
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword UNIQUE"
            ) {
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword UNIQUE"
            ) {
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                None,
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}, indexProvider : 'native-btree-1.0'}"
            ) {
              // will fail in options converter
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
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
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE OPTIONS $$options"
            ) {
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsParam(parameter("options", CTMap)),
                containsOn,
                constraintVersion
              ))
            }
          })

          Seq("RELATIONSHIP", "REL", "").foreach(relKeyWord => {
            // Relationship key

            test(s"CREATE CONSTRAINT $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY") {
              yields[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()-[r1:R]->() $requireOrAssertString (r2.prop1, r3.prop2) IS $relKeyWord KEY"
            ) {
              yields[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop1"), prop("r3", "prop2")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()<-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              yields[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()<-[r1:R]->() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              yields[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              yields[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              yields[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              yields[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY OPTIONS {key: 'value'}"
            ) {
              yields[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("key" -> literalString("value"))),
                containsOn,
                constraintVersion
              ))
            }

            // Relationship uniqueness

            test(
              s"CREATE CONSTRAINT $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()-[r1:R]->() $requireOrAssertString (r2.prop1, r3.prop2) IS $relKeyWord UNIQUE"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop1"), prop("r3", "prop2")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()<-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()<-[r1:R]->() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE OPTIONS {key: 'value'}"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("key" -> literalString("value"))),
                containsOn,
                constraintVersion
              ))
            }
          })

          // Node property existence

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL") {
            yields[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              None,
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NOT NULL") {
            yields[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              None,
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL"
          ) {
            yields[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              None,
              ast.IfExistsReplace,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL"
          ) {
            yields[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              None,
              ast.IfExistsInvalidSyntax,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(
            s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NOT NULL"
          ) {
            yields[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              None,
              ast.IfExistsDoNothing,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          // Relationship property existence

          test(s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS NOT NULL") {
            yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              None,
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()-[r:R]->() $requireOrAssertString r.prop IS NOT NULL") {
            yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              None,
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()<-[r:R]-() $requireOrAssertString (r.prop) IS NOT NULL") {
            yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              None,
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()<-[r:R]->() $requireOrAssertString (r.prop) IS NOT NULL") {
            yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              None,
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(s"CREATE OR REPLACE CONSTRAINT $forOrOnString ()<-[r:R]-() $requireOrAssertString r.prop IS NOT NULL") {
            yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              None,
              ast.IfExistsReplace,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS NOT NULL"
          ) {
            yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              None,
              ast.IfExistsInvalidSyntax,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(
            s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r:R]->() $requireOrAssertString r.prop IS NOT NULL"
          ) {
            yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              None,
              ast.IfExistsDoNothing,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString (r.prop) IS NOT NULL OPTIONS {}") {
            yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              None,
              ast.IfExistsThrowError,
              ast.OptionsMap(Map.empty),
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          Seq("IS TYPED", "IS ::", "::").foreach(typeKeyword => {
            // Node property type

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop $typeKeyword BOOLEAN"
            ) {
              yields[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) $typeKeyword BOOLEAN"
            ) {
              yields[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop $typeKeyword BOOLEAN"
            ) {
              yields[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop $typeKeyword BOOLEAN"
            ) {
              yields[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop) $typeKeyword BOOLEAN"
            ) {
              yields[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            // Relationship property type

            test(s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop $typeKeyword BOOLEAN") {
              yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(s"CREATE CONSTRAINT $forOrOnString ()-[r:R]->() $requireOrAssertString r.prop $typeKeyword BOOLEAN") {
              yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()<-[r:R]-() $requireOrAssertString (r.prop) $typeKeyword BOOLEAN"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()<-[r:R]->() $requireOrAssertString (r.prop) $typeKeyword BOOLEAN"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT $forOrOnString ()<-[r:R]-() $requireOrAssertString r.prop $typeKeyword BOOLEAN"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop $typeKeyword BOOLEAN"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r:R]->() $requireOrAssertString r.prop $typeKeyword BOOLEAN"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString (r.prop) $typeKeyword BOOLEAN OPTIONS {}"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                BooleanType(isNullable = true)(pos),
                None,
                ast.IfExistsThrowError,
                ast.OptionsMap(Map.empty),
                containsOn,
                constraintVersion
              ))
            }
          })

          // Create constraint: With name

          Seq("NODE", "").foreach(nodeKeyword => {
            // Node key
            test(
              s"USE neo4j CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY"
            ) {
              yields[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion,
                Some(use(List("neo4j")))
              ))
            }

            test(
              s"USE neo4j CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY"
            ) {
              yields[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                Some("my_constraint"),
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion,
                Some(use(List("neo4j")))
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword KEY"
            ) {
              yields[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword KEY"
            ) {
              yields[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                Some("my_constraint"),
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword KEY"
            ) {
              yields[Statements](ast.CreateNodeKeyConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                Some("my_constraint"),
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword KEY OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}}"
            ) {
              // will fail in options converter
              yields[Statements](ast.CreateNodeKeyConstraint(
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
              ))
            }

            // Node uniqueness

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString node.prop IS $nodeKeyword UNIQUE"
            ) {
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE"
            ) {
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE"
            ) {
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                Some("my_constraint"),
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword UNIQUE"
            ) {
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword UNIQUE"
            ) {
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                Some("my_constraint"),
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword UNIQUE"
            ) {
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                Some("my_constraint"),
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE OPTIONS {indexProvider : 'range-1.0'}"
            ) {
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexProvider" -> literalString("range-1.0"))),
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}"
            ) {
              // will fail in options converter
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
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
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE OPTIONS {indexConfig : {someConfig: 'toShowItCanBeParsed' }}"
            ) {
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("indexConfig" -> mapOf("someConfig" -> literalString("toShowItCanBeParsed")))),
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS $nodeKeyword UNIQUE OPTIONS {nonValidOption : 42}"
            ) {
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("nonValidOption" -> literalInt(42))),
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS $nodeKeyword UNIQUE OPTIONS {}"
            ) {
              yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
                varFor("node"),
                labelName("Label"),
                Seq(prop("node", "prop1"), prop("node", "prop2")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.OptionsMap(Map.empty),
                containsOn,
                constraintVersion
              ))
            }
          })

          Seq("RELATIONSHIP", "REL", "").foreach(relKeyWord => {
            // Relationship key

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              yields[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()-[r1:R]->() $requireOrAssertString (r2.prop1, r3.prop2) IS $relKeyWord KEY"
            ) {
              yields[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop1"), prop("r3", "prop2")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()<-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              yields[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()<-[r1:R]->() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              yields[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              yields[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              yields[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY"
            ) {
              yields[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord KEY OPTIONS {key: 'value'}"
            ) {
              yields[Statements](ast.CreateRelationshipKeyConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("key" -> literalString("value"))),
                containsOn,
                constraintVersion
              ))
            }

            // Relationship uniqueness

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()-[r1:R]->() $requireOrAssertString (r2.prop1, r3.prop2) IS $relKeyWord UNIQUE"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop1"), prop("r3", "prop2")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()<-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()<-[r1:R]->() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS $relKeyWord UNIQUE OPTIONS {key: 'value'}"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
                varFor("r1"),
                relTypeName("R"),
                Seq(prop("r2", "prop")),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.OptionsMap(Map("key" -> literalString("value"))),
                containsOn,
                constraintVersion
              ))
            }
          })

          // Node property existence

          test(
            s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NOT NULL"
          ) {
            yields[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              Some("my_constraint"),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL"
          ) {
            yields[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              Some("my_constraint"),
              ast.IfExistsReplace,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL"
          ) {
            yields[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              Some("my_constraint"),
              ast.IfExistsInvalidSyntax,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(
            s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL"
          ) {
            yields[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              Some("my_constraint"),
              ast.IfExistsDoNothing,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(
            s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL OPTIONS {}"
          ) {
            yields[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("node"),
              labelName("Label"),
              prop("node", "prop"),
              Some("my_constraint"),
              ast.IfExistsThrowError,
              ast.OptionsMap(Map.empty),
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop2, node.prop3) IS NOT NULL"
          ) {
            assertFailsWithException[Statements](
              testName,
              new Neo4jASTConstructionException(
                ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_IS_NOT_NULL)
              )
            )
          }

          // Relationship property existence

          test(
            s"CREATE CONSTRAINT `$$my_constraint` $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS NOT NULL"
          ) {
            yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              Some("$my_constraint"),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(
            s"CREATE CONSTRAINT my_constraint $forOrOnString ()-[r:R]-() $requireOrAssertString (r.prop) IS NOT NULL"
          ) {
            yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              Some("my_constraint"),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT `$$my_constraint` $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS NOT NULL"
          ) {
            yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              Some("$my_constraint"),
              ast.IfExistsReplace,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT `$$my_constraint` IF NOT EXISTS $forOrOnString ()-[r:R]->() $requireOrAssertString (r.prop) IS NOT NULL"
          ) {
            yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              Some("$my_constraint"),
              ast.IfExistsInvalidSyntax,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(
            s"CREATE CONSTRAINT `$$my_constraint` IF NOT EXISTS $forOrOnString ()<-[r:R]-() $requireOrAssertString r.prop IS NOT NULL"
          ) {
            yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              Some("$my_constraint"),
              ast.IfExistsDoNothing,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(
            s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString ()-[r1:REL]-() $requireOrAssertString (r2.prop2, r3.prop3) IS NOT NULL"
          ) {
            assertFailsWithException[Statements](
              testName,
              new Neo4jASTConstructionException(
                ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.REL_IS_NOT_NULL)
              )
            )
          }

          Seq("IS TYPED", "IS ::", "::").foreach(typeKeyword => {
            // Node property type

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) $typeKeyword STRING"
            ) {
              yields[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                StringType(isNullable = true)(pos),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString node.prop $typeKeyword STRING"
            ) {
              yields[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                StringType(isNullable = true)(pos),
                Some("my_constraint"),
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop $typeKeyword STRING"
            ) {
              yields[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                StringType(isNullable = true)(pos),
                Some("my_constraint"),
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop $typeKeyword STRING"
            ) {
              yields[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                StringType(isNullable = true)(pos),
                Some("my_constraint"),
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString node.prop $typeKeyword STRING OPTIONS {}"
            ) {
              yields[Statements](ast.CreateNodePropertyTypeConstraint(
                varFor("node"),
                labelName("Label"),
                prop("node", "prop"),
                StringType(isNullable = true)(pos),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.OptionsMap(Map.empty),
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop2, node.prop3) $typeKeyword STRING"
            ) {
              assertFailsWithException[Statements](
                testName,
                new Neo4jASTConstructionException(
                  ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_IS_TYPED)
                )
              )
            }

            // Relationship property type

            test(
              s"CREATE CONSTRAINT `$$my_constraint` $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop $typeKeyword STRING"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                StringType(isNullable = true)(pos),
                Some("$my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT my_constraint $forOrOnString ()-[r:R]-() $requireOrAssertString (r.prop) $typeKeyword STRING"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                StringType(isNullable = true)(pos),
                Some("my_constraint"),
                ast.IfExistsThrowError,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT `$$my_constraint` $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop $typeKeyword STRING"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                StringType(isNullable = true)(pos),
                Some("$my_constraint"),
                ast.IfExistsReplace,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT `$$my_constraint` IF NOT EXISTS $forOrOnString ()-[r:R]->() $requireOrAssertString (r.prop) $typeKeyword STRING"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                StringType(isNullable = true)(pos),
                Some("$my_constraint"),
                ast.IfExistsInvalidSyntax,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE CONSTRAINT `$$my_constraint` IF NOT EXISTS $forOrOnString ()<-[r:R]-() $requireOrAssertString r.prop $typeKeyword STRING"
            ) {
              yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
                varFor("r"),
                relTypeName("R"),
                prop("r", "prop"),
                StringType(isNullable = true)(pos),
                Some("$my_constraint"),
                ast.IfExistsDoNothing,
                ast.NoOptions,
                containsOn,
                constraintVersion
              ))
            }

            test(
              s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString ()-[r1:REL]-() $requireOrAssertString (r2.prop2, r3.prop3) $typeKeyword STRING"
            ) {
              assertFailsWithException[Statements](
                testName,
                new Neo4jASTConstructionException(
                  ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.REL_IS_TYPED)
                )
              )
            }
          })

          // Negative tests

          test(
            s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY {indexProvider : 'range-1.0'}"
          ) {
            failsToParse[Statements]
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY OPTIONS"
          ) {
            failsToParse[Statements]
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop.part IS UNIQUE") {
            failsToParse[Statements]
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop.part) IS UNIQUE") {
            failsToParse[Statements]
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS UNIQUE {indexProvider : 'range-1.0'}"
          ) {
            failsToParse[Statements]
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop1, node.prop2) IS UNIQUE OPTIONS"
          ) {
            failsToParse[Statements]
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop1, node.prop2) IS NOT NULL"
          ) {
            assertFailsWithMessage[Statements](
              testName,
              "Constraint type 'IS NOT NULL' does not allow multiple properties"
            )
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString (r.prop1, r.prop2) IS NOT NULL"
          ) {
            assertFailsWithMessage[Statements](
              testName,
              "Constraint type 'IS NOT NULL' does not allow multiple properties"
            )
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()-[r1:REL]-() $requireOrAssertString (r2.prop) IS NODE KEY") {
            assertFailsWithMessageStart[Statements](
              testName,
              ASTExceptionFactory.relationshipPatternNotAllowed(ConstraintType.NODE_KEY)
            )
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()-[r1:REL]-() $requireOrAssertString (r2.prop) IS NODE UNIQUE") {
            assertFailsWithMessageStart[Statements](
              testName,
              ASTExceptionFactory.relationshipPatternNotAllowed(ConstraintType.NODE_UNIQUE)
            )
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (r.prop) IS RELATIONSHIP KEY") {
            assertFailsWithMessageStart[Statements](
              testName,
              ASTExceptionFactory.nodePatternNotAllowed(ConstraintType.REL_KEY)
            )
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (r.prop) IS REL KEY") {
            assertFailsWithMessageStart[Statements](
              testName,
              ASTExceptionFactory.nodePatternNotAllowed(ConstraintType.REL_KEY)
            )
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (r.prop) IS RELATIONSHIP UNIQUE"
          ) {
            assertFailsWithMessageStart[Statements](
              testName,
              ASTExceptionFactory.nodePatternNotAllowed(ConstraintType.REL_UNIQUE)
            )
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (r.prop) IS REL UNIQUE") {
            assertFailsWithMessageStart[Statements](
              testName,
              ASTExceptionFactory.nodePatternNotAllowed(ConstraintType.REL_UNIQUE)
            )
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS UNIQUENESS"
          ) {
            failsToParse[Statements]
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS NODE UNIQUENESS"
          ) {
            failsToParse[Statements]
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS UNIQUENESS"
          ) {
            failsToParse[Statements]
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS RELATIONSHIP UNIQUENESS"
          ) {
            failsToParse[Statements]
          }

          test(
            s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS REL UNIQUENESS"
          ) {
            failsToParse[Statements]
          }

          // constraint name parameter

          test(s"CREATE CONSTRAINT $$name $forOrOnString (n:L) $requireOrAssertString n.prop IS NODE KEY") {
            yields[Statements](ast.CreateNodeKeyConstraint(
              varFor("n"),
              labelName("L"),
              Seq(prop("n", "prop")),
              Some(Right(stringParam("name"))),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersion
            ))
          }

          test(
            s"CREATE CONSTRAINT $$name $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS RELATIONSHIP KEY"
          ) {
            yields[Statements](ast.CreateRelationshipKeyConstraint(
              varFor("r"),
              relTypeName("R"),
              Seq(prop("r", "prop")),
              Some(Right(stringParam("name"))),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersion
            ))
          }

          test(s"CREATE CONSTRAINT $$name $forOrOnString (n:L) $requireOrAssertString n.prop IS UNIQUE") {
            yields[Statements](ast.CreateNodePropertyUniquenessConstraint(
              varFor("n"),
              labelName("L"),
              Seq(prop("n", "prop")),
              Some(Right(stringParam("name"))),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersion
            ))
          }

          test(s"CREATE CONSTRAINT $$name $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS UNIQUE") {
            yields[Statements](ast.CreateRelationshipPropertyUniquenessConstraint(
              varFor("r"),
              relTypeName("R"),
              Seq(prop("r", "prop")),
              Some(Right(stringParam("name"))),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersion
            ))
          }

          test(s"CREATE CONSTRAINT $$name $forOrOnString (n:L) $requireOrAssertString n.prop IS NOT NULL") {
            yields[Statements](ast.CreateNodePropertyExistenceConstraint(
              varFor("n"),
              labelName("L"),
              prop("n", "prop"),
              Some(Right(stringParam("name"))),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(s"CREATE CONSTRAINT $$name $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS NOT NULL") {
            yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              Some(Right(stringParam("name"))),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersionOneOrTwo
            ))
          }

          test(s"CREATE CONSTRAINT $$name $forOrOnString (n:L) $requireOrAssertString n.prop IS TYPED STRING") {
            yields[Statements](ast.CreateNodePropertyTypeConstraint(
              varFor("n"),
              labelName("L"),
              prop("n", "prop"),
              StringType(isNullable = true)(pos),
              Some(Right(stringParam("name"))),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersion
            ))
          }

          test(s"CREATE CONSTRAINT $$name $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS TYPED STRING") {
            yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
              varFor("r"),
              relTypeName("R"),
              prop("r", "prop"),
              StringType(isNullable = true)(pos),
              Some(Right(stringParam("name"))),
              ast.IfExistsThrowError,
              ast.NoOptions,
              containsOn,
              constraintVersion
            ))
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
      yields[Statements](ast.CreateNodePropertyTypeConstraint(
        varFor("n"),
        labelName("Label"),
        prop("r", "prop"),
        typeExpr,
        None,
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      ))
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR (n:Label) REQUIRE r.prop IS TYPED ${typeString.toLowerCase}"
    ) {
      yields[Statements](ast.CreateNodePropertyTypeConstraint(
        varFor("n"),
        labelName("Label"),
        prop("r", "prop"),
        typeExpr,
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      ))
    }

    test(s"CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE n.prop IS TYPED ${typeString.toLowerCase}") {
      yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("n", "prop"),
        typeExpr,
        None,
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      ))
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE n.prop IS TYPED $typeString"
    ) {
      yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("n", "prop"),
        typeExpr,
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      ))
    }
  }

  disallowedNonListSingleTypes.foreach { case (typeString, typeExpr: CypherType) =>
    test(
      s"CREATE CONSTRAINT my_constraint FOR (n:Label) REQUIRE r.prop IS TYPED ${typeString.toLowerCase}"
    ) {
      yields[Statements](ast.CreateNodePropertyTypeConstraint(
        varFor("n"),
        labelName("Label"),
        prop("r", "prop"),
        typeExpr,
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      ))
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE n.prop IS TYPED $typeString"
    ) {
      yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("n", "prop"),
        typeExpr,
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      ))
    }
  }

  listSingleTypes.foreach { case (listTypeString, listTypeExpr: CypherType) =>
    test(
      s"CREATE CONSTRAINT my_constraint FOR (n:Label) REQUIRE r.prop IS TYPED ${listTypeString.toLowerCase}"
    ) {
      yields[Statements](ast.CreateNodePropertyTypeConstraint(
        varFor("n"),
        labelName("Label"),
        prop("r", "prop"),
        listTypeExpr,
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      ))
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE n.prop IS TYPED $listTypeString"
    ) {
      yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("n", "prop"),
        listTypeExpr,
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      ))
    }
  }

  unionTypes.foreach { case (unionTypeString, unionTypeExpr: CypherType) =>
    test(
      s"CREATE CONSTRAINT my_constraint FOR (n:Label) REQUIRE r.prop IS TYPED ${unionTypeString.toLowerCase}"
    ) {
      yields[Statements](ast.CreateNodePropertyTypeConstraint(
        varFor("n"),
        labelName("Label"),
        prop("r", "prop"),
        unionTypeExpr,
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      ))
    }

    test(
      s"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE n.prop IS TYPED $unionTypeString"
    ) {
      yields[Statements](ast.CreateRelationshipPropertyTypeConstraint(
        varFor("r"),
        relTypeName("R"),
        prop("n", "prop"),
        unionTypeExpr,
        Some("my_constraint"),
        ast.IfExistsThrowError,
        ast.NoOptions,
        containsOn = false,
        ast.ConstraintVersion2
      ))
    }
  }

  test("CREATE CONSTRAINT my_constraint FOR (n:L) REQUIRE n.p IS :: ANY<BOOLEAN | STRING> NOT NULL") {
    assertFailsWithMessage[Statements](
      testName,
      "Closed Dynamic Union Types can not be appended with `NOT NULL`, specify `NOT NULL` on all inner types instead. (line 1, column 61 (offset: 60))",
      failsOnlyJavaCC = true
    )
  }

  test("CREATE CONSTRAINT my_constraint FOR (n:L) REQUIRE n.p IS :: BOOLEAN LIST NOT NULL NOT NULL") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'NOT': expected \"ARRAY\", \"LIST\", \"OPTIONS\" or <EOF> (line 1, column 83 (offset: 82))"
    )
  }

  // ASSERT EXISTS

  test("CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
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
    yields[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      None,
      ast.IfExistsReplace,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      None,
      ast.IfExistsInvalidSyntax,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test("CREATE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      None,
      ast.IfExistsDoNothing,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop) OPTIONS {}") {
    yields[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      None,
      ast.IfExistsThrowError,
      ast.OptionsMap(Map.empty),
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test("CREATE CONSTRAINT ON (node1:Label) ASSERT EXISTS (node2.prop1, node3.prop2)") {
    assertFailsWithException[Statements](
      testName,
      new Neo4jASTConstructionException(ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_EXISTS))
    )
  }

  test("CREATE CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test("CREATE CONSTRAINT ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test("CREATE CONSTRAINT ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      None,
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
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
    yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      None,
      ast.IfExistsReplace,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      None,
      ast.IfExistsInvalidSyntax,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test("CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      None,
      ast.IfExistsDoNothing,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test("CREATE CONSTRAINT ON ()-[r1:REL]-() ASSERT EXISTS (r2.prop1, r3.prop2)") {
    assertFailsWithException[Statements](
      testName,
      new Neo4jASTConstructionException(ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.REL_EXISTS))
    )
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      Some("my_constraint"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      Some("my_constraint"),
      ast.IfExistsReplace,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      Some("my_constraint"),
      ast.IfExistsInvalidSyntax,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test("CREATE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop"),
      Some("my_constraint"),
      ast.IfExistsDoNothing,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test("CREATE CONSTRAINT `$my_constraint` ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      Some("$my_constraint"),
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test("CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop) OPTIONS {}") {
    yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      Some("my_constraint"),
      ast.IfExistsThrowError,
      ast.OptionsMap(Map.empty),
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test("CREATE OR REPLACE CONSTRAINT `$my_constraint` ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      Some("$my_constraint"),
      ast.IfExistsReplace,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test("CREATE OR REPLACE CONSTRAINT `$my_constraint` IF NOT EXISTS ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      Some("$my_constraint"),
      ast.IfExistsInvalidSyntax,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test("CREATE CONSTRAINT `$my_constraint` IF NOT EXISTS ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      Some("$my_constraint"),
      ast.IfExistsDoNothing,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  test(
    s"CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop1, node.prop2)"
  ) {
    assertFailsWithMessage[Statements](testName, "Constraint type 'EXISTS' does not allow multiple properties")
  }

  test(
    s"CREATE CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop1, r.prop2)"
  ) {
    assertFailsWithMessage[Statements](testName, "Constraint type 'EXISTS' does not allow multiple properties")
  }

  test("CREATE CONSTRAINT $my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields[Statements](ast.CreateRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop"),
      Some(Right(stringParam("my_constraint"))),
      ast.IfExistsThrowError,
      ast.NoOptions,
      containsOn = true,
      ast.ConstraintVersion0
    ))
  }

  // Edge case tests

  test(
    "CREATE CONSTRAINT my_constraint FOR (n:Person) REQUIRE n.prop IS NOT NULL OPTIONS {indexProvider : 'range-1.0'};"
  ) {
    yields[Statements](ast.CreateNodePropertyExistenceConstraint(
      varFor("n"),
      labelName("Person"),
      prop("n", "prop"),
      Some("my_constraint"),
      ast.IfExistsThrowError,
      ast.OptionsMap(Map("indexProvider" -> literalString("range-1.0"))),
      containsOn = false,
      constraintVersion = ast.ConstraintVersion2
    ))
  }

  test(
    "CREATE CONSTRAINT FOR (n:Person) REQUIRE n.prop IS NOT NULL; CREATE CONSTRAINT FOR (n:User) REQUIRE n.prop IS UNIQUE"
  ) {
    // The test setup does 'fromParser(_.Statements().get(0)', so only the first statement is yielded.
    // The purpose of the test is to make sure the parser does not throw an error on the semicolon, which was an issue before.
    // If we want to test that both statements are parsed, the test framework needs to be extended.
    parses[Statements](NotAnyAntlr).withAstLike { statements =>
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
    assertFailsWithMessageStart[Statements](testName, """Invalid input ':': expected ")" or an identifier""")
  }

  test("CREATE CONSTRAINT FOR ()-[n1:R]-(:A) REQUIRE (n2.name) IS UNIQUE") {
    // label on node
    assertFailsWithMessageStart[Statements](testName, """Invalid input ':': expected ")"""")
  }

  test("CREATE CONSTRAINT FOR (n2)-[n1:R]-() REQUIRE (n2.name) IS NOT NULL") {
    // variable on node
    assertFailsWithMessageStart[Statements](testName, """Invalid input ')': expected ":"""")
  }

  test("CREATE CONSTRAINT FOR ()-[n1:R]-(n2) REQUIRE (n2.name) IS :: STRING") {
    // variable on node
    assertFailsWithMessageStart[Statements](testName, """Invalid input 'n2': expected ")"""")
  }

  test("CREATE CONSTRAINT FOR (n2:A)-[n1:R]-() REQUIRE (n2.name) IS KEY") {
    // variable on node
    assertFailsWithMessageStart[Statements](testName, """Invalid input '-': expected "ASSERT" or "REQUIRE"""")
  }

  test("CREATE CONSTRAINT FOR ()-[n1:R]-(n2:A) REQUIRE (n2.name) IS RELATIONSHIP UNIQUE") {
    // variable on node
    assertFailsWithMessageStart[Statements](testName, """Invalid input 'n2': expected ")"""")
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop) IS NOT NULL") {
    failsToParse[Statements]
  }

  test("CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop) IS NOT NULL") {
    assertFailsWithMessageStart[Statements](
      testName,
      "Invalid input 'IS': expected \"OPTIONS\" or <EOF> (line 1, column 71 (offset: 70))"
    )
  }

  test("CREATE CONSTRAINT FOR (n:Label) REQUIRE (n.prop)") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input '': expected \"::\" or \"IS\" (line 1, column 49 (offset: 48))"
    )
  }

  test("CREATE CONSTRAINT FOR (node:Label) REQUIRE EXISTS (node.prop)") {
    assertFailsWithMessage[Statements](testName, "Invalid input '(': expected \".\" (line 1, column 51 (offset: 50))")
  }

  test("CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE EXISTS (r.prop)") {
    assertFailsWithMessage[Statements](testName, "Invalid input '(': expected \".\" (line 1, column 50 (offset: 49))")
  }

  test(s"CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT r.prop IS NULL") {
    assertFailsWithMessage[Statements](
      testName,
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
  }

  test(s"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE r.prop IS NULL") {
    assertFailsWithMessage[Statements](
      testName,
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
  }

  test(s"CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT node.prop IS NULL") {
    assertFailsWithMessage[Statements](
      testName,
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
  }

  test(s"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE node.prop IS NULL") {
    assertFailsWithMessage[Statements](
      testName,
      """Invalid input 'NULL': expected
        |  "::"
        |  "KEY"
        |  "NODE"
        |  "NOT"
        |  "REL"
        |  "RELATIONSHIP"
        |  "TYPED"
        |  "UNIQUE" (line 1, column 71 (offset: 70))""".stripMargin
    )
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS TYPED") {
    assertFailsWithMessageStart[Statements](
      testName,
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
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS ::") {
    assertFailsWithMessageStart[Statements](
      testName,
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
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p ::") {
    assertFailsWithMessageStart[Statements](
      testName,
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
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p :: TYPED") {
    assertFailsWithMessage[Statements](
      testName,
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
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p :: UNIQUE") {
    assertFailsWithMessage[Statements](
      testName,
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
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p :: BOOLEAN UNIQUE") {
    assertFailsWithMessageStart[Statements](
      testName,
      """Invalid input 'UNIQUE': expected
        |  "!"
        |  "ARRAY"
        |  "LIST"
        |  "NOT"
        |  "OPTIONS"
        |  <EOF>""".stripMargin
    )
  }

  test("CREATE CONSTRAINT FOR (n:L) REQUIRE n.p IS :: BOOL EAN") {
    assertFailsWithMessageStart[Statements](
      testName,
      """Invalid input 'EAN': expected
        |  "!"
        |  "ARRAY"
        |  "LIST"
        |  "NOT"
        |  "OPTIONS"
        |  <EOF>""".stripMargin
    )
  }

  // Drop constraint

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    yields[Statements](ast.DropNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop"))))
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
    yields[Statements](ast.DropNodeKeyConstraint(
      varFor("node"),
      labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2"))
    ))
  }

  test("DROP CONSTRAINT ON ()-[r1:R]-() ASSERT r2.prop IS NODE KEY") {
    assertFailsWithMessageStart[Statements](
      testName,
      ASTExceptionFactory.relationshipPatternNotAllowed(ConstraintType.NODE_KEY)
    )
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE") {
    yields[Statements](ast.DropPropertyUniquenessConstraint(
      varFor("node"),
      labelName("Label"),
      Seq(prop("node", "prop"))
    ))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop) IS UNIQUE") {
    yields[Statements](ast.DropPropertyUniquenessConstraint(
      varFor("node"),
      labelName("Label"),
      Seq(prop("node", "prop"))
    ))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE") {
    yields[Statements](ast.DropPropertyUniquenessConstraint(
      varFor("node"),
      labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2"))
    ))
  }

  test("DROP CONSTRAINT ON ()-[r1:R]-() ASSERT r2.prop IS UNIQUE") {
    assertFailsWithMessageStart[Statements](
      testName,
      ASTExceptionFactory.relationshipPatternNotAllowed(ConstraintType.NODE_UNIQUE)
    )
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields[Statements](ast.DropNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "prop")
    ))
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
    yields[Statements](ast.DropRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop")
    ))
  }

  test("DROP CONSTRAINT ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    yields[Statements](ast.DropRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop")
    ))
  }

  test("DROP CONSTRAINT ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields[Statements](ast.DropRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "prop")
    ))
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

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NOT NULL") {
    assertFailsWithException[Statements](
      testName,
      new Neo4jASTConstructionException(ASTExceptionFactory.invalidDropCommand)
    )
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS NOT NULL") {
    assertFailsWithException[Statements](
      testName,
      new Neo4jASTConstructionException(ASTExceptionFactory.invalidDropCommand)
    )
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT (r.prop) IS NOT NULL") {
    assertFailsWithException[Statements](
      testName,
      new Neo4jASTConstructionException(ASTExceptionFactory.invalidDropCommand)
    )
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.prop IS NOT NULL") {
    assertFailsWithException[Statements](
      testName,
      new Neo4jASTConstructionException(ASTExceptionFactory.invalidDropCommand)
    )
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.EXISTS) IS NODE KEY") {
    yields[Statements](ast.DropNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "EXISTS"))))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.EXISTS) IS UNIQUE") {
    yields[Statements](ast.DropPropertyUniquenessConstraint(
      varFor("node"),
      labelName("Label"),
      Seq(prop("node", "EXISTS"))
    ))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT EXISTS (node.EXISTS)") {
    yields[Statements](ast.DropNodePropertyExistenceConstraint(
      varFor("node"),
      labelName("Label"),
      prop("node", "EXISTS")
    ))
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.EXISTS)") {
    yields[Statements](ast.DropRelationshipPropertyExistenceConstraint(
      varFor("r"),
      relTypeName("R"),
      prop("r", "EXISTS")
    ))
  }

  test("DROP CONSTRAINT my_constraint") {
    yields[Statements](ast.DropConstraintOnName("my_constraint", ifExists = false))
  }

  test("DROP CONSTRAINT `$my_constraint`") {
    yields[Statements](ast.DropConstraintOnName("$my_constraint", ifExists = false))
  }

  test("DROP CONSTRAINT my_constraint IF EXISTS") {
    yields[Statements](ast.DropConstraintOnName("my_constraint", ifExists = true))
  }

  test("DROP CONSTRAINT $my_constraint") {
    yields[Statements](ast.DropConstraintOnName(Right(stringParam("my_constraint")), ifExists = false))
  }

  test("DROP CONSTRAINT my_constraint IF EXISTS;") {
    yields[Statements](ast.DropConstraintOnName("my_constraint", ifExists = true))
  }

  test("DROP CONSTRAINT my_constraint; DROP CONSTRAINT my_constraint2;") {
    // The test setup does 'fromParser(_.Statements().get(0)', so only the first statement is yielded.
    // The purpose of the test is to make sure the parser does not throw an error on the semicolon, which was an issue before.
    // If we want to test that both statements are parsed, the test framework needs to be extended.
    parses[Statements](NotAnyAntlr).withAstLike { statements =>
      statements.get(0) shouldBe ast.DropConstraintOnName("my_constraint", ifExists = false)(pos)
    }
  }

  test("DROP CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    assertFailsWithMessage[Statements](
      testName,
      "Invalid input 'ON': expected \"IF\" or <EOF> (line 1, column 31 (offset: 30))"
    )
  }
}
