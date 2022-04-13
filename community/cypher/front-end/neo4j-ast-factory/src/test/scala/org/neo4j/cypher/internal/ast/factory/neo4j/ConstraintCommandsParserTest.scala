/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.ConstraintVersion0
import org.neo4j.cypher.internal.ast.ConstraintVersion1
import org.neo4j.cypher.internal.ast.ConstraintVersion2
import org.neo4j.cypher.internal.ast.CreateNodeKeyConstraint
import org.neo4j.cypher.internal.ast.CreateNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateUniquePropertyConstraint
import org.neo4j.cypher.internal.ast.DropNodeKeyConstraint
import org.neo4j.cypher.internal.ast.DropNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.DropRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.OptionsParam
import org.neo4j.cypher.internal.ast.factory.ASTExceptionFactory
import org.neo4j.cypher.internal.ast.factory.ConstraintType
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.PropertyKeyName
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.symbols.CTMap

/* Tests for creating and dropping constraints */
class ConstraintCommandsParserTest extends AdministrationAndSchemaCommandParserTestBase {

  Seq("ON", "FOR")
    .foreach { forOrOnString =>
      Seq("ASSERT", "REQUIRE")
        .foreach { requireOrAssertString =>
          val containsOn = forOrOnString == "ON"
          val constraintVersion = if (requireOrAssertString == "REQUIRE") ConstraintVersion2 else ConstraintVersion0
          val constraintVersionOneOrTwo = if (requireOrAssertString == "REQUIRE") ConstraintVersion2 else ConstraintVersion1

          // Create constraint: Without name

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY") {
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, ast.IfExistsThrowError, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS NODE KEY") {
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, ast.IfExistsThrowError, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE OR REPLACE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY") {
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, ast.IfExistsReplace, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY") {
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, ast.IfExistsDoNothing, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS NODE KEY") {
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"),
              Seq(prop("node", "prop1"), prop("node", "prop2")), None, ast.IfExistsThrowError, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS NODE KEY") {
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"),
              Seq(prop("node", "prop1"), prop("node", "prop2")), None, ast.IfExistsInvalidSyntax, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY OPTIONS {indexProvider : 'range-1.0'}") {
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")),
              None, ast.IfExistsThrowError, OptionsMap(Map("indexProvider" -> literalString("range-1.0"))), containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}") {
            // will fail in options converter
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, ast.IfExistsThrowError,
              OptionsMap(Map("indexProvider" -> literalString("native-btree-1.0"),
                "indexConfig" -> mapOf(
                  "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
                  "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
                ))
              ), containsOn, constraintVersion
            ))
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY OPTIONS {indexConfig : {someConfig: 'toShowItCanBeParsed' }}") {
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, ast.IfExistsThrowError,
              OptionsMap(Map("indexConfig" -> mapOf("someConfig" -> literalString("toShowItCanBeParsed")))), containsOn, constraintVersion
            ))
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY OPTIONS {nonValidOption : 42}") {
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, ast.IfExistsThrowError,
              OptionsMap(Map("nonValidOption" -> literalInt(42))), containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY OPTIONS {}") {
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, ast.IfExistsThrowError, OptionsMap(Map.empty), containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY OPTIONS $$param") {
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, ast.IfExistsThrowError,
              OptionsParam(parameter("param", CTMap)), containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()-[r1:REL]-() $requireOrAssertString (r2.prop) IS NODE KEY") {
            assertFailsWithMessageStart(testName, ASTExceptionFactory.relationshipPattternNotAllowed(ConstraintType.NODE_KEY))
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS UNIQUE") {
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, ast.IfExistsThrowError, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE OR REPLACE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS UNIQUE") {
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, ast.IfExistsReplace, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop IS UNIQUE") {
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, ast.IfExistsDoNothing, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS UNIQUE") {
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, ast.IfExistsThrowError, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS UNIQUE") {
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"),
              Seq(prop("node", "prop1"), prop("node", "prop2")), None, ast.IfExistsThrowError, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS UNIQUE") {
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"),
              Seq(prop("node", "prop1"), prop("node", "prop2")), None, ast.IfExistsInvalidSyntax, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS UNIQUE OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}, indexProvider : 'native-btree-1.0'}") {
            // will fail in options converter
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, ast.IfExistsThrowError,
              OptionsMap(Map("indexProvider" -> literalString("native-btree-1.0"),
                "indexConfig" -> mapOf(
                  "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
                  "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
                )
              )), containsOn, constraintVersion
            ))
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS UNIQUE OPTIONS $$options") {
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, ast.IfExistsThrowError,
              OptionsParam(parameter("options", CTMap)), containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()-[r1:R]-() $requireOrAssertString (r2.prop) IS UNIQUE") {
            assertFailsWithMessageStart(testName, ASTExceptionFactory.relationshipPattternNotAllowed(ConstraintType.UNIQUE))
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL") {
            yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), None, ast.IfExistsThrowError, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NOT NULL") {
            yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), None, ast.IfExistsThrowError, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE OR REPLACE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL") {
            yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), None, ast.IfExistsReplace, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL") {
            yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), None, ast.IfExistsInvalidSyntax, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NOT NULL") {
            yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), None, ast.IfExistsDoNothing, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS NOT NULL") {
            yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, ast.IfExistsThrowError, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()-[r:R]->() $requireOrAssertString r.prop IS NOT NULL") {
            yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, ast.IfExistsThrowError, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()<-[r:R]-() $requireOrAssertString (r.prop) IS NOT NULL") {
            yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, ast.IfExistsThrowError, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE OR REPLACE CONSTRAINT $forOrOnString ()<-[r:R]-() $requireOrAssertString r.prop IS NOT NULL") {
            yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, ast.IfExistsReplace, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE OR REPLACE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS NOT NULL") {
            yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, ast.IfExistsInvalidSyntax, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE CONSTRAINT IF NOT EXISTS $forOrOnString ()-[r:R]->() $requireOrAssertString r.prop IS NOT NULL") {
            yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, ast.IfExistsDoNothing, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE CONSTRAINT $forOrOnString ()-[r:R]-() $requireOrAssertString (r.prop) IS NOT NULL OPTIONS {}") {
            yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, ast.IfExistsThrowError, OptionsMap(Map.empty), containsOn, constraintVersionOneOrTwo))
          }

          // Create constraint: With name

          test(s"USE neo4j CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY") {
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), ast.IfExistsThrowError, NoOptions, containsOn, constraintVersion, Some(use(varFor("neo4j")))))
          }

          test(s"USE neo4j CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY") {
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), ast.IfExistsInvalidSyntax, NoOptions, containsOn, constraintVersion, Some(use(varFor("neo4j")))))
          }

          test(s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS NODE KEY") {
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"),
              Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), ast.IfExistsThrowError, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS NODE KEY") {
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"),
              Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), ast.IfExistsReplace, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS NODE KEY") {
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"),
              Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), ast.IfExistsDoNothing, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}}") {
            // will fail in options converter
            yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), ast.IfExistsThrowError,
              OptionsMap(Map("indexProvider" -> literalString("native-btree-1.0"),
                "indexConfig" -> mapOf(
                  "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
                  "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
                )
              )), containsOn, constraintVersion
            ))
          }

          test(s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString node.prop IS UNIQUE") {
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), ast.IfExistsThrowError, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS UNIQUE") {
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), ast.IfExistsThrowError, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS UNIQUE") {
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), ast.IfExistsInvalidSyntax, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS UNIQUE") {
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"),
              Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), ast.IfExistsThrowError, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS UNIQUE") {
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"),
              Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), ast.IfExistsReplace, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS UNIQUE") {
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"),
              Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), ast.IfExistsDoNothing, NoOptions, containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS UNIQUE OPTIONS {indexProvider : 'range-1.0'}") {
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")),
              Some("my_constraint"), ast.IfExistsThrowError, OptionsMap(Map("indexProvider" -> literalString("range-1.0"))), containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS UNIQUE OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}") {
            // will fail in options converter
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), ast.IfExistsThrowError,
              OptionsMap(Map("indexProvider" -> literalString("native-btree-1.0"),
                "indexConfig" -> mapOf(
                  "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
                  "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
                )
              )), containsOn, constraintVersion
            ))
          }

          test(s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS UNIQUE OPTIONS {indexConfig : {someConfig: 'toShowItCanBeParsed' }}") {
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), ast.IfExistsThrowError,
              OptionsMap(Map("indexConfig" -> mapOf("someConfig" -> literalString("toShowItCanBeParsed")))), containsOn, constraintVersion
            ))
          }

          test(s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS UNIQUE OPTIONS {nonValidOption : 42}") {
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")),
              Some("my_constraint"), ast.IfExistsThrowError, OptionsMap(Map("nonValidOption" -> literalInt(42))), containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop1,node.prop2) IS UNIQUE OPTIONS {}") {
            yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop1"), prop("node", "prop2")),
              Some("my_constraint"), ast.IfExistsThrowError, OptionsMap(Map.empty), containsOn, constraintVersion))
          }

          test(s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NOT NULL") {
             yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), Some("my_constraint"), ast.IfExistsThrowError, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL") {
            yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), Some("my_constraint"), ast.IfExistsReplace, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL") {
            yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), Some("my_constraint"), ast.IfExistsInvalidSyntax, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE CONSTRAINT my_constraint IF NOT EXISTS $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL") {
            yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), Some("my_constraint"), ast.IfExistsDoNothing, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString node.prop IS NOT NULL OPTIONS {}") {
            yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), Some("my_constraint"), ast.IfExistsThrowError, OptionsMap(Map.empty), containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString (node:Label) $requireOrAssertString (node.prop2, node.prop3) IS NOT NULL") {
            assertFailsWithException(testName, new Neo4jASTConstructionException(ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_IS_NOT_NULL)))
          }

          test(s"CREATE CONSTRAINT `$$my_constraint` $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS NOT NULL") {
            yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), Some("$my_constraint"), ast.IfExistsThrowError, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE CONSTRAINT my_constraint $forOrOnString ()-[r:R]-() $requireOrAssertString (r.prop) IS NOT NULL") {
            yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), Some("my_constraint"), ast.IfExistsThrowError, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE OR REPLACE CONSTRAINT `$$my_constraint` $forOrOnString ()-[r:R]-() $requireOrAssertString r.prop IS NOT NULL") {
            yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), Some("$my_constraint"), ast.IfExistsReplace, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE OR REPLACE CONSTRAINT `$$my_constraint` IF NOT EXISTS $forOrOnString ()-[r:R]->() $requireOrAssertString (r.prop) IS NOT NULL") {
            yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), Some("$my_constraint"), ast.IfExistsInvalidSyntax, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE CONSTRAINT `$$my_constraint` IF NOT EXISTS $forOrOnString ()<-[r:R]-() $requireOrAssertString r.prop IS NOT NULL") {
            yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), Some("$my_constraint"), ast.IfExistsDoNothing, NoOptions, containsOn, constraintVersionOneOrTwo))
          }

          test(s"CREATE OR REPLACE CONSTRAINT my_constraint $forOrOnString ()-[r1:REL]-() $requireOrAssertString (r2.prop2, r3.prop3) IS NOT NULL") {
            assertFailsWithException(testName, new Neo4jASTConstructionException(ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.REL_IS_NOT_NULL)))
          }

          // Negative tests

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY {indexProvider : 'range-1.0'}") {
            failsToParse
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS NODE KEY OPTIONS") {
            failsToParse
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString node.prop.part IS UNIQUE") {
            failsToParse
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop.part) IS UNIQUE") {
            failsToParse
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop) IS UNIQUE {indexProvider : 'range-1.0'}") {
            failsToParse
          }

          test(s"CREATE CONSTRAINT $forOrOnString (node:Label) $requireOrAssertString (node.prop1, node.prop2) IS UNIQUE OPTIONS") {
            failsToParse
          }
        }
    }

  // ASSERT EXISTS

  test("CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), None, ast.IfExistsThrowError, NoOptions, containsOn = true, ConstraintVersion0))
  }

  test("CREATE CONSTRAINT ON (node1:Label) ASSERT EXISTS node2.prop") {
    assertAst(CreateNodePropertyExistenceConstraint(
        Variable("node1")(1, 23, 22),
        LabelName("Label")(1, 29, 28),
        Property(Variable("node2")(1, 50, 49), PropertyKeyName("prop")(1, 56, 55))(1, 50, 49),
        None, IfExistsThrowError, NoOptions, containsOn = true, ConstraintVersion0, None)(defaultPos))
  }

  test("CREATE OR REPLACE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), None, ast.IfExistsReplace, NoOptions, containsOn = true, ConstraintVersion0))
  }

  test("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), None, ast.IfExistsInvalidSyntax, NoOptions, containsOn = true, ConstraintVersion0))
  }

  test("CREATE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), None, ast.IfExistsDoNothing, NoOptions, containsOn = true, ConstraintVersion0))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop) OPTIONS {}") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), None, ast.IfExistsThrowError, OptionsMap(Map.empty), containsOn = true, ConstraintVersion0))
  }

  test("CREATE CONSTRAINT ON (node1:Label) ASSERT EXISTS (node2.prop1, node3.prop2)") {
    assertFailsWithException(testName, new Neo4jASTConstructionException(ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.NODE_EXISTS)))
  }

  test("CREATE CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, ast.IfExistsThrowError, NoOptions, containsOn = true, ConstraintVersion0))
  }

  test("CREATE CONSTRAINT ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, ast.IfExistsThrowError, NoOptions, containsOn = true, ConstraintVersion0))
  }

  test("CREATE CONSTRAINT ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, ast.IfExistsThrowError, NoOptions, containsOn = true, ConstraintVersion0))
  }

  test("CREATE CONSTRAINT ON ()-[r1:R]-() ASSERT EXISTS r2.prop") {
    assertAst(CreateRelationshipPropertyExistenceConstraint(
        Variable("r1")(1, 26, 25),
        RelTypeName("R")(1, 29, 28),
        Property(Variable("r2")(1, 49, 48), PropertyKeyName("prop")(1, 52, 51))(1, 49, 48),
        None, IfExistsThrowError, NoOptions, containsOn = true, ConstraintVersion0, None)(defaultPos)
    )
  }

  test("CREATE OR REPLACE CONSTRAINT ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, ast.IfExistsReplace, NoOptions, containsOn = true, ConstraintVersion0))
  }

  test("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, ast.IfExistsInvalidSyntax, NoOptions, containsOn = true, ConstraintVersion0))
  }

  test("CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, ast.IfExistsDoNothing, NoOptions, containsOn = true, ConstraintVersion0))
  }

  test("CREATE CONSTRAINT ON ()-[r1:REL]-() ASSERT EXISTS (r2.prop1, r3.prop2)") {
    assertFailsWithException(testName, new Neo4jASTConstructionException(ASTExceptionFactory.onlySinglePropertyAllowed(ConstraintType.REL_EXISTS)))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), Some("my_constraint"), ast.IfExistsThrowError, NoOptions, containsOn = true, ConstraintVersion0))
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), Some("my_constraint"), ast.IfExistsReplace, NoOptions, containsOn = true, ConstraintVersion0))
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), Some("my_constraint"), ast.IfExistsInvalidSyntax, NoOptions, containsOn = true, ConstraintVersion0))
  }

  test("CREATE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), Some("my_constraint"), ast.IfExistsDoNothing, NoOptions, containsOn = true, ConstraintVersion0))
  }

  test("CREATE CONSTRAINT `$my_constraint` ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), Some("$my_constraint"), ast.IfExistsThrowError, NoOptions, containsOn = true, ConstraintVersion0))
  }

  test("CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop) OPTIONS {}") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), Some("my_constraint"), ast.IfExistsThrowError, OptionsMap(Map.empty), containsOn = true, ConstraintVersion0))
  }

  test("CREATE OR REPLACE CONSTRAINT `$my_constraint` ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), Some("$my_constraint"), ast.IfExistsReplace, NoOptions, containsOn = true, ConstraintVersion0))
  }

  test("CREATE OR REPLACE CONSTRAINT `$my_constraint` IF NOT EXISTS ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), Some("$my_constraint"), ast.IfExistsInvalidSyntax, NoOptions, containsOn = true, ConstraintVersion0))
  }

  test("CREATE CONSTRAINT `$my_constraint` IF NOT EXISTS ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), Some("$my_constraint"), ast.IfExistsDoNothing, NoOptions, containsOn = true, ConstraintVersion0))
  }

  // Edge case tests

  test("CREATE CONSTRAINT my_constraint FOR (n:Person) REQUIRE n.prop IS NOT NULL OPTIONS {indexProvider : 'range-1.0'};") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("n"), labelName("Person"), prop("n", "prop"), Some("my_constraint"),
      ast.IfExistsThrowError, OptionsMap(Map("indexProvider" -> literalString("range-1.0"))),
      containsOn = false, constraintVersion = ConstraintVersion2)
    )
  }

  test("CREATE CONSTRAINT FOR (n:Person) REQUIRE n.prop IS NOT NULL; CREATE CONSTRAINT FOR (n:User) REQUIRE n.prop IS UNIQUE") {
    // The test setup does 'fromParser(_.Statements().get(0)', so only the first statement is yielded.
    // The purpose of the test is to make sure the parser does not throw an error on the semicolon, which was an issue before.
    // If we want to test that both statements are parsed, the test framework needs to be extended.
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("n"), labelName("Person"), prop("n", "prop"), None,
      ast.IfExistsThrowError, NoOptions, containsOn = false, constraintVersion = ConstraintVersion2))
  }

  test("CREATE CONSTRAINT FOR FOR (node:Label) REQUIRE (node.prop) IS NODE KEY") {
    assertAst(CreateNodeKeyConstraint(
      varFor("node", (1, 28, 27)),
      LabelName("Label")(1, 33, 32),
      Seq(prop("node", "prop", (1, 49, 48))),
      Some("FOR"), IfExistsThrowError, NoOptions, containsOn = false, ConstraintVersion2)(defaultPos))
  }

  test("CREATE CONSTRAINT FOR FOR (node:Label) REQUIRE (node.prop) IS UNIQUE") {
    assertAst(CreateUniquePropertyConstraint(
      varFor("node", (1, 28, 27)),
      LabelName("Label")(1, 33, 32),
      Seq(prop("node", "prop", (1, 49, 48))),
      Some("FOR"), IfExistsThrowError, NoOptions, containsOn = false, ConstraintVersion2)(defaultPos))
  }

  test("CREATE CONSTRAINT FOR FOR (node:Label) REQUIRE node.prop IS NOT NULL") {
    assertAst(CreateNodePropertyExistenceConstraint(
      varFor("node", (1, 28, 27)),
      labelName("Label",(1, 33, 32)),
      prop("node", "prop", (1, 48, 47)),
      Some("FOR"), IfExistsThrowError, NoOptions, containsOn = false, ConstraintVersion2)(defaultPos))
  }

  test("CREATE CONSTRAINT FOR FOR ()-[r:R]-() REQUIRE (r.prop) IS NOT NULL") {
    assertAst(CreateRelationshipPropertyExistenceConstraint(
      varFor("r", (1, 31, 30)),
      relTypeName("R", (1, 33, 32)),
      prop("r", "prop", (1, 48, 47)),
      Some("FOR"), IfExistsThrowError, NoOptions, containsOn = false, ConstraintVersion2)(defaultPos))
  }

  // Negative tests

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop) IS NOT NULL") {
    failsToParse
  }

  test("CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop) IS NOT NULL") {
    assertFailsWithMessageStart(testName, "Invalid input 'IS': expected \"OPTIONS\" or <EOF> (line 1, column 71 (offset: 70))")
  }

  test("CREATE CONSTRAINT $my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    assertFailsWithMessage(testName, "Invalid input '$': expected \"FOR\", \"IF\", \"ON\" or an identifier (line 1, column 19 (offset: 18))")
  }

  test("CREATE CONSTRAINT FOR (n:Label) REQUIRE (n.prop)") {
    assertFailsWithMessage(testName, "Invalid input '': expected \"IS\" (line 1, column 49 (offset: 48))")
  }

  test("CREATE CONSTRAINT FOR (node:Label) REQUIRE EXISTS (node.prop)") {
    assertFailsWithMessage(testName, "Invalid input '(': expected \".\" (line 1, column 51 (offset: 50))")
  }

  test("CREATE CONSTRAINT FOR ()-[r:R]-() REQUIRE EXISTS (r.prop)") {
     assertFailsWithMessage(testName, "Invalid input '(': expected \".\" (line 1, column 50 (offset: 49))")
  }

  test(s"CREATE CONSTRAINT my_constraint ON ()-[r:R]-() ASSERT r.prop IS NULL") {
    assertFailsWithMessage(testName, "Invalid input 'NULL': expected \"NODE\", \"NOT\" or \"UNIQUE\" (line 1, column 65 (offset: 64))")
  }

  test(s"CREATE CONSTRAINT my_constraint FOR ()-[r:R]-() REQUIRE r.prop IS NULL") {
    assertFailsWithMessage(testName, "Invalid input 'NULL': expected \"NODE\", \"NOT\" or \"UNIQUE\" (line 1, column 67 (offset: 66))")
  }

  test(s"CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT node.prop IS NULL") {
    assertFailsWithMessage(testName, "Invalid input 'NULL': expected \"NODE\", \"NOT\" or \"UNIQUE\" (line 1, column 69 (offset: 68))")
  }

  test(s"CREATE CONSTRAINT my_constraint FOR (node:Label) REQUIRE node.prop IS NULL") {
    assertFailsWithMessage(testName, "Invalid input 'NULL': expected \"NODE\", \"NOT\" or \"UNIQUE\" (line 1, column 71 (offset: 70))")
  }

  // Drop constraint

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    yields(ast.DropNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop"))))
  }

  test("DROP CONSTRAINT ON (node1:Label) ASSERT node2.prop IS NODE KEY") {
    assertAst(
      DropNodeKeyConstraint(Variable("node1")(1, 21, 20),
        LabelName("Label")(1, 27, 26),
        Seq(Property(Variable("node2")(1, 41, 40), PropertyKeyName("prop")(1, 47, 46))(1, 42, 40)),
        None)(defaultPos)
    )
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    yields(ast.DropNodeKeyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2"))))
  }

  test("DROP CONSTRAINT ON ()-[r1:R]-() ASSERT r2.prop IS NODE KEY") {
    assertFailsWithMessageStart(testName, ASTExceptionFactory.relationshipPattternNotAllowed(ConstraintType.NODE_KEY))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE") {
    yields(ast.DropUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop"))))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop) IS UNIQUE") {
    yields(ast.DropUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop"))))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE") {
    yields(ast.DropUniquePropertyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2"))))
  }

  test("DROP CONSTRAINT ON ()-[r1:R]-() ASSERT r2.prop IS UNIQUE") {
    assertFailsWithMessageStart(testName, ASTExceptionFactory.relationshipPattternNotAllowed(ConstraintType.UNIQUE))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.DropNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop")))
  }

  test("DROP CONSTRAINT ON (node1:Label) ASSERT EXISTS node2.prop") {
    assertAst(
      DropNodePropertyExistenceConstraint(Variable("node1")(1, 21, 20),
        LabelName("Label")(1, 27, 26),
        Property(Variable("node2")(1, 48, 47), PropertyKeyName("prop")(1, 54, 53))(1, 48, 47),
        None)(defaultPos)
    )
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields(ast.DropRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop")))
  }

  test("DROP CONSTRAINT ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    yields(ast.DropRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop")))
  }

  test("DROP CONSTRAINT ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields(ast.DropRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop")))
  }

  test("DROP CONSTRAINT ON ()-[r1:R]-() ASSERT EXISTS r2.prop") {
    assertAst(
      DropRelationshipPropertyExistenceConstraint(
        Variable("r1")(1, 24, 23),
        RelTypeName("R")(1, 27, 26),
        Property(Variable("r2")(1, 47, 46), PropertyKeyName("prop")(1, 50, 49))(1, 47, 46),
        None)(defaultPos)
    )
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NOT NULL") {
    assertFailsWithException(testName, new Neo4jASTConstructionException(ASTExceptionFactory.invalidDropCommand))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS NOT NULL") {
    assertFailsWithException(testName, new Neo4jASTConstructionException(ASTExceptionFactory.invalidDropCommand))
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT (r.prop) IS NOT NULL") {
    assertFailsWithException(testName, new Neo4jASTConstructionException(ASTExceptionFactory.invalidDropCommand))
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT r.prop IS NOT NULL") {
    assertFailsWithException(testName, new Neo4jASTConstructionException(ASTExceptionFactory.invalidDropCommand))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.EXISTS) IS NODE KEY") {
    yields(ast.DropNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "EXISTS"))))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.EXISTS) IS UNIQUE") {
    yields(ast.DropUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "EXISTS"))))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT EXISTS (node.EXISTS)") {
    yields(ast.DropNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "EXISTS")))
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.EXISTS)") {
    yields(ast.DropRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "EXISTS")))
  }

  test("DROP CONSTRAINT my_constraint") {
    yields(ast.DropConstraintOnName("my_constraint", ifExists = false))
  }

  test("DROP CONSTRAINT `$my_constraint`") {
    yields(ast.DropConstraintOnName("$my_constraint", ifExists = false))
  }

  test("DROP CONSTRAINT my_constraint IF EXISTS") {
    yields(ast.DropConstraintOnName("my_constraint", ifExists = true))
  }

  test("DROP CONSTRAINT $my_constraint") {
    failsToParse
  }

  test("DROP CONSTRAINT my_constraint IF EXISTS;") {
    yields(ast.DropConstraintOnName("my_constraint", ifExists = true))
  }

  test("DROP CONSTRAINT my_constraint; DROP CONSTRAINT my_constraint2;") {
    // The test setup does 'fromParser(_.Statements().get(0)', so only the first statement is yielded.
    // The purpose of the test is to make sure the parser does not throw an error on the semicolon, which was an issue before.
    // If we want to test that both statements are parsed, the test framework needs to be extended.
    yields(ast.DropConstraintOnName("my_constraint", ifExists = false))
  }

  test("DROP CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    assertFailsWithMessage(testName, "Invalid input 'ON': expected \"IF\" or <EOF> (line 1, column 31 (offset: 30))")
  }
}
