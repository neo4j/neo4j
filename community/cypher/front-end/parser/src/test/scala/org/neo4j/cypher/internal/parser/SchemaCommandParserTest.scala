/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.parser

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.expressions
import org.parboiled.scala.Rule1

class SchemaCommandParserTest
  extends ParserAstTest[ast.SchemaCommand]
    with SchemaCommand
    with AstConstructionTestSupport {

  implicit val parser: Rule1[ast.SchemaCommand] = SchemaCommand

  // Create index

  test("CREATE INDEX ON :Person(name)") {
    yields(ast.CreateIndexOldSyntax(labelName("Person"), List(propertyKeyName("name"))))
  }

  test("CREATE INDEX ON :Person(name,age)") {
    yields(ast.CreateIndexOldSyntax(labelName("Person"), List(propertyKeyName("name"), propertyKeyName("age"))))
  }

  test("CREATE INDEX my_index ON :Person(name)") {
    failsToParse
  }

  test("CREATE INDEX my_index ON :Person(name,age)") {
    failsToParse
  }

  // new syntax

  test("CREATE INDEX FOR (n:Person) ON (n.name)") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name")), None, IfExistsThrowError, Map.empty))
  }

  test("USE neo4j CREATE INDEX FOR (n:Person) ON (n.name)") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name")), None, IfExistsThrowError, Map.empty, Some(use(varFor("neo4j")))))
  }

  test("CREATE INDEX FOR (n:Person) ON (n.name, n.age)") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name"), prop("n", "age")), None, IfExistsThrowError, Map.empty))
  }

  test("CREATE BTREE INDEX my_index FOR (n:Person) ON (n.name)") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name")), Some("my_index"), IfExistsThrowError, Map.empty))
  }

  test("CREATE INDEX my_index FOR (n:Person) ON (n.name, n.age)") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name"), prop("n", "age")), Some("my_index"), IfExistsThrowError, Map.empty))
  }

  test("CREATE INDEX `$my_index` FOR (n:Person) ON (n.name)") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name")), Some("$my_index"), IfExistsThrowError, Map.empty))
  }

  test("CREATE OR REPLACE INDEX FOR (n:Person) ON (n.name)") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name")), None, IfExistsReplace, Map.empty))
  }

  test("CREATE OR REPLACE INDEX my_index FOR (n:Person) ON (n.name, n.age)") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name"), prop("n", "age")), Some("my_index"), IfExistsReplace, Map.empty))
  }

  test("CREATE OR REPLACE INDEX IF NOT EXISTS FOR (n:Person) ON (n.name)") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name")), None, IfExistsInvalidSyntax, Map.empty))
  }

  test("CREATE OR REPLACE INDEX my_index IF NOT EXISTS FOR (n:Person) ON (n.name)") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name")), Some("my_index"), IfExistsInvalidSyntax, Map.empty))
  }

  test("CREATE INDEX IF NOT EXISTS FOR (n:Person) ON (n.name, n.age)") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name"), prop("n", "age")), None, IfExistsDoNothing, Map.empty))
  }

  test("CREATE INDEX my_index IF NOT EXISTS FOR (n:Person) ON (n.name)") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name")), Some("my_index"), IfExistsDoNothing, Map.empty))
  }

  test("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider : 'native-btree-1.0'}") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name")),
      None, IfExistsThrowError, Map("indexProvider" -> literalString("native-btree-1.0"))))
  }

  test("CREATE BTREE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexProvider : 'lucene+native-3.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name")), None, IfExistsThrowError,
      Map("indexProvider" -> literalString("lucene+native-3.0"),
          "indexConfig"   -> mapOf(
            "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
            "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
          )
      )
    ))
  }

  test("CREATE BTREE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }, indexProvider : 'lucene+native-3.0'}") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name")), None, IfExistsThrowError,
      Map("indexProvider" -> literalString("lucene+native-3.0"),
        "indexConfig"   -> mapOf(
          "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
          "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
        )
      )
    ))
  }

  test("CREATE BTREE INDEX FOR (n:Person) ON (n.name) OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0] }}") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name")), None, IfExistsThrowError,
      Map("indexConfig" -> mapOf(
          "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
          "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
        ))
    ))
  }

  test("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS {nonValidOption : 42}") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name")), None, IfExistsThrowError, Map("nonValidOption" -> literalInt(42))))
  }

  test("CREATE INDEX my_index FOR (n:Person) ON (n.name) OPTIONS {}") {
    yields(ast.CreateIndex(varFor("n"), labelName("Person"), List(prop("n", "name")), Some("my_index"), IfExistsThrowError, Map.empty))
  }

  test("CREATE INDEX $my_index FOR (n:Person) ON (n.name)") {
    failsToParse
  }

  test("CREATE INDEX FOR n:Person ON (n.name)") {
    failsToParse
  }

  test("CREATE INDEX FOR (n:Person) ON n.name") {
    failsToParse
  }

  test("CREATE INDEX FOR (n:Person) ON (n.name) {indexProvider : 'native-btree-1.0'}") {
    failsToParse
  }

  test("CREATE INDEX FOR (n:Person) ON (n.name) OPTIONS") {
    failsToParse
  }

  // Drop index

  test("DROP INDEX ON :Person(name)") {
    yields(ast.DropIndex(labelName("Person"), List(propertyKeyName("name"))))
  }

  test("DROP INDEX ON :Person(name, age)") {
    yields(ast.DropIndex(labelName("Person"), List(propertyKeyName("name"), propertyKeyName("age"))))
  }

  test("DROP INDEX my_index") {
    yields(ast.DropIndexOnName("my_index", ifExists = false))
  }

  test("DROP INDEX `$my_index`") {
    yields(ast.DropIndexOnName("$my_index", ifExists = false))
  }

  test("DROP INDEX my_index IF EXISTS") {
    yields(ast.DropIndexOnName("my_index", ifExists = true))
  }

  test("DROP INDEX $my_index") {
    failsToParse
  }

  test("DROP INDEX my_index ON :Person(name)") {
    failsToParse
  }

  // Create constraint

  // Without name

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsThrowError, Map.empty))
  }

  test("CREATE OR REPLACE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsReplace, Map.empty))
  }

  test("CREATE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsDoNothing, Map.empty))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), None, IfExistsThrowError, Map.empty))
  }

  test("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), None, IfExistsInvalidSyntax, Map.empty))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY OPTIONS {indexProvider : 'native-btree-1.0'}") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")),
      None, IfExistsThrowError, Map("indexProvider" -> literalString("native-btree-1.0"))))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY OPTIONS {indexProvider : 'lucene+native-3.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsThrowError,
      Map("indexProvider" -> literalString("lucene+native-3.0"),
          "indexConfig"   -> mapOf(
            "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
            "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
          )
      )
    ))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0] }}") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsThrowError,
      Map("indexConfig" -> mapOf(
        "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
        "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
      ))
    ))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY OPTIONS {nonValidOption : 42}") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsThrowError, Map("nonValidOption" -> literalInt(42))))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY OPTIONS {}") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsThrowError, Map.empty))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsThrowError, Map.empty))
  }

  test("CREATE OR REPLACE CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsReplace, Map.empty))
  }

  test("CREATE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT node.prop IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsDoNothing, Map.empty))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsThrowError, Map.empty))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), None, IfExistsThrowError, Map.empty))
  }

  test("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), None, IfExistsInvalidSyntax, Map.empty))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS UNIQUE OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}, indexProvider : 'native-btree-1.0'}") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsThrowError,
      Map("indexProvider" -> literalString("native-btree-1.0"),
          "indexConfig"   -> mapOf(
            "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
            "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
          )
      )
    ))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), None, IfExistsThrowError))
  }

  test("CREATE OR REPLACE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), None, IfExistsReplace))
  }

  test("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), None, IfExistsInvalidSyntax))
  }

  test("CREATE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), None, IfExistsDoNothing))
  }

  test("CREATE CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, IfExistsThrowError))
  }

  test("CREATE CONSTRAINT ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, IfExistsThrowError))
  }

  test("CREATE CONSTRAINT ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, IfExistsThrowError))
  }

  test("CREATE OR REPLACE CONSTRAINT ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, IfExistsReplace))
  }

  test("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, IfExistsInvalidSyntax))
  }

  test("CREATE CONSTRAINT IF NOT EXISTS ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), None, IfExistsDoNothing))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS NODE KEY") {
    failsToParse
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY {indexProvider : 'native-btree-1.0'}") {
    failsToParse
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY OPTIONS") {
    failsToParse
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT node.prop.part IS UNIQUE") {
    failsToParse
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop.part) IS UNIQUE") {
    failsToParse
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS UNIQUE {indexProvider : 'native-btree-1.0'}") {
    failsToParse
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1, node.prop2) IS UNIQUE OPTIONS") {
    failsToParse
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS node.prop") {
    failsToParse
  }

  test("CREATE CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS r.prop") {
    failsToParse
  }

  // With name

  test("USE neo4j CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), IfExistsThrowError, Map.empty, Some(use(varFor("neo4j")))))
  }

  test("USE neo4j CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), IfExistsInvalidSyntax, Map.empty, Some(use(varFor("neo4j")))))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), IfExistsThrowError, Map.empty))
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), IfExistsReplace, Map.empty))
  }

  test("CREATE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), IfExistsDoNothing, Map.empty))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop) IS NODE KEY OPTIONS {indexProvider : 'native-btree-1.0', indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0]}}") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), IfExistsThrowError,
      Map("indexProvider" -> literalString("native-btree-1.0"),
          "indexConfig"   -> mapOf(
            "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
            "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
          )
      )
    ))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT node.prop IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), IfExistsThrowError, Map.empty))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop) IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), IfExistsThrowError, Map.empty))
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT (node.prop) IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), IfExistsInvalidSyntax, Map.empty))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), IfExistsThrowError, Map.empty))
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), IfExistsReplace, Map.empty))
  }

  test("CREATE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), IfExistsDoNothing, Map.empty))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop) IS UNIQUE OPTIONS {indexProvider : 'native-btree-1.0'}") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")),
      Some("my_constraint"), IfExistsThrowError, Map("indexProvider" -> literalString("native-btree-1.0"))))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop) IS UNIQUE OPTIONS {indexProvider : 'lucene+native-3.0', indexConfig : {`spatial.cartesian.max`: [100.0,100.0], `spatial.cartesian.min`: [-100.0,-100.0] }}") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), IfExistsThrowError,
      Map("indexProvider" -> literalString("lucene+native-3.0"),
          "indexConfig"   -> mapOf(
            "spatial.cartesian.max" -> listOf(literalFloat(100.0), literalFloat(100.0)),
            "spatial.cartesian.min" -> listOf(literalFloat(-100.0), literalFloat(-100.0))
          )
      )
    ))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop) IS UNIQUE OPTIONS {indexConfig : {`spatial.wgs-84.max`: [60.0,60.0], `spatial.wgs-84.min`: [-40.0,-40.0] }}") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), IfExistsThrowError,
      Map("indexConfig" -> mapOf(
        "spatial.wgs-84.max" -> listOf(literalFloat(60.0), literalFloat(60.0)),
        "spatial.wgs-84.min" -> listOf(literalFloat(-40.0), literalFloat(-40.0))
      ))
    ))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop) IS UNIQUE OPTIONS {nonValidOption : 42}") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")),
      Some("my_constraint"), IfExistsThrowError, Map("nonValidOption" -> literalInt(42))))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE OPTIONS {}") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop1"), prop("node", "prop2")),
      Some("my_constraint"), IfExistsThrowError, Map.empty))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), Some("my_constraint"), IfExistsThrowError))
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), Some("my_constraint"), IfExistsReplace))
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), Some("my_constraint"), IfExistsInvalidSyntax))
  }

  test("CREATE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.CreateNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop"), Some("my_constraint"), IfExistsDoNothing))
  }

  test("CREATE CONSTRAINT `$my_constraint` ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), Some("$my_constraint"), IfExistsThrowError))
  }

  test("CREATE OR REPLACE CONSTRAINT `$my_constraint` ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), Some("$my_constraint"), IfExistsReplace))
  }

  test("CREATE OR REPLACE CONSTRAINT `$my_constraint` IF NOT EXISTS ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), Some("$my_constraint"), IfExistsInvalidSyntax))
  }

  test("CREATE CONSTRAINT `$my_constraint` IF NOT EXISTS ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    yields(ast.CreateRelationshipPropertyExistenceConstraint(varFor("r"), relTypeName("R"), prop("r", "prop"), Some("$my_constraint"), IfExistsDoNothing))
  }

  test("CREATE CONSTRAINT $my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    failsToParse
  }

  // Drop constraint

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    yields(ast.DropNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop"))))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    yields(ast.DropNodeKeyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2"))))
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

  test("DROP CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)") {
    yields(ast.DropNodePropertyExistenceConstraint(varFor("node"), labelName("Label"), prop("node", "prop")))
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

  test("DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS NODE KEY") {
    failsToParse
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT EXISTS node.prop") {
    failsToParse
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS r.prop") {
    failsToParse
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

  test("DROP CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    failsToParse
  }

  // help methods

  private def propertyKeyName(name: String) = {
    expressions.PropertyKeyName(name)(pos)
  }

  //noinspection SameParameterValue
  private def relTypeName(name: String) = {
    expressions.RelTypeName(name)(pos)
  }
}
