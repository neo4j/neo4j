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
    yields(ast.CreateIndex(labelName("Person"), List(propertyKeyName("name"))))
  }

  test("CREATE INDEX ON :Person(name,age)") {
    yields(ast.CreateIndex(labelName("Person"), List(propertyKeyName("name"), propertyKeyName("age"))))
  }

  test("CREATE INDEX my_index ON :Person(name)") {
    failsToParse
  }

  test("CREATE INDEX my_index ON :Person(name,age)") {
    failsToParse
  }

  // new syntax

  test("CREATE INDEX FOR (n:Person) ON (n.name)") {
    yields(ast.CreateIndexNewSyntax(varFor("n"), labelName("Person"), List(prop("n", "name")), None, IfExistsThrowError))
  }

  test("USE neo4j CREATE INDEX FOR (n:Person) ON (n.name)") {
    yields(ast.CreateIndexNewSyntax(varFor("n"), labelName("Person"), List(prop("n", "name")), None, IfExistsThrowError, Some(use(varFor("neo4j")))))
  }

  test("CREATE INDEX FOR (n:Person) ON (n.name, n.age)") {
    yields(ast.CreateIndexNewSyntax(varFor("n"), labelName("Person"), List(prop("n", "name"), prop("n", "age")), None, IfExistsThrowError))
  }

  test("CREATE INDEX my_index FOR (n:Person) ON (n.name)") {
    yields(ast.CreateIndexNewSyntax(varFor("n"), labelName("Person"), List(prop("n", "name")), Some("my_index"), IfExistsThrowError))
  }

  test("CREATE INDEX my_index FOR (n:Person) ON (n.name, n.age)") {
    yields(ast.CreateIndexNewSyntax(varFor("n"), labelName("Person"), List(prop("n", "name"), prop("n", "age")), Some("my_index"), IfExistsThrowError))
  }

  test("CREATE INDEX `$my_index` FOR (n:Person) ON (n.name)") {
    yields(ast.CreateIndexNewSyntax(varFor("n"), labelName("Person"), List(prop("n", "name")), Some("$my_index"), IfExistsThrowError))
  }

  test("CREATE OR REPLACE INDEX FOR (n:Person) ON (n.name)") {
    yields(ast.CreateIndexNewSyntax(varFor("n"), labelName("Person"), List(prop("n", "name")), None, IfExistsReplace))
  }

  test("CREATE OR REPLACE INDEX my_index FOR (n:Person) ON (n.name, n.age)") {
    yields(ast.CreateIndexNewSyntax(varFor("n"), labelName("Person"), List(prop("n", "name"), prop("n", "age")), Some("my_index"), IfExistsReplace))
  }

  test("CREATE OR REPLACE INDEX IF NOT EXISTS FOR (n:Person) ON (n.name)") {
    yields(ast.CreateIndexNewSyntax(varFor("n"), labelName("Person"), List(prop("n", "name")), None, IfExistsInvalidSyntax))
  }

  test("CREATE OR REPLACE INDEX my_index IF NOT EXISTS FOR (n:Person) ON (n.name)") {
    yields(ast.CreateIndexNewSyntax(varFor("n"), labelName("Person"), List(prop("n", "name")), Some("my_index"), IfExistsInvalidSyntax))
  }

  test("CREATE INDEX IF NOT EXISTS FOR (n:Person) ON (n.name, n.age)") {
    yields(ast.CreateIndexNewSyntax(varFor("n"), labelName("Person"), List(prop("n", "name"), prop("n", "age")), None, IfExistsDoNothing))
  }

  test("CREATE INDEX my_index IF NOT EXISTS FOR (n:Person) ON (n.name)") {
    yields(ast.CreateIndexNewSyntax(varFor("n"), labelName("Person"), List(prop("n", "name")), Some("my_index"), IfExistsDoNothing))
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

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsThrowError))
  }

  test("CREATE OR REPLACE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsReplace))
  }

  test("CREATE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsDoNothing))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), None, IfExistsThrowError))
  }

  test("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), None, IfExistsInvalidSyntax))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsThrowError))
  }

  test("CREATE OR REPLACE CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsReplace))
  }

  test("CREATE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT node.prop IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsDoNothing))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), None, IfExistsThrowError))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), None, IfExistsThrowError))
  }

  test("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), None, IfExistsInvalidSyntax))
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

  test("CREATE CONSTRAINT ON (node:Label) ASSERT node.prop.part IS UNIQUE") {
    failsToParse
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop.part) IS UNIQUE") {
    failsToParse
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS node.prop") {
    failsToParse
  }

  test("CREATE CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS r.prop") {
    failsToParse
  }

  test("USE neo4j CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), IfExistsThrowError, Some(use(varFor("neo4j")))))
  }

  test("USE neo4j CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), IfExistsInvalidSyntax, Some(use(varFor("neo4j")))))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), IfExistsThrowError))
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), IfExistsReplace))
  }

  test("CREATE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    yields(ast.CreateNodeKeyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), IfExistsDoNothing))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT node.prop IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), IfExistsThrowError))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop) IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), IfExistsThrowError))
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT (node.prop) IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"), Seq(prop("node", "prop")), Some("my_constraint"), IfExistsInvalidSyntax))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), IfExistsThrowError))
  }

  test("CREATE OR REPLACE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), IfExistsReplace))
  }

  test("CREATE CONSTRAINT my_constraint IF NOT EXISTS ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE") {
    yields(ast.CreateUniquePropertyConstraint(varFor("node"), labelName("Label"),
      Seq(prop("node", "prop1"), prop("node", "prop2")), Some("my_constraint"), IfExistsDoNothing))
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

  // help method

  private def propertyKeyName(name: String) = {
    expressions.PropertyKeyName(name)(pos)
  }

  private def relTypeName(name: String) = {
    expressions.RelTypeName(name)(pos)
  }
}
