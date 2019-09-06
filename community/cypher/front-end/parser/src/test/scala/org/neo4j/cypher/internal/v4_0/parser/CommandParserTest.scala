/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.cypher.internal.v4_0.parser

import org.neo4j.cypher.internal.v4_0.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.v4_0.{ast, expressions => exp}
import org.parboiled.scala.Rule1

import scala.language.implicitConversions

class CommandParserTest
  extends ParserAstTest[ast.Command]
    with Command
    with AstConstructionTestSupport {

  implicit val parser: Rule1[ast.Command] = Command

  // Create index

  test("CREATE INDEX ON :Person(name)") {
    yields(ast.CreateIndex(exp.LabelName("Person")(_), List(exp.PropertyKeyName("name")(pos)), None))
  }

  test("CREATE INDEX ON :Person(name,age)") {
    yields(ast.CreateIndex(exp.LabelName("Person")(_), List(exp.PropertyKeyName("name")(pos), exp.PropertyKeyName("age")(pos)), None))
  }

  test("CREATE INDEX my_index ON :Person(name)") {
    yields(ast.CreateIndex(exp.LabelName("Person")(_), List(exp.PropertyKeyName("name")(pos)), Some("my_index")))
  }

  test("CREATE INDEX my_index ON :Person(name,age)") {
    yields(ast.CreateIndex(exp.LabelName("Person")(_), List(exp.PropertyKeyName("name")(pos), exp.PropertyKeyName("age")(pos)), Some("my_index")))
  }

  test("CREATE INDEX `$my_index` ON :Person(name)") {
    yields(ast.CreateIndex(exp.LabelName("Person")(_), List(exp.PropertyKeyName("name")(pos)), Some("$my_index")))
  }

  test("CREATE INDEX $my_index ON :Person(name)") {
    failsToParse
  }

  // Drop index

  test("DROP INDEX ON :Person(name)") {
    yields(ast.DropIndex(exp.LabelName("Person")(_), List(exp.PropertyKeyName("name")(pos))))
  }

  test("DROP INDEX ON :Person(name, age)") {
    yields(ast.DropIndex(exp.LabelName("Person")(_), List(exp.PropertyKeyName("name")(pos), exp.PropertyKeyName("age")(pos))))
  }

  test("DROP INDEX my_index") {
    yields(ast.DropIndexOnName("my_index"))
  }

  test("DROP INDEX `$my_index`") {
    yields(ast.DropIndexOnName("$my_index"))
  }

  test("DROP INDEX $my_index") {
    failsToParse
  }

  test("DROP INDEX my_index ON :Person(name)") {
    failsToParse
  }

  // Create constraint

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    val variable = exp.Variable("node")(_)
    yields(ast.CreateNodeKeyConstraint(variable, exp.LabelName("Label")(_), Seq(exp.Property(variable, exp.PropertyKeyName("prop")(_))(_)), None))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    val variable = exp.Variable("node")(_)
    yields(ast.CreateNodeKeyConstraint(variable, exp.LabelName("Label")(_),
      Seq(exp.Property(variable, exp.PropertyKeyName("prop1")(_))(_), exp.Property(variable, exp.PropertyKeyName("prop2")(_))(_)), None))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE") {
    val variable = exp.Variable("node")(_)
    yields(ast.CreateUniquePropertyConstraint(variable, exp.LabelName("Label")(_), Seq(exp.Property(variable, exp.PropertyKeyName("prop")(_))(_)), None))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop) IS UNIQUE") {
    val variable = exp.Variable("node")(_)
    yields(ast.CreateUniquePropertyConstraint(variable, exp.LabelName("Label")(_), Seq(exp.Property(variable, exp.PropertyKeyName("prop")(_))(_)), None))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE") {
    val variable = exp.Variable("node")(_)
    yields(ast.CreateUniquePropertyConstraint(variable, exp.LabelName("Label")(_),
      Seq(exp.Property(variable, exp.PropertyKeyName("prop1")(_))(_), exp.Property(variable, exp.PropertyKeyName("prop2")(_))(_)), None))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)") {
    val variable = exp.Variable("node")(_)
    yields(ast.CreateNodePropertyExistenceConstraint(variable, exp.LabelName("Label")(_), exp.Property(variable, exp.PropertyKeyName("prop")(_))(_), None))
  }

  test("CREATE CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    val variable = exp.Variable("r")(_)
    yields(ast.CreateRelationshipPropertyExistenceConstraint(variable, exp.RelTypeName("R")(_), exp.Property(variable, exp.PropertyKeyName("prop")(_))(_), None))
  }

  test("CREATE CONSTRAINT ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    val variable = exp.Variable("r")(_)
    yields(ast.CreateRelationshipPropertyExistenceConstraint(variable, exp.RelTypeName("R")(_), exp.Property(variable, exp.PropertyKeyName("prop")(_))(_), None))
  }

  test("CREATE CONSTRAINT ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    val variable = exp.Variable("r")(_)
    yields(ast.CreateRelationshipPropertyExistenceConstraint(variable, exp.RelTypeName("R")(_), exp.Property(variable, exp.PropertyKeyName("prop")(_))(_), None))
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT node.prop IS NODE KEY") {
    failsToParse
  }

  test("CREATE CONSTRAINT ON (node:Label) ASSERT EXISTS node.prop") {
    failsToParse
  }

  test("CREATE CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS r.prop") {
    failsToParse
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    val variable = exp.Variable("node")(_)
    yields(ast.CreateNodeKeyConstraint(variable, exp.LabelName("Label")(_), Seq(exp.Property(variable, exp.PropertyKeyName("prop")(_))(_)), Some("my_constraint")))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    val variable = exp.Variable("node")(_)
    yields(ast.CreateNodeKeyConstraint(variable, exp.LabelName("Label")(_),
      Seq(exp.Property(variable, exp.PropertyKeyName("prop1")(_))(_), exp.Property(variable, exp.PropertyKeyName("prop2")(_))(_)), Some("my_constraint")))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT node.prop IS UNIQUE") {
    val variable = exp.Variable("node")(_)
    yields(ast.CreateUniquePropertyConstraint(variable, exp.LabelName("Label")(_), Seq(exp.Property(variable, exp.PropertyKeyName("prop")(_))(_)), Some("my_constraint")))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop) IS UNIQUE") {
    val variable = exp.Variable("node")(_)
    yields(ast.CreateUniquePropertyConstraint(variable, exp.LabelName("Label")(_), Seq(exp.Property(variable, exp.PropertyKeyName("prop")(_))(_)), Some("my_constraint")))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE") {
    val variable = exp.Variable("node")(_)
    yields(ast.CreateUniquePropertyConstraint(variable, exp.LabelName("Label")(_),
      Seq(exp.Property(variable, exp.PropertyKeyName("prop1")(_))(_), exp.Property(variable, exp.PropertyKeyName("prop2")(_))(_)), Some("my_constraint")))
  }

  test("CREATE CONSTRAINT my_constraint ON (node:Label) ASSERT EXISTS (node.prop)") {
    val variable = exp.Variable("node")(_)
    yields(ast.CreateNodePropertyExistenceConstraint(variable, exp.LabelName("Label")(_), exp.Property(variable, exp.PropertyKeyName("prop")(_))(_), Some("my_constraint")))
  }

  test("CREATE CONSTRAINT `$my_constraint` ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    val variable = exp.Variable("r")(_)
    yields(ast.CreateRelationshipPropertyExistenceConstraint(variable, exp.RelTypeName("R")(_), exp.Property(variable, exp.PropertyKeyName("prop")(_))(_), Some("$my_constraint")))
  }

  test("CREATE CONSTRAINT $my_constraint ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    failsToParse
  }

  // Drop constraint

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop) IS NODE KEY") {
    val variable = exp.Variable("node")(_)
    yields(ast.DropNodeKeyConstraint(variable, exp.LabelName("Label")(_), Seq(exp.Property(variable, exp.PropertyKeyName("prop")(_))(_))))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    val variable = exp.Variable("node")(_)
    yields(ast.DropNodeKeyConstraint(variable, exp.LabelName("Label")(_),
      Seq(exp.Property(variable, exp.PropertyKeyName("prop1")(_))(_), exp.Property(variable, exp.PropertyKeyName("prop2")(_))(_))))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT node.prop IS UNIQUE") {
    val variable = exp.Variable("node")(_)
    yields(ast.DropUniquePropertyConstraint(variable, exp.LabelName("Label")(_), Seq(exp.Property(variable, exp.PropertyKeyName("prop")(_))(_))))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop) IS UNIQUE") {
    val variable = exp.Variable("node")(_)
    yields(ast.DropUniquePropertyConstraint(variable, exp.LabelName("Label")(_), Seq(exp.Property(variable, exp.PropertyKeyName("prop")(_))(_))))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT (node.prop1,node.prop2) IS UNIQUE") {
    val variable = exp.Variable("node")(_)
    yields(ast.DropUniquePropertyConstraint(variable, exp.LabelName("Label")(_),
      Seq(exp.Property(variable, exp.PropertyKeyName("prop1")(_))(_), exp.Property(variable, exp.PropertyKeyName("prop2")(_))(_))))
  }

  test("DROP CONSTRAINT ON (node:Label) ASSERT EXISTS (node.prop)") {
    val variable = exp.Variable("node")(_)
    yields(ast.DropNodePropertyExistenceConstraint(variable, exp.LabelName("Label")(_), exp.Property(variable, exp.PropertyKeyName("prop")(_))(_)))
  }

  test("DROP CONSTRAINT ON ()-[r:R]-() ASSERT EXISTS (r.prop)") {
    val variable = exp.Variable("r")(_)
    yields(ast.DropRelationshipPropertyExistenceConstraint(variable, exp.RelTypeName("R")(_), exp.Property(variable, exp.PropertyKeyName("prop")(_))(_)))
  }

  test("DROP CONSTRAINT ON ()-[r:R]->() ASSERT EXISTS (r.prop)") {
    val variable = exp.Variable("r")(_)
    yields(ast.DropRelationshipPropertyExistenceConstraint(variable, exp.RelTypeName("R")(_), exp.Property(variable, exp.PropertyKeyName("prop")(_))(_)))
  }

  test("DROP CONSTRAINT ON ()<-[r:R]-() ASSERT EXISTS (r.prop)") {
    val variable = exp.Variable("r")(_)
    yields(ast.DropRelationshipPropertyExistenceConstraint(variable, exp.RelTypeName("R")(_), exp.Property(variable, exp.PropertyKeyName("prop")(_))(_)))
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
    yields(ast.DropConstraintOnName("my_constraint"))
  }

  test("DROP CONSTRAINT `$my_constraint`") {
    yields(ast.DropConstraintOnName("$my_constraint"))
  }

  test("DROP CONSTRAINT $my_constraint") {
    failsToParse
  }

  test("DROP CONSTRAINT my_constraint ON (node:Label) ASSERT (node.prop1,node.prop2) IS NODE KEY") {
    failsToParse
  }

}
