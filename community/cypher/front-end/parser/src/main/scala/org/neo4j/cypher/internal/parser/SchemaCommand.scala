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
import org.neo4j.cypher.internal.ast.AllConstraints
import org.neo4j.cypher.internal.ast.ExistsConstraints
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.IfExistsInvalidSyntax
import org.neo4j.cypher.internal.ast.IfExistsReplace
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.ShowConstraintType
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.parboiled.scala.Parser
import org.parboiled.scala.Rule0
import org.parboiled.scala.Rule1
import org.parboiled.scala.Rule3
import org.parboiled.scala.Rule4
import org.parboiled.scala.group

trait SchemaCommand extends Parser
  with Expressions
  with Literals
  with Base
  with ProcedureCalls
  with GraphSelection {

  def SchemaCommand: Rule1[ast.SchemaCommand] = rule(
    optional(UseGraph) ~~ (
      CreateUniqueConstraint
      | CreateUniqueCompositeConstraint
      | CreateNodeKeyConstraint
      | CreateNodePropertyExistenceConstraint
      | CreateRelationshipPropertyExistenceConstraint
      | CreateIndexOldSyntax
      | CreateIndex
      | DropUniqueConstraint
      | DropUniqueCompositeConstraint
      | DropNodeKeyConstraint
      | DropNodePropertyExistenceConstraint
      | DropRelationshipPropertyExistenceConstraint
      | DropConstraintOnName
      | DropIndex
      | DropIndexOnName
      | ShowIndexes
      | ShowConstraints) ~~> ((use, command) => command.withGraph(use))
  )

  def VariablePropertyExpression: Rule1[Property] = rule("single property expression from variable") {
    Variable ~ PropertyLookup
  }

  def VariablePropertyExpressions: Rule1[Seq[Property]] = rule("multiple property expressions from variable") {
    oneOrMore(WS ~ VariablePropertyExpression, separator = CommaSep)
  }

  def options: Rule1[Map[String, Expression]] = rule {
    keyword("OPTIONS") ~~ group(ch('{') ~~ zeroOrMore(SymbolicNameString ~~ ch(':') ~~ Expression, separator = CommaSep) ~~ ch('}')) ~~>> (l => _ => l.toMap)
  }

  private def SchemaOutput: Rule1[Boolean] = rule("type of show output") {
    keyword("VERBOSE") ~~ optional(keyword("OUTPUT")) ~~~> (_ => true) |
      optional(keyword("BRIEF") ~~ optional(keyword("OUTPUT"))) ~~~> (_ => false)
  }

  def CreateIndexOldSyntax: Rule1[ast.CreateIndexOldSyntax] = rule {
    group(keyword("CREATE INDEX ON") ~~ NodeLabel ~~ "(" ~~ PropertyKeyNames ~~ ")") ~~>> (ast.CreateIndexOldSyntax(_, _))
  }

  def CreateIndex: Rule1[ast.CreateIndex] = rule {
    // without name
    group((keyword("CREATE OR REPLACE BTREE INDEX IF NOT EXISTS FOR") | keyword("CREATE OR REPLACE INDEX IF NOT EXISTS FOR")) ~~ IndexPatternSyntax) ~~>>
      ((variable, label, properties, options) => ast.CreateIndex(variable, label, properties.toList, None, IfExistsInvalidSyntax, options)) |
    group((keyword("CREATE OR REPLACE BTREE INDEX FOR") | keyword("CREATE OR REPLACE INDEX FOR")) ~~ IndexPatternSyntax) ~~>>
      ((variable, label, properties, options) => ast.CreateIndex(variable, label, properties.toList, None, IfExistsReplace, options)) |
    group((keyword("CREATE BTREE INDEX IF NOT EXISTS FOR") | keyword("CREATE INDEX IF NOT EXISTS FOR")) ~~ IndexPatternSyntax) ~~>>
      ((variable, label, properties, options) => ast.CreateIndex(variable, label, properties.toList, None, IfExistsDoNothing, options)) |
    group( (keyword("CREATE BTREE INDEX FOR") | keyword("CREATE INDEX FOR")) ~~ IndexPatternSyntax) ~~>>
      ((variable, label, properties, options) => ast.CreateIndex(variable, label, properties.toList, None, IfExistsThrowError, options)) |
    // with name
    group((keyword("CREATE OR REPLACE BTREE INDEX") | keyword("CREATE OR REPLACE INDEX")) ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS FOR") ~~ IndexPatternSyntax) ~~>>
      ((name, variable, label, properties, options) => ast.CreateIndex(variable, label, properties.toList, Some(name), IfExistsInvalidSyntax, options)) |
    group((keyword("CREATE OR REPLACE BTREE INDEX") | keyword("CREATE OR REPLACE INDEX")) ~~ SymbolicNameString ~~ keyword("FOR") ~~ IndexPatternSyntax) ~~>>
      ((name, variable, label, properties, options) => ast.CreateIndex(variable, label, properties.toList, Some(name), IfExistsReplace, options)) |
    group((keyword("CREATE BTREE INDEX") | keyword("CREATE INDEX")) ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS FOR") ~~ IndexPatternSyntax) ~~>>
      ((name, variable, label, properties, options) => ast.CreateIndex(variable, label, properties.toList, Some(name), IfExistsDoNothing, options)) |
    group((keyword("CREATE BTREE INDEX") | keyword("CREATE INDEX"))~~ SymbolicNameString ~~ keyword("FOR") ~~ IndexPatternSyntax) ~~>>
      ((name, variable, label, properties, options) => ast.CreateIndex(variable, label, properties.toList, Some(name), IfExistsThrowError, options))
  }

  def IndexPatternSyntax: Rule4[Variable, LabelName, Seq[Property], Map[String, Expression]] = rule {
    group("(" ~~ Variable ~~ NodeLabel ~~ ")" ~~ keyword("ON") ~~ "(" ~~ VariablePropertyExpressions ~~ ")" ~~ options) |
    group("(" ~~ Variable ~~ NodeLabel ~~ ")" ~~ keyword("ON") ~~ "(" ~~ VariablePropertyExpressions ~~ ")") ~> (_ => Map.empty)
  }

  def DropIndex: Rule1[ast.DropIndex] = rule {
    group(keyword("DROP INDEX ON") ~~ NodeLabel ~~ "(" ~~ PropertyKeyNames ~~ ")") ~~>> (ast.DropIndex(_, _))
  }

  def DropIndexOnName: Rule1[ast.DropIndexOnName] = rule {
    group(keyword("DROP INDEX") ~~ SymbolicNameString ~~ keyword("IF EXISTS")) ~~>> (ast.DropIndexOnName(_, ifExists = true)) |
    group(keyword("DROP INDEX") ~~ SymbolicNameString) ~~>> (ast.DropIndexOnName(_, ifExists = false))
  }

  def ShowIndexes: Rule1[ast.ShowIndexes] = rule("SHOW INDEXES") {
    keyword("SHOW") ~~ IndexType ~~ IndexKeyword ~~ SchemaOutput ~~>>
      ((all, verbose) => ast.ShowIndexes(all, verbose))
  }

  private def IndexType: Rule1[Boolean] = rule("type of indexes") {
    keyword("BTREE") ~~~> (_ => false) |
      optional(keyword("ALL")) ~~~> (_ => true)
  }

  def IndexKeyword: Rule0 = keyword("INDEXES") | keyword("INDEX")

  def CreateUniqueConstraint: Rule1[ast.CreateUniquePropertyConstraint] = rule {
    // without name
    group(keyword("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS") ~~ UniqueConstraintWithOptionsSyntax) ~~>>
      ((variable, label, property, options) => ast.CreateUniquePropertyConstraint(variable, label, Seq(property), None, IfExistsInvalidSyntax, options)) |
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ UniqueConstraintWithOptionsSyntax) ~~>>
      ((variable, label, property, options) => ast.CreateUniquePropertyConstraint(variable, label, Seq(property), None, IfExistsReplace, options)) |
    group(keyword("CREATE CONSTRAINT IF NOT EXISTS") ~~ UniqueConstraintWithOptionsSyntax) ~~>>
      ((variable, label, property, options) => ast.CreateUniquePropertyConstraint(variable, label, Seq(property), None, IfExistsDoNothing, options)) |
    group(keyword("CREATE CONSTRAINT") ~~ UniqueConstraintWithOptionsSyntax) ~~>>
      ((variable, label, property, options) => ast.CreateUniquePropertyConstraint(variable, label, Seq(property), None, IfExistsThrowError, options)) |
    // with name
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS") ~~ UniqueConstraintWithOptionsSyntax) ~~>>
      ((name, variable, label, property, options) => ast.CreateUniquePropertyConstraint(variable, label, Seq(property), Some(name), IfExistsInvalidSyntax, options)) |
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ SymbolicNameString ~~ UniqueConstraintWithOptionsSyntax) ~~>>
      ((name, variable, label, property, options) => ast.CreateUniquePropertyConstraint(variable, label, Seq(property), Some(name), IfExistsReplace, options)) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS") ~~ UniqueConstraintWithOptionsSyntax) ~~>>
      ((name, variable, label, property, options) => ast.CreateUniquePropertyConstraint(variable, label, Seq(property), Some(name), IfExistsDoNothing, options)) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ UniqueConstraintWithOptionsSyntax) ~~>>
      ((name, variable, label, property, options) => ast.CreateUniquePropertyConstraint(variable, label, Seq(property), Some(name), IfExistsThrowError, options))
  }

  def CreateUniqueCompositeConstraint: Rule1[ast.CreateUniquePropertyConstraint] = rule {
    // without name
    group(keyword("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS") ~~ UniqueCompositeConstraintWithOptionsSyntax) ~~>> (ast.CreateUniquePropertyConstraint(_, _, _, None, IfExistsInvalidSyntax, _)) |
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ UniqueCompositeConstraintWithOptionsSyntax) ~~>> (ast.CreateUniquePropertyConstraint(_, _, _, None, IfExistsReplace, _)) |
    group(keyword("CREATE CONSTRAINT IF NOT EXISTS") ~~ UniqueCompositeConstraintWithOptionsSyntax) ~~>> (ast.CreateUniquePropertyConstraint(_, _, _, None, IfExistsDoNothing, _)) |
    group(keyword("CREATE CONSTRAINT") ~~ UniqueCompositeConstraintWithOptionsSyntax) ~~>> (ast.CreateUniquePropertyConstraint(_, _, _, None, IfExistsThrowError, _)) |
    // with name
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS") ~~ UniqueCompositeConstraintWithOptionsSyntax) ~~>>
      ((name, variable, labelName, properties, options) => ast.CreateUniquePropertyConstraint(variable, labelName, properties, Some(name), IfExistsInvalidSyntax, options)) |
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ SymbolicNameString ~~ UniqueCompositeConstraintWithOptionsSyntax) ~~>>
      ((name, variable, labelName, properties, options) => ast.CreateUniquePropertyConstraint(variable, labelName, properties, Some(name), IfExistsReplace, options)) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS") ~~ UniqueCompositeConstraintWithOptionsSyntax) ~~>>
      ((name, variable, labelName, properties, options) => ast.CreateUniquePropertyConstraint(variable, labelName, properties, Some(name), IfExistsDoNothing, options)) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ UniqueCompositeConstraintWithOptionsSyntax) ~~>>
      ((name, variable, labelName, properties, options) => ast.CreateUniquePropertyConstraint(variable, labelName, properties, Some(name), IfExistsThrowError, options))
  }

  def CreateNodeKeyConstraint: Rule1[ast.CreateNodeKeyConstraint] = rule {
    // without name
    group(keyword("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS") ~~ NodeKeyConstraintWithOptionsSyntax) ~~>> (ast.CreateNodeKeyConstraint(_, _, _, None, IfExistsInvalidSyntax, _)) |
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ NodeKeyConstraintWithOptionsSyntax) ~~>> (ast.CreateNodeKeyConstraint(_, _, _, None, IfExistsReplace, _)) |
    group(keyword("CREATE CONSTRAINT IF NOT EXISTS") ~~ NodeKeyConstraintWithOptionsSyntax) ~~>> (ast.CreateNodeKeyConstraint(_, _, _, None, IfExistsDoNothing, _)) |
    group(keyword("CREATE CONSTRAINT") ~~ NodeKeyConstraintWithOptionsSyntax) ~~>> (ast.CreateNodeKeyConstraint(_, _, _, None, IfExistsThrowError, _)) |
    // with name
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS") ~~ NodeKeyConstraintWithOptionsSyntax) ~~>>
      ((name, variable, labelName, property, options) => ast.CreateNodeKeyConstraint(variable, labelName, property, Some(name), IfExistsInvalidSyntax, options)) |
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ SymbolicNameString ~~ NodeKeyConstraintWithOptionsSyntax) ~~>>
      ((name, variable, labelName, property, options) => ast.CreateNodeKeyConstraint(variable, labelName, property, Some(name), IfExistsReplace, options)) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS") ~~ NodeKeyConstraintWithOptionsSyntax) ~~>>
      ((name, variable, labelName, property, options) => ast.CreateNodeKeyConstraint(variable, labelName, property, Some(name), IfExistsDoNothing, options)) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ NodeKeyConstraintWithOptionsSyntax) ~~>>
      ((name, variable, labelName, property, options) => ast.CreateNodeKeyConstraint(variable, labelName, property, Some(name), IfExistsThrowError, options))
  }

  def CreateNodePropertyExistenceConstraint: Rule1[ast.CreateNodePropertyExistenceConstraint] = rule {
    // without name
    group(keyword("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS") ~~ NodePropertyExistenceConstraintWithOptionsSyntax) ~~>> (ast.CreateNodePropertyExistenceConstraint(_, _, _, None, IfExistsInvalidSyntax, _)) |
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ NodePropertyExistenceConstraintWithOptionsSyntax) ~~>> (ast.CreateNodePropertyExistenceConstraint(_, _, _, None, IfExistsReplace, _)) |
    group(keyword("CREATE CONSTRAINT IF NOT EXISTS") ~~ NodePropertyExistenceConstraintWithOptionsSyntax) ~~>> (ast.CreateNodePropertyExistenceConstraint(_, _, _, None, IfExistsDoNothing, _)) |
    group(keyword("CREATE CONSTRAINT") ~~ NodePropertyExistenceConstraintWithOptionsSyntax) ~~>> (ast.CreateNodePropertyExistenceConstraint(_, _, _, None, IfExistsThrowError, _)) |
    // with name
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS") ~~ NodePropertyExistenceConstraintWithOptionsSyntax) ~~>>
      ((name, variable, labelName, property, options) => ast.CreateNodePropertyExistenceConstraint(variable, labelName, property, Some(name), IfExistsInvalidSyntax, options)) |
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ SymbolicNameString ~~ NodePropertyExistenceConstraintWithOptionsSyntax) ~~>>
      ((name, variable, labelName, property, options) => ast.CreateNodePropertyExistenceConstraint(variable, labelName, property, Some(name), IfExistsReplace, options)) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS") ~~ NodePropertyExistenceConstraintWithOptionsSyntax) ~~>>
      ((name, variable, labelName, property, options) => ast.CreateNodePropertyExistenceConstraint(variable, labelName, property, Some(name), IfExistsDoNothing, options)) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ NodePropertyExistenceConstraintWithOptionsSyntax) ~~>>
      ((name, variable, labelName, property, options) => ast.CreateNodePropertyExistenceConstraint(variable, labelName, property, Some(name), IfExistsThrowError, options))
  }

  def CreateRelationshipPropertyExistenceConstraint: Rule1[ast.CreateRelationshipPropertyExistenceConstraint] = rule {
    // without name
    group(keyword("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS") ~~ RelationshipPropertyExistenceConstraintWithOptionsSyntax) ~~>> (ast.CreateRelationshipPropertyExistenceConstraint(_, _, _, None, IfExistsInvalidSyntax, _)) |
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ RelationshipPropertyExistenceConstraintWithOptionsSyntax) ~~>> (ast.CreateRelationshipPropertyExistenceConstraint(_, _, _, None, IfExistsReplace, _)) |
    group(keyword("CREATE CONSTRAINT IF NOT EXISTS") ~~ RelationshipPropertyExistenceConstraintWithOptionsSyntax) ~~>> (ast.CreateRelationshipPropertyExistenceConstraint(_, _, _, None, IfExistsDoNothing, _)) |
    group(keyword("CREATE CONSTRAINT") ~~ RelationshipPropertyExistenceConstraintWithOptionsSyntax) ~~>> (ast.CreateRelationshipPropertyExistenceConstraint(_, _, _, None, IfExistsThrowError, _)) |
    // with name
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS") ~~ RelationshipPropertyExistenceConstraintWithOptionsSyntax) ~~>>
      ((name, variable, relTypeName, property, options) => ast.CreateRelationshipPropertyExistenceConstraint(variable, relTypeName, property, Some(name), IfExistsInvalidSyntax, options)) |
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ SymbolicNameString ~~ RelationshipPropertyExistenceConstraintWithOptionsSyntax) ~~>>
      ((name, variable, relTypeName, property, options) => ast.CreateRelationshipPropertyExistenceConstraint(variable, relTypeName, property, Some(name), IfExistsReplace, options)) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS") ~~ RelationshipPropertyExistenceConstraintWithOptionsSyntax) ~~>>
      ((name, variable, relTypeName, property, options) => ast.CreateRelationshipPropertyExistenceConstraint(variable, relTypeName, property, Some(name), IfExistsDoNothing, options)) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ RelationshipPropertyExistenceConstraintWithOptionsSyntax) ~~>>
      ((name, variable, relTypeName, property, options) => ast.CreateRelationshipPropertyExistenceConstraint(variable, relTypeName, property, Some(name), IfExistsThrowError, options))
  }

  def DropUniqueConstraint: Rule1[ast.DropUniquePropertyConstraint] = rule {
    group(keyword("DROP CONSTRAINT") ~~ UniqueConstraintSyntax) ~~>>
      ((variable, label, property) => ast.DropUniquePropertyConstraint(variable, label, Seq(property)))
  }

  def DropUniqueCompositeConstraint: Rule1[ast.DropUniquePropertyConstraint] = rule {
    group(keyword("DROP CONSTRAINT") ~~ UniqueCompositeConstraintSyntax) ~~>> (ast.DropUniquePropertyConstraint(_, _, _))
  }

  def DropNodeKeyConstraint: Rule1[ast.DropNodeKeyConstraint] = rule {
    group(keyword("DROP CONSTRAINT") ~~ NodeKeyConstraintSyntax) ~~>> (ast.DropNodeKeyConstraint(_, _, _))
  }

  def DropNodePropertyExistenceConstraint: Rule1[ast.DropNodePropertyExistenceConstraint] = rule {
    group(keyword("DROP CONSTRAINT") ~~ NodePropertyExistenceConstraintSyntax) ~~>> (ast.DropNodePropertyExistenceConstraint(_, _, _))
  }

  def DropRelationshipPropertyExistenceConstraint: Rule1[ast.DropRelationshipPropertyExistenceConstraint] = rule {
    group(keyword("DROP CONSTRAINT") ~~ RelationshipPropertyExistenceConstraintSyntax) ~~>> (ast.DropRelationshipPropertyExistenceConstraint(_, _, _))
  }

  def DropConstraintOnName: Rule1[ast.DropConstraintOnName] = rule {
    group(keyword("DROP CONSTRAINT") ~~ SymbolicNameString ~~ keyword("IF EXISTS")) ~~>> (ast.DropConstraintOnName(_, ifExists = true)) |
    group(keyword("DROP CONSTRAINT") ~~ SymbolicNameString) ~~>> (ast.DropConstraintOnName(_, ifExists = false))
  }

  def ShowConstraints: Rule1[ast.ShowConstraints] = rule("SHOW CONSTRAINTS") {
    keyword("SHOW") ~~ ConstraintType ~~ ConstraintKeyword ~~ SchemaOutput ~~>>
      ((constraintType, verbose) => ast.ShowConstraints(constraintType, verbose))
  }

  private def ConstraintType: Rule1[ShowConstraintType] = rule("type of constraints") {
    keyword("UNIQUE") ~~~> (_ => UniqueConstraints) |
    keyword("NODE KEY") ~~~> (_ => NodeKeyConstraints) |
    keyword("NODE") ~~ ExistsKeyword ~~~> (_ => NodeExistsConstraints) |
    keyword("RELATIONSHIP") ~~ ExistsKeyword ~~~> (_ => RelExistsConstraints) |
    ExistsKeyword ~~~> (_ => ExistsConstraints) |
    optional(keyword("ALL")) ~~~> (_ => AllConstraints)
  }

  def ConstraintKeyword: Rule0 = keyword("CONSTRAINTS") | keyword("CONSTRAINT")

  private def ExistsKeyword: Rule0 = keyword("EXISTS") | keyword("EXIST")

  private def NodeKeyConstraintSyntax: Rule3[Variable, LabelName, Seq[Property]] = keyword("ON") ~~ "(" ~~ Variable ~~ NodeLabel ~~ ")" ~~
    keyword("ASSERT") ~~ "(" ~~ VariablePropertyExpressions ~~ ")" ~~ keyword("IS NODE KEY")

  private def NodeKeyConstraintWithOptionsSyntax: Rule4[Variable, LabelName, Seq[Property], Map[String, Expression]] = rule {
    NodeKeyConstraintSyntax ~~ options |
    NodeKeyConstraintSyntax ~> (_ => Map.empty)
  }

  private def UniqueConstraintSyntax: Rule3[Variable, LabelName, Property] = keyword("ON") ~~ "(" ~~ Variable ~~ NodeLabel ~~ ")" ~~
    keyword("ASSERT") ~~ VariablePropertyExpression ~~ keyword("IS UNIQUE")

  private def UniqueConstraintWithOptionsSyntax: Rule4[Variable, LabelName, Property, Map[String, Expression]] = rule {
    UniqueConstraintSyntax ~~ options |
    UniqueConstraintSyntax ~> (_ => Map.empty)
  }

  private def UniqueCompositeConstraintSyntax: Rule3[Variable, LabelName, Seq[Property]] = keyword("ON") ~~ "(" ~~ Variable ~~ NodeLabel ~~ ")" ~~
    keyword("ASSERT") ~~ "(" ~~ VariablePropertyExpressions ~~ ")" ~~ keyword("IS UNIQUE")

  private def UniqueCompositeConstraintWithOptionsSyntax: Rule4[Variable, LabelName, Seq[Property], Map[String, Expression]] = rule {
    UniqueCompositeConstraintSyntax ~~ options |
    UniqueCompositeConstraintSyntax ~> (_ => Map.empty)
  }

  private def NodePropertyExistenceConstraintSyntax: Rule3[Variable, LabelName, Property] = keyword("ON") ~~ "(" ~~ Variable ~~ NodeLabel ~~ ")" ~~
    keyword("ASSERT EXISTS") ~~ "(" ~~ VariablePropertyExpression ~~ ")"

  private def NodePropertyExistenceConstraintWithOptionsSyntax: Rule4[Variable, LabelName, Property, Option[Map[String, Expression]]] = rule {
    NodePropertyExistenceConstraintSyntax ~~ options ~~> (o => Some(o)) |
    NodePropertyExistenceConstraintSyntax ~> (_ => None)
  }

  private def RelationshipPropertyExistenceConstraintSyntax: Rule3[Variable, RelTypeName, Property] = keyword("ON") ~~ RelationshipPatternSyntax ~~
    keyword("ASSERT EXISTS") ~~ "(" ~~ VariablePropertyExpression ~~ ")"

  private def RelationshipPropertyExistenceConstraintWithOptionsSyntax: Rule4[Variable, RelTypeName, Property, Option[Map[String, Expression]]] = rule {
    RelationshipPropertyExistenceConstraintSyntax ~~ options ~~> (o => Some(o)) |
    RelationshipPropertyExistenceConstraintSyntax ~> (_ => None)
  }

  private def RelationshipPatternSyntax = rule(
    ("()-[" ~~ Variable~~ RelType ~~ "]-()")
      | ("()-[" ~~ Variable~~ RelType ~~ "]->()")
      | ("()<-[" ~~ Variable~~ RelType ~~ "]-()")
  )
}
