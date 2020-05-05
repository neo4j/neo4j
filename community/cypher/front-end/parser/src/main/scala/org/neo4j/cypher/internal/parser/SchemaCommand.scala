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
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.Variable
import org.parboiled.scala.Parser
import org.parboiled.scala.Rule1
import org.parboiled.scala.Rule3
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
      | CreateIndex
      | CreateIndexNewSyntax
      | DropUniqueConstraint
      | DropUniqueCompositeConstraint
      | DropNodeKeyConstraint
      | DropNodePropertyExistenceConstraint
      | DropRelationshipPropertyExistenceConstraint
      | DropConstraintOnName
      | DropIndex
      | DropIndexOnName) ~~> ((use, command) => command.withGraph(use))
  )

  def VariablePropertyExpression: Rule1[Property] = rule("single property expression from variable") {
    Variable ~ PropertyLookup
  }

  def VariablePropertyExpressions: Rule1[Seq[Property]] = rule("multiple property expressions from variable") {
    oneOrMore(WS ~ VariablePropertyExpression, separator = CommaSep)
  }

  def CreateIndex: Rule1[ast.CreateIndex] = rule {
    group(keyword("CREATE INDEX ON") ~~ NodeLabel ~~ "(" ~~ PropertyKeyNames ~~ ")") ~~>> (ast.CreateIndex(_, _))
  }

  def CreateIndexNewSyntax: Rule1[ast.CreateIndexNewSyntax] = rule {
    group(keyword("CREATE INDEX FOR") ~~ IndexPatternSyntax) ~~>>
      ((variable, label, properties) => ast.CreateIndexNewSyntax(variable, label, properties.toList, None)) |
    group(keyword("CREATE INDEX") ~~ SymbolicNameString ~~ keyword("FOR") ~~ IndexPatternSyntax) ~~>>
      ((name, variable, label, properties) => ast.CreateIndexNewSyntax(variable, label, properties.toList, Some(name)))
  }

  def IndexPatternSyntax: Rule3[Variable, LabelName, Seq[Property]] = rule {
    group("(" ~~ Variable ~~ NodeLabel ~~ ")" ~~ keyword("ON") ~~ "(" ~~ VariablePropertyExpressions ~~ ")")
  }

  def DropIndex: Rule1[ast.DropIndex] = rule {
    group(keyword("DROP INDEX ON") ~~ NodeLabel ~~ "(" ~~ PropertyKeyNames ~~ ")") ~~>> (ast.DropIndex(_, _))
  }

  def DropIndexOnName: Rule1[ast.DropIndexOnName] = rule {
    group(keyword("DROP INDEX") ~~ SymbolicNameString) ~~>> (ast.DropIndexOnName(_))
  }

  def CreateUniqueConstraint: Rule1[ast.CreateUniquePropertyConstraint] = rule {
    group(keyword("CREATE CONSTRAINT") ~~ UniqueConstraintSyntax) ~~>>
      ((variable, label, property) => ast.CreateUniquePropertyConstraint(variable, label, Seq(property), None)) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ UniqueConstraintSyntax) ~~>>
      ((name, variable, label, property) => ast.CreateUniquePropertyConstraint(variable, label, Seq(property), Some(name)))
  }

  def CreateUniqueCompositeConstraint: Rule1[ast.CreateUniquePropertyConstraint] = rule {
    group(keyword("CREATE CONSTRAINT") ~~ UniqueCompositeConstraintSyntax) ~~>> (ast.CreateUniquePropertyConstraint(_, _, _, None)) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ UniqueCompositeConstraintSyntax) ~~>>
      ((name, variable, labelName, properties) => ast.CreateUniquePropertyConstraint(variable, labelName, properties, Some(name)))
  }

  def CreateNodeKeyConstraint: Rule1[ast.CreateNodeKeyConstraint] = rule {
    group(keyword("CREATE CONSTRAINT") ~~ NodeKeyConstraintSyntax) ~~>> (ast.CreateNodeKeyConstraint(_, _, _, None)) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ NodeKeyConstraintSyntax) ~~>>
      ((name, variable, labelName, property) => ast.CreateNodeKeyConstraint(variable, labelName, property, Some(name)))
  }

  def CreateNodePropertyExistenceConstraint: Rule1[ast.CreateNodePropertyExistenceConstraint] = rule {
    group(keyword("CREATE CONSTRAINT") ~~ NodePropertyExistenceConstraintSyntax) ~~>> (ast.CreateNodePropertyExistenceConstraint(_, _, _, None)) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ NodePropertyExistenceConstraintSyntax) ~~>>
      ((name, variable, labelName, property) => ast.CreateNodePropertyExistenceConstraint(variable, labelName, property, Some(name)))
  }

  def CreateRelationshipPropertyExistenceConstraint: Rule1[ast.CreateRelationshipPropertyExistenceConstraint] = rule {
    group(keyword("CREATE CONSTRAINT") ~~ RelationshipPropertyExistenceConstraintSyntax) ~~>> (ast.CreateRelationshipPropertyExistenceConstraint(_, _, _, None)) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ RelationshipPropertyExistenceConstraintSyntax) ~~>>
      ((name, variable, relTypeName, property) => ast.CreateRelationshipPropertyExistenceConstraint(variable, relTypeName, property, Some(name)))
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
    group(keyword("DROP CONSTRAINT") ~~ SymbolicNameString) ~~>> (ast.DropConstraintOnName(_))
  }

  private def NodeKeyConstraintSyntax: Rule3[Variable, LabelName, Seq[Property]] = keyword("ON") ~~ "(" ~~ Variable ~~ NodeLabel ~~ ")" ~~
    keyword("ASSERT") ~~ "(" ~~ VariablePropertyExpressions ~~ ")" ~~ keyword("IS NODE KEY")

  private def UniqueConstraintSyntax: Rule3[Variable, LabelName, Property] = keyword("ON") ~~ "(" ~~ Variable ~~ NodeLabel ~~ ")" ~~
    keyword("ASSERT") ~~ VariablePropertyExpression ~~ keyword("IS UNIQUE")

  private def UniqueCompositeConstraintSyntax: Rule3[Variable, LabelName, Seq[Property]] = keyword("ON") ~~ "(" ~~ Variable ~~ NodeLabel ~~ ")" ~~
    keyword("ASSERT") ~~ "(" ~~ VariablePropertyExpressions ~~ ")" ~~ keyword("IS UNIQUE")

  private def NodePropertyExistenceConstraintSyntax = keyword("ON") ~~ "(" ~~ Variable ~~ NodeLabel ~~ ")" ~~
    keyword("ASSERT EXISTS") ~~ "(" ~~ VariablePropertyExpression ~~ ")"

  private def RelationshipPropertyExistenceConstraintSyntax = keyword("ON") ~~ RelationshipPatternSyntax ~~
    keyword("ASSERT EXISTS") ~~ "(" ~~ VariablePropertyExpression ~~ ")"

  private def RelationshipPatternSyntax = rule(
    ("()-[" ~~ Variable~~ RelType ~~ "]-()")
      | ("()-[" ~~ Variable~~ RelType ~~ "]->()")
      | ("()<-[" ~~ Variable~~ RelType ~~ "]-()")
  )
}
