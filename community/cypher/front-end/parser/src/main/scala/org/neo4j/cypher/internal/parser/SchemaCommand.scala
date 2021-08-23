/*
 * Copyright (c) "Neo4j"
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
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.LabelName
import org.neo4j.cypher.internal.expressions.Property
import org.neo4j.cypher.internal.expressions.RelTypeName
import org.neo4j.cypher.internal.expressions.Variable
import org.neo4j.cypher.internal.util.ConstraintVersion.CONSTRAINT_VERSION_0
import org.neo4j.cypher.internal.util.ConstraintVersion.CONSTRAINT_VERSION_1
import org.neo4j.cypher.internal.util.ConstraintVersion.CONSTRAINT_VERSION_2
import org.parboiled.scala.Parser
import org.parboiled.scala.Rule1
import org.parboiled.scala.Rule2
import org.parboiled.scala.Rule3
import org.parboiled.scala.Rule4
import org.parboiled.scala.group

trait SchemaCommand extends Parser
  with Expressions
  with Literals
  with Base
  with GraphSelection
  with CommandHelper {

  def SchemaCommand: Rule1[ast.SchemaCommand] = rule(
    optional(UseGraph) ~~ (
      CreateConstraint
      | CreateIndexOldSyntax
      | CreateIndex
      | DropUniqueConstraint
      | DropUniqueCompositeConstraint
      | DropNodeKeyConstraint
      | DropNodePropertyExistenceConstraint
      | DropRelationshipPropertyExistenceConstraint
      | DropConstraintOnName
      | DropIndex
      | DropIndexOnName) ~~> ((use, command) => command.withGraph(use))
  )

  private def VariablePropertyExpression: Rule1[Property] = rule("single property expression from variable") {
    Variable ~ PropertyLookup
  }

  private def VariablePropertyExpressions: Rule1[Seq[Property]] = rule("multiple property expressions from variable") {
    oneOrMore(WS ~ VariablePropertyExpression, separator = CommaSep)
  }

  private def RelationshipPatternSyntax: Rule2[Variable, RelTypeName] = rule(
    ("()-[" ~~ Variable ~~ RelType ~~ "]-()")
      | ("()-[" ~~ Variable ~~ RelType ~~ "]->()")
      | ("()<-[" ~~ Variable ~~ RelType ~~ "]-()")
  )

  private def NodeIndexPatternSyntax: Rule4[Variable, LabelName, Seq[Property], Options] = rule {
    group("(" ~~ Variable ~~ NodeLabel ~~ ")" ~~ keyword("ON") ~~ "(" ~~ VariablePropertyExpressions ~~ ")" ~~ optionsMapOrParameter) |
      group("(" ~~ Variable ~~ NodeLabel ~~ ")" ~~ keyword("ON") ~~ "(" ~~ VariablePropertyExpressions ~~ ")") ~> (_ => NoOptions)
  }

  private def RelationshipIndexPatternSyntax: Rule4[Variable, RelTypeName, Seq[Property], Options] = rule {
    group(RelationshipPatternSyntax ~~ keyword("ON") ~~ "(" ~~ VariablePropertyExpressions ~~ ")" ~~ optionsMapOrParameter) |
      group(RelationshipPatternSyntax ~~ keyword("ON") ~~ "(" ~~ VariablePropertyExpressions ~~ ")") ~> (_ => NoOptions)
  }


  // INDEX commands

  private def CreateIndexOldSyntax: Rule1[ast.CreateIndexOldSyntax] = rule {
    group(keyword("CREATE INDEX ON") ~~ NodeLabel ~~ "(" ~~ PropertyKeyNames ~~ ")") ~~>> (ast.CreateIndexOldSyntax(_, _))
  }

  private def CreateIndex: Rule1[ast.CreateIndex] = rule {
    CreateLookupIndex | CreateFulltextIndex | CreateTextIndex | CreateBtreeIndex
  }

  private def CreateBtreeIndex: Rule1[ast.CreateIndex] = rule {
    group(CreateBtreeIndexStart ~~ RelationshipIndexPatternSyntax) ~~>>
      ((name, ifExistsDo, variable, relType, properties, options) => ast.CreateBtreeRelationshipIndex(variable, relType, properties.toList, name, ifExistsDo, options)) |
    group(CreateBtreeIndexStart ~~ NodeIndexPatternSyntax) ~~>>
      ((name, ifExistsDo, variable, label, properties, options) => ast.CreateBtreeNodeIndex(variable, label, properties.toList, name, ifExistsDo, options))
  }

  private def CreateBtreeIndexStart: Rule2[Option[String], ast.IfExistsDo] = rule {
    // without name
    group(keyword("CREATE OR REPLACE BTREE INDEX IF NOT EXISTS FOR") | keyword("CREATE OR REPLACE INDEX IF NOT EXISTS FOR")) ~~~> (_ => None) ~> (_ => ast.IfExistsInvalidSyntax) |
    group(keyword("CREATE OR REPLACE BTREE INDEX FOR") | keyword("CREATE OR REPLACE INDEX FOR")) ~~~> (_ => None) ~> (_ => ast.IfExistsReplace) |
    group(keyword("CREATE BTREE INDEX IF NOT EXISTS FOR") | keyword("CREATE INDEX IF NOT EXISTS FOR")) ~~~> (_ => None) ~> (_ => ast.IfExistsDoNothing) |
    group(keyword("CREATE BTREE INDEX FOR") | keyword("CREATE INDEX FOR")) ~~~> (_ => None) ~> (_ => ast.IfExistsThrowError) |
    // with name
    group((keyword("CREATE OR REPLACE BTREE INDEX") | keyword("CREATE OR REPLACE INDEX")) ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsInvalidSyntax) |
    group((keyword("CREATE OR REPLACE BTREE INDEX") | keyword("CREATE OR REPLACE INDEX")) ~~ SymbolicNameString ~~ keyword("FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsReplace) |
    group((keyword("CREATE BTREE INDEX") | keyword("CREATE INDEX")) ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsDoNothing) |
    group((keyword("CREATE BTREE INDEX") | keyword("CREATE INDEX")) ~~ SymbolicNameString ~~ keyword("FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsThrowError)
  }

  private def CreateLookupIndex: Rule1[ast.CreateIndex] = rule {
    group(CreateLookupIndexStart ~~ LookupRelationshipIndexPatternSyntax) ~~>>
      ((name, ifExistsDo, variable, function, options) => ast.CreateLookupIndex(variable, isNodeIndex = false, function, name, ifExistsDo, options)) |
    group(CreateLookupIndexStart ~~ LookupNodeIndexPatternSyntax) ~~>>
      ((name, ifExistsDo, variable, function, options) => ast.CreateLookupIndex(variable, isNodeIndex = true, function, name, ifExistsDo, options))
  }

  private def CreateLookupIndexStart: Rule2[Option[String], ast.IfExistsDo] = rule {
    // without name
    keyword("CREATE OR REPLACE LOOKUP INDEX IF NOT EXISTS FOR") ~~~> (_ => None) ~> (_ => ast.IfExistsInvalidSyntax) |
    keyword("CREATE OR REPLACE LOOKUP INDEX FOR") ~~~> (_ => None) ~> (_ => ast.IfExistsReplace) |
    keyword("CREATE LOOKUP INDEX IF NOT EXISTS FOR") ~~~> (_ => None) ~> (_ => ast.IfExistsDoNothing) |
    keyword("CREATE LOOKUP INDEX FOR") ~~~> (_ => None) ~> (_ => ast.IfExistsThrowError) |
    // with name
    group(keyword("CREATE OR REPLACE LOOKUP INDEX") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsInvalidSyntax) |
    group(keyword("CREATE OR REPLACE LOOKUP INDEX") ~~ SymbolicNameString ~~ keyword("FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsReplace) |
    group(keyword("CREATE LOOKUP INDEX") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsDoNothing) |
    group(keyword("CREATE LOOKUP INDEX") ~~ SymbolicNameString ~~ keyword("FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsThrowError)
  }

  private def LookupNodeIndexPatternSyntax: Rule3[Variable, expressions.FunctionInvocation, Options] = rule {
    group("(" ~~ Variable ~~ ")" ~~ keyword("ON EACH") ~~ lookupIndexFunctions ~~ optionsMapOrParameter) |
    group("(" ~~ Variable ~~ ")" ~~ keyword("ON EACH") ~~ lookupIndexFunctions) ~> (_ => NoOptions)
  }

  private def LookupRelationshipIndexPatternSyntax: Rule3[Variable, expressions.FunctionInvocation, Options] = rule {
    group(LookupRelationshipPatternSyntax ~~ keyword("ON") ~~ optional(keyword("EACH")) ~~ lookupIndexFunctions ~~ optionsMapOrParameter) |
    group(LookupRelationshipPatternSyntax ~~ keyword("ON") ~~ optional(keyword("EACH")) ~~ lookupIndexFunctions) ~> (_ => NoOptions)
  }

  private def LookupRelationshipPatternSyntax: Rule1[Variable] = rule(
    ("()-[" ~~ Variable ~~ "]-()")
      | ("()-[" ~~ Variable ~~ "]->()")
      | ("()<-[" ~~ Variable ~~ "]-()")
  )

  private def lookupIndexFunctions: Rule1[expressions.FunctionInvocation] = rule("a function for lookup index creation") {
    group(FunctionName ~~ "(" ~~ Variable ~~ ")") ~~>> ((fName, variable) => expressions.FunctionInvocation(fName, distinct = false, IndexedSeq(variable)))
  }

  private def CreateFulltextIndex: Rule1[ast.CreateIndex] = rule {
    group(CreateFulltextIndexStart ~~ FulltextRelationshipIndexPatternSyntax) ~~>>
      ((name, ifExistsDo, variable, relTypes, properties, options) => ast.CreateFulltextRelationshipIndex(variable, relTypes, properties, name, ifExistsDo, options)) |
    group(CreateFulltextIndexStart ~~ FulltextNodeIndexPatternSyntax) ~~>>
      ((name, ifExistsDo, variable, labels, properties, options) => ast.CreateFulltextNodeIndex(variable, labels, properties, name, ifExistsDo, options))
  }

  private def CreateFulltextIndexStart: Rule2[Option[String], ast.IfExistsDo] = rule {
    // without name
    keyword("CREATE OR REPLACE FULLTEXT INDEX IF NOT EXISTS FOR") ~~~> (_ => None) ~> (_ => ast.IfExistsInvalidSyntax) |
      keyword("CREATE OR REPLACE FULLTEXT INDEX FOR") ~~~> (_ => None) ~> (_ => ast.IfExistsReplace) |
      keyword("CREATE FULLTEXT INDEX IF NOT EXISTS FOR") ~~~> (_ => None) ~> (_ => ast.IfExistsDoNothing) |
      keyword("CREATE FULLTEXT INDEX FOR") ~~~> (_ => None) ~> (_ => ast.IfExistsThrowError) |
      // with name
      group(keyword("CREATE OR REPLACE FULLTEXT INDEX") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsInvalidSyntax) |
      group(keyword("CREATE OR REPLACE FULLTEXT INDEX") ~~ SymbolicNameString ~~ keyword("FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsReplace) |
      group(keyword("CREATE FULLTEXT INDEX") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsDoNothing) |
      group(keyword("CREATE FULLTEXT INDEX") ~~ SymbolicNameString ~~ keyword("FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsThrowError)
  }

  private def FulltextNodeIndexPatternSyntax: Rule4[Variable, List[LabelName], List[Property], Options] = rule {
    group("(" ~~ Variable ~~ FulltextNodeLabels ~~ ")" ~~ keyword("ON") ~~ FulltextPropertyProjection ~~ optionsMapOrParameter) |
    group("(" ~~ Variable ~~ FulltextNodeLabels ~~ ")" ~~ keyword("ON") ~~ FulltextPropertyProjection) ~> (_ => NoOptions)
  }

  private def FulltextNodeLabels: Rule1[List[LabelName]] = rule(
    group(NodeLabel ~~ operator("|") ~~ oneOrMore(LabelName, operator("|"))) ~~>> ((firstLabel, labels) => _ => firstLabel +: labels) |
    NodeLabel ~~>> (label => _ => List(label))
  )

  private def FulltextRelationshipIndexPatternSyntax: Rule4[Variable, List[RelTypeName], List[Property], Options] = rule {
    group(FulltextRelationshipPatternSyntax ~~ keyword("ON") ~~ FulltextPropertyProjection ~~ optionsMapOrParameter) |
      group(FulltextRelationshipPatternSyntax ~~ keyword("ON") ~~ FulltextPropertyProjection) ~> (_ => NoOptions)
  }

  private def FulltextRelationshipPatternSyntax: Rule2[Variable, List[RelTypeName]] = rule(
    group("()-[" ~~ Variable ~~ FulltextRelTypes ~~ "]-()") |
    group("()-[" ~~ Variable ~~ FulltextRelTypes ~~ "]->()") |
    group("()<-[" ~~ Variable ~~ FulltextRelTypes ~~ "]-()")
  )

  private def FulltextRelTypes: Rule1[List[RelTypeName]] = rule(
    group(RelType ~~ operator("|") ~~ oneOrMore(RelTypeName, operator("|"))) ~~>> ((firstType, types) => _ => firstType +: types) |
      RelType ~~>> (relType => _ => List(relType))
  )

  private def FulltextPropertyProjection: Rule1[List[Property]] = rule(
    keyword("EACH") ~~ "[" ~~ VariablePropertyExpressions ~~ "]" ~~> (s => s.toList)
  )

  private def CreateTextIndex: Rule1[ast.CreateIndex] = rule {
    group(CreateTextIndexStart ~~ RelationshipIndexPatternSyntax) ~~>>
      ((name, ifExistsDo, variable, relType, properties, options) => ast.CreateTextRelationshipIndex(variable, relType, properties.toList, name, ifExistsDo, options)) |
    group(CreateTextIndexStart ~~ NodeIndexPatternSyntax) ~~>>
      ((name, ifExistsDo, variable, label, properties, options) => ast.CreateTextNodeIndex(variable, label, properties.toList, name, ifExistsDo, options))
  }

  private def CreateTextIndexStart: Rule2[Option[String], ast.IfExistsDo] = rule {
    // without name
    keyword("CREATE OR REPLACE TEXT INDEX IF NOT EXISTS FOR") ~~~> (_ => None) ~> (_ => ast.IfExistsInvalidSyntax) |
    keyword("CREATE OR REPLACE TEXT INDEX FOR") ~~~> (_ => None) ~> (_ => ast.IfExistsReplace) |
    keyword("CREATE TEXT INDEX IF NOT EXISTS FOR") ~~~> (_ => None) ~> (_ => ast.IfExistsDoNothing) |
    keyword("CREATE TEXT INDEX FOR") ~~~> (_ => None) ~> (_ => ast.IfExistsThrowError) |
    // with name
    group(keyword("CREATE OR REPLACE TEXT INDEX") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsInvalidSyntax) |
    group(keyword("CREATE OR REPLACE TEXT INDEX") ~~ SymbolicNameString ~~ keyword("FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsReplace) |
    group(keyword("CREATE TEXT INDEX") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsDoNothing) |
    group(keyword("CREATE TEXT INDEX") ~~ SymbolicNameString ~~ keyword("FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsThrowError)
  }

  private def DropIndex: Rule1[ast.DropIndex] = rule {
    group(keyword("DROP INDEX ON") ~~ NodeLabel ~~ "(" ~~ PropertyKeyNames ~~ ")") ~~>> (ast.DropIndex(_, _))
  }

  private def DropIndexOnName: Rule1[ast.DropIndexOnName] = rule {
    group(keyword("DROP INDEX") ~~ SymbolicNameString ~~ keyword("IF EXISTS")) ~~>> (ast.DropIndexOnName(_, ifExists = true)) |
    group(keyword("DROP INDEX") ~~ SymbolicNameString) ~~>> (ast.DropIndexOnName(_, ifExists = false))
  }

  // CONSTRAINT commands

  private def CreateConstraint: Rule1[ast.SchemaCommand] = rule {
    val constraintStart = CreateConstraintStart
    group(constraintStart ~~ UniqueConstraintWithOptionsSyntax) ~~>>
      ((name, ifExistsDo, containsOn, variable, label, property, options) => ast.CreateUniquePropertyConstraint(variable, label, Seq(property), name, ifExistsDo, options, containsOn, CONSTRAINT_VERSION_0)) |
    group(constraintStart ~~ UniqueConstraintWithOptionsSyntax_2) ~~>>
      ((name, ifExistsDo, containsOn, variable, label, property, options) => ast.CreateUniquePropertyConstraint(variable, label, Seq(property), name, ifExistsDo, options, containsOn, CONSTRAINT_VERSION_2)) |
    group(constraintStart ~~ UniqueCompositeConstraintWithOptionsSyntax) ~~>>
      ((name, ifExistsDo, containsOn, variable, labelName, properties, options) => ast.CreateUniquePropertyConstraint(variable, labelName, properties, name, ifExistsDo, options, containsOn, CONSTRAINT_VERSION_0)) |
    group(constraintStart ~~ UniqueCompositeConstraintWithOptionsSyntax_2) ~~>>
      ((name, ifExistsDo, containsOn, variable, labelName, properties, options) => ast.CreateUniquePropertyConstraint(variable, labelName, properties, name, ifExistsDo, options, containsOn, CONSTRAINT_VERSION_2)) |
    group(constraintStart ~~ NodeKeyConstraintWithOptionsSyntax) ~~>>
      ((name, ifExistsDo, containsOn, variable, labelName, property, options) => ast.CreateNodeKeyConstraint(variable, labelName, property, name, ifExistsDo, options, containsOn, CONSTRAINT_VERSION_0)) |
    group(constraintStart ~~ NodeKeyConstraintWithOptionsSyntax_2) ~~>>
      ((name, ifExistsDo, containsOn, variable, labelName, property, options) => ast.CreateNodeKeyConstraint(variable, labelName, property, name, ifExistsDo, options, containsOn, CONSTRAINT_VERSION_2)) |
    group(constraintStart ~~ NodePropertyExistenceConstraintWithOptionsSyntax) ~~>>
      ((name, ifExistsDo, containsOn, variable, labelName, property, options) => ast.CreateNodePropertyExistenceConstraint(variable, labelName, property, name, ifExistsDo, options, containsOn, CONSTRAINT_VERSION_0)) |
    group(constraintStart ~~ NodePropertyExistenceConstraintWithOptionsSyntax_1) ~~>>
      ((name, ifExistsDo, containsOn, variable, labelName, property, options) => ast.CreateNodePropertyExistenceConstraint(variable, labelName, property, name, ifExistsDo, options, containsOn, CONSTRAINT_VERSION_1)) |
    group(constraintStart ~~ NodePropertyExistenceConstraintWithOptionsSyntax_2) ~~>>
      ((name, ifExistsDo, containsOn, variable, labelName, property, options) => ast.CreateNodePropertyExistenceConstraint(variable, labelName, property, name, ifExistsDo, options, containsOn, CONSTRAINT_VERSION_2)) |
    group(constraintStart ~~ RelationshipPropertyExistenceConstraintWithOptionsSyntax) ~~>>
      ((name, ifExistsDo, containsOn, variable, relTypeName, property, options) => ast.CreateRelationshipPropertyExistenceConstraint(variable, relTypeName, property, name, ifExistsDo, options, containsOn, CONSTRAINT_VERSION_0)) |
    group(constraintStart ~~ RelationshipPropertyExistenceConstraintWithOptionsSyntax_1) ~~>>
      ((name, ifExistsDo, containsOn, variable, relTypeName, property, options) => ast.CreateRelationshipPropertyExistenceConstraint(variable, relTypeName, property, name, ifExistsDo, options, containsOn, CONSTRAINT_VERSION_1)) |
    group(constraintStart ~~ RelationshipPropertyExistenceConstraintWithOptionsSyntax_2) ~~>>
      ((name, ifExistsDo, containsOn, variable, relTypeName, property, options) => ast.CreateRelationshipPropertyExistenceConstraint(variable, relTypeName, property, name, ifExistsDo, options, containsOn, CONSTRAINT_VERSION_2))
  }

  private def CreateConstraintStart: Rule3[Option[String], ast.IfExistsDo, Boolean] = rule {
    // without name
    keyword("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS ON") ~~~> (_ => None) ~> (_ => ast.IfExistsInvalidSyntax) ~> (_ => true) |
    keyword("CREATE OR REPLACE CONSTRAINT IF NOT EXISTS FOR") ~~~> (_ => None) ~> (_ => ast.IfExistsInvalidSyntax) ~> (_ => false) |
    keyword("CREATE OR REPLACE CONSTRAINT ON") ~~~> (_ => None) ~> (_ => ast.IfExistsReplace) ~> (_ => true) |
    keyword("CREATE OR REPLACE CONSTRAINT FOR") ~~~> (_ => None) ~> (_ => ast.IfExistsReplace) ~> (_ => false) |
    keyword("CREATE CONSTRAINT IF NOT EXISTS ON") ~~~> (_ => None) ~> (_ => ast.IfExistsDoNothing) ~> (_ => true) |
    keyword("CREATE CONSTRAINT IF NOT EXISTS FOR") ~~~> (_ => None) ~> (_ => ast.IfExistsDoNothing) ~> (_ => false) |
    keyword("CREATE CONSTRAINT ON") ~~~> (_ => None) ~> (_ => ast.IfExistsThrowError) ~> (_ => true) |
    keyword("CREATE CONSTRAINT FOR") ~~~> (_ => None) ~> (_ => ast.IfExistsThrowError) ~> (_ => false) |
    // with name
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS ON")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsInvalidSyntax) ~> (_ => true) |
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsInvalidSyntax) ~> (_ => false) |
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("ON")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsReplace) ~> (_ => true) |
    group(keyword("CREATE OR REPLACE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsReplace) ~> (_ => false) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS ON")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsDoNothing) ~> (_ => true) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("IF NOT EXISTS FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsDoNothing) ~> (_ => false) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("ON")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsThrowError) ~> (_ => true) |
    group(keyword("CREATE CONSTRAINT") ~~ SymbolicNameString ~~ keyword("FOR")) ~~>> (name => _ => Some(name)) ~> (_ => ast.IfExistsThrowError) ~> (_ => false)
  }

  private def DropUniqueConstraint: Rule1[ast.DropUniquePropertyConstraint] = rule {
    group(keyword("DROP CONSTRAINT ON") ~~ UniqueConstraintSyntax) ~~>>
      ((variable, label, property) => ast.DropUniquePropertyConstraint(variable, label, Seq(property)))
  }

  private def DropUniqueCompositeConstraint: Rule1[ast.DropUniquePropertyConstraint] = rule {
    group(keyword("DROP CONSTRAINT ON") ~~ UniqueCompositeConstraintSyntax) ~~>> (ast.DropUniquePropertyConstraint(_, _, _))
  }

  private def DropNodeKeyConstraint: Rule1[ast.DropNodeKeyConstraint] = rule {
    group(keyword("DROP CONSTRAINT ON") ~~ NodeKeyConstraintSyntax) ~~>> (ast.DropNodeKeyConstraint(_, _, _))
  }

  private def DropNodePropertyExistenceConstraint: Rule1[ast.DropNodePropertyExistenceConstraint] = rule {
    group(keyword("DROP CONSTRAINT ON") ~~ NodePropertyExistenceConstraintSyntax_0) ~~>> (ast.DropNodePropertyExistenceConstraint(_, _, _))
  }

  private def DropRelationshipPropertyExistenceConstraint: Rule1[ast.DropRelationshipPropertyExistenceConstraint] = rule {
    group(keyword("DROP CONSTRAINT ON") ~~ RelationshipPropertyExistenceConstraintSyntax_0) ~~>> (ast.DropRelationshipPropertyExistenceConstraint(_, _, _))
  }

  private def DropConstraintOnName: Rule1[ast.DropConstraintOnName] = rule {
    group(keyword("DROP CONSTRAINT") ~~ SymbolicNameString ~~ keyword("IF EXISTS")) ~~>> (ast.DropConstraintOnName(_, ifExists = true)) |
    group(keyword("DROP CONSTRAINT") ~~ SymbolicNameString) ~~>> (ast.DropConstraintOnName(_, ifExists = false))
  }

  private def NodeKeyConstraintSyntax: Rule3[Variable, LabelName, Seq[Property]] = "(" ~~ Variable ~~ NodeLabel ~~ ")" ~~
    keyword("ASSERT") ~~ "(" ~~ VariablePropertyExpressions ~~ ")" ~~ keyword("IS NODE KEY")

  private def NodeKeyConstraintSyntax_2: Rule3[Variable, LabelName, Seq[Property]] = "(" ~~ Variable ~~ NodeLabel ~~ ")" ~~
    keyword("REQUIRE") ~~ "(" ~~ VariablePropertyExpressions ~~ ")" ~~ keyword("IS NODE KEY")

  private def NodeKeyConstraintWithOptionsSyntax: Rule4[Variable, LabelName, Seq[Property], Options] = rule {
    NodeKeyConstraintSyntax ~~ optionsMapOrParameter |
    NodeKeyConstraintSyntax ~> (_ => NoOptions)
  }

  private def NodeKeyConstraintWithOptionsSyntax_2: Rule4[Variable, LabelName, Seq[Property], Options] = rule {
      NodeKeyConstraintSyntax_2 ~~ optionsMapOrParameter|
      NodeKeyConstraintSyntax_2 ~> (_ => NoOptions)
  }

  private def UniqueConstraintSyntax: Rule3[Variable, LabelName, Property] = "(" ~~ Variable ~~ NodeLabel ~~ ")" ~~
    keyword("ASSERT") ~~ VariablePropertyExpression ~~ keyword("IS UNIQUE")

  private def UniqueConstraintSyntax_2: Rule3[Variable, LabelName, Property] = "(" ~~ Variable ~~ NodeLabel ~~ ")" ~~
    keyword("REQUIRE") ~~ VariablePropertyExpression ~~ keyword("IS UNIQUE")

  private def UniqueConstraintWithOptionsSyntax: Rule4[Variable, LabelName, Property, Options] = rule {
    UniqueConstraintSyntax ~~ optionsMapOrParameter |
    UniqueConstraintSyntax ~> (_ => NoOptions)
  }

  private def UniqueConstraintWithOptionsSyntax_2: Rule4[Variable, LabelName, Property, Options] = rule {
    UniqueConstraintSyntax_2 ~~ optionsMapOrParameter |
    UniqueConstraintSyntax_2 ~> (_ => NoOptions)
  }

  private def UniqueCompositeConstraintSyntax: Rule3[Variable, LabelName, Seq[Property]] = "(" ~~ Variable ~~ NodeLabel ~~ ")" ~~
    keyword("ASSERT") ~~ "(" ~~ VariablePropertyExpressions ~~ ")" ~~ keyword("IS UNIQUE")

  private def UniqueCompositeConstraintSyntax_2: Rule3[Variable, LabelName, Seq[Property]] = "(" ~~ Variable ~~ NodeLabel ~~ ")" ~~
    keyword("REQUIRE") ~~ "(" ~~ VariablePropertyExpressions ~~ ")" ~~ keyword("IS UNIQUE")

  private def UniqueCompositeConstraintWithOptionsSyntax: Rule4[Variable, LabelName, Seq[Property], Options] = rule {
    UniqueCompositeConstraintSyntax ~~ optionsMapOrParameter |
    UniqueCompositeConstraintSyntax ~> (_ => NoOptions)
  }

  private def UniqueCompositeConstraintWithOptionsSyntax_2: Rule4[Variable, LabelName, Seq[Property], Options] = rule {
      UniqueCompositeConstraintSyntax_2 ~~ optionsMapOrParameter |
      UniqueCompositeConstraintSyntax_2 ~> (_ => NoOptions)
  }

  private def NodePropertyExistenceConstraintSyntax_0: Rule3[Variable, LabelName, Property] = "(" ~~ Variable ~~ NodeLabel ~~ ")" ~~
    keyword("ASSERT EXISTS") ~~ "(" ~~ VariablePropertyExpression ~~ ")"

  private def NodePropertyExistenceConstraintSyntax_1: Rule3[Variable, LabelName, Property] = "(" ~~ Variable ~~ NodeLabel ~~ ")" ~~
    keyword("ASSERT") ~~ group(group("(" ~~ VariablePropertyExpression ~~ ")") | VariablePropertyExpression) ~~ keyword("IS NOT NULL")

  private def NodePropertyExistenceConstraintSyntax_2: Rule3[Variable, LabelName, Property] = "(" ~~ Variable ~~ NodeLabel ~~ ")" ~~
    keyword("REQUIRE") ~~ group(group("(" ~~ VariablePropertyExpression ~~ ")") | VariablePropertyExpression) ~~ keyword("IS NOT NULL")

  private def NodePropertyExistenceConstraintWithOptionsSyntax: Rule4[Variable, LabelName, Property, Options] = rule {
    NodePropertyExistenceConstraintSyntax_0 ~~ optionsMapOrParameter |
    NodePropertyExistenceConstraintSyntax_0 ~> (_ => NoOptions)
  }

  private def NodePropertyExistenceConstraintWithOptionsSyntax_1: Rule4[Variable, LabelName, Property, Options] = rule {
      NodePropertyExistenceConstraintSyntax_1 ~~ optionsMapOrParameter |
      NodePropertyExistenceConstraintSyntax_1 ~> (_ => NoOptions)
  }

  private def NodePropertyExistenceConstraintWithOptionsSyntax_2: Rule4[Variable, LabelName, Property, Options] = rule {
      NodePropertyExistenceConstraintSyntax_2 ~~ optionsMapOrParameter |
      NodePropertyExistenceConstraintSyntax_2 ~> (_ => NoOptions)
  }

  private def RelationshipPropertyExistenceConstraintSyntax_0: Rule3[Variable, RelTypeName, Property] = RelationshipPatternSyntax ~~
    keyword("ASSERT EXISTS") ~~ "(" ~~ VariablePropertyExpression ~~ ")"

  private def RelationshipPropertyExistenceConstraintSyntax_1: Rule3[Variable, RelTypeName, Property] =  RelationshipPatternSyntax ~~
    keyword("ASSERT") ~~ group(group("(" ~~ VariablePropertyExpression ~~ ")") | VariablePropertyExpression) ~~ keyword("IS NOT NULL")

  private def RelationshipPropertyExistenceConstraintSyntax_2: Rule3[Variable, RelTypeName, Property] =  RelationshipPatternSyntax ~~
    keyword("REQUIRE") ~~ group(group("(" ~~ VariablePropertyExpression ~~ ")") | VariablePropertyExpression) ~~ keyword("IS NOT NULL")

  private def RelationshipPropertyExistenceConstraintWithOptionsSyntax: Rule4[Variable, RelTypeName, Property, Options] = rule {
    RelationshipPropertyExistenceConstraintSyntax_0 ~~ optionsMapOrParameter |
    RelationshipPropertyExistenceConstraintSyntax_0 ~> (_ => NoOptions)
  }

  private def RelationshipPropertyExistenceConstraintWithOptionsSyntax_1: Rule4[Variable, RelTypeName, Property, Options] = rule {
      RelationshipPropertyExistenceConstraintSyntax_1 ~~ optionsMapOrParameter |
      RelationshipPropertyExistenceConstraintSyntax_1 ~> (_ => NoOptions)
  }

  private def RelationshipPropertyExistenceConstraintWithOptionsSyntax_2: Rule4[Variable, RelTypeName, Property, Options] = rule {
      RelationshipPropertyExistenceConstraintSyntax_2 ~~ optionsMapOrParameter |
      RelationshipPropertyExistenceConstraintSyntax_2 ~> (_ => NoOptions)
  }
}
