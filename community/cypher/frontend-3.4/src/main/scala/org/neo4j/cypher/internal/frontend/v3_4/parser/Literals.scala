/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.frontend.v3_4.parser

import org.neo4j.cypher.internal.aux.v3_4.symbols._
import org.neo4j.cypher.internal.v3_4.{expressions => ast}
import org.parboiled.scala._

import scala.language.postfixOps

trait Literals extends Parser
  with Base with Strings {

  def Expression: Rule1[ast.Expression]

  def Variable: Rule1[ast.Variable] =
    rule("a variable") { SymbolicNameString ~~>> (ast.Variable(_) ) }.memoMismatches

  def ReservedClauseStartKeyword: Rule0 =
    keyword("CALL") |
    keyword("CREATE") |
    keyword("CYPHER") |
    keyword("DELETE") |
    keyword("DO") |
    keyword("DROP") |
    keyword("EXPLAIN") |
    keyword("FOREACH") |
    keyword("FROM") |
    keyword("INTO") |
    keyword("LOAD") |
    keyword("MATCH") |
    keyword("MERGE") |
    keyword("OPTIONAL") |
    keyword("PERSIST") |
    keyword("PROFILE") |
    keyword("_PRAGMA") |
    keyword("RELOCATE") |
    keyword("RETURN") |
    keyword("SNAPSHOT") |
    keyword("START") |
    keyword("UNION") |
    keyword("USING") |
    keyword("UNWIND") |
    keyword("WITH")

  def ProcedureName: Rule1[ast.ProcedureName] =
    rule("a procedure name") { SymbolicNameString ~~>> (ast.ProcedureName(_) ) }.memoMismatches

  def FunctionName: Rule1[ast.FunctionName] =
    rule("a function name") { SymbolicNameString ~~>> (ast.FunctionName(_) ) }.memoMismatches

  def PropertyKeyName: Rule1[ast.PropertyKeyName] =
    rule("a property key name") { SymbolicNameString ~~>> (ast.PropertyKeyName(_) ) }.memoMismatches

  def PropertyKeyNames: Rule1[List[ast.PropertyKeyName]] =
    rule("a list of property key names") {
      (oneOrMore(WS ~~ SymbolicNameString ~~ WS ~~>> (ast.PropertyKeyName(_) ), separator = ",") memoMismatches).suppressSubnodes
    }

  def LabelName: Rule1[ast.LabelName] =
    rule("a label name") { SymbolicNameString ~~>> (ast.LabelName(_) ) }.memoMismatches

  def RelTypeName: Rule1[ast.RelTypeName] =
    rule("a rel type name") { SymbolicNameString ~~>> (ast.RelTypeName(_) ) }.memoMismatches

  def Operator: Rule1[ast.Variable] = rule {
    OpChar ~ zeroOrMore(OpCharTail) ~>>> (ast.Variable(_: String)) ~ !OpCharTail
  }

  def MapLiteral: Rule1[ast.MapExpression] = rule {
    group(
      ch('{') ~~ zeroOrMore(PropertyKeyName ~~ ch(':') ~~ Expression, separator = CommaSep) ~~ ch('}')
    ) ~~>> (ast.MapExpression(_))
  }

  def LiteralEntry: Rule1[ast.MapProjectionElement] = rule("literal entry")(
    PropertyKeyName ~~ ch(':') ~~ Expression ~~>> (ast.LiteralEntry(_, _)))

  def PropertySelector: Rule1[ast.MapProjectionElement] = rule("property selector")(
    ch('.') ~~ Variable ~~>> (ast.PropertySelector(_)))

  def VariableSelector: Rule1[ast.MapProjectionElement] = rule("variable selector")(
    Variable ~~>> (ast.VariableSelector(_)))

  def AllPropertiesSelector: Rule1[ast.MapProjectionElement] = rule("all properties selector")(
    ch('.') ~~ ch('*') ~ push(ast.AllPropertiesSelector()(_)))

  def MapProjection: Rule1[ast.MapProjection] = rule {
    group(
      Variable ~~ ch('{') ~~ zeroOrMore(LiteralEntry | PropertySelector | VariableSelector | AllPropertiesSelector, CommaSep) ~~ ch('}')
    ) ~~>> (ast.MapProjection(_, _))
  }

  def Parameter: Rule1[ast.Parameter] = rule("a parameter") {
    NewParameter | OldParameter
  }

  def NewParameter: Rule1[ast.Parameter] = rule("a parameter (new syntax") {
    ((ch('$') ~~ (UnescapedSymbolicNameString | EscapedSymbolicNameString | UnsignedDecimalInteger ~> (_.toString))) memoMismatches) ~~>> (ast.Parameter(_, CTAny))
  }

  def OldParameter: Rule1[ast.Parameter] = rule("a parameter (old syntax)") {
    ((ch('{') ~~ (UnescapedSymbolicNameString | EscapedSymbolicNameString | UnsignedDecimalInteger ~> (_.toString)) ~~ ch('}')) memoMismatches) ~~>> (ast.Parameter(_, CTAny))
  }

  def NumberLiteral: Rule1[ast.Literal] = rule("a number") (
      DoubleLiteral
    | SignedIntegerLiteral
  ).memoMismatches

  def DoubleLiteral: Rule1[ast.DecimalDoubleLiteral] = rule("a floating point number") (
      ExponentDecimalReal ~>>> (ast.DecimalDoubleLiteral(_))
    | RegularDecimalReal ~>>> (ast.DecimalDoubleLiteral(_))
  )

  def SignedIntegerLiteral: Rule1[ast.SignedIntegerLiteral] = rule("an integer") (
      HexInteger ~>>> (ast.SignedHexIntegerLiteral(_))
    | OctalInteger ~>>> (ast.SignedOctalIntegerLiteral(_))
    | DecimalInteger ~>>> (ast.SignedDecimalIntegerLiteral(_))
  )

  def UnsignedIntegerLiteral: Rule1[ast.UnsignedIntegerLiteral] = rule("an unsigned integer") {
    UnsignedDecimalInteger ~>>> (ast.UnsignedDecimalIntegerLiteral(_))
  }

  def RangeLiteral: Rule1[ast.Range] = rule (
      group(
        optional(UnsignedIntegerLiteral ~ WS) ~
        ".." ~
        optional(WS ~ UnsignedIntegerLiteral)
      ) ~~>> (ast.Range(_, _))
    | UnsignedIntegerLiteral ~~>> (l => ast.Range(Some(l), Some(l)))
  )

  def NodeLabels: Rule1[Seq[ast.LabelName]] = rule("node labels") {
    (oneOrMore(NodeLabel, separator = WS) memoMismatches).suppressSubnodes
  }

  def NodeLabel: Rule1[ast.LabelName] = rule {
    ((operator(":") ~~ LabelName) memoMismatches).suppressSubnodes
  }

  def RelType: Rule1[ast.RelTypeName] = rule {
    ((operator(":") ~~ RelTypeName) memoMismatches).suppressSubnodes
  }

  def StringLiteral: Rule1[ast.StringLiteral] = rule("\"...string...\"") {
    (((
       ch('\'') ~ StringCharacters('\'') ~ ch('\'')
     | ch('"') ~ StringCharacters('"') ~ ch('"')
    ) memoMismatches) suppressSubnodes) ~~>> (ast.StringLiteral(_))
  }
}
