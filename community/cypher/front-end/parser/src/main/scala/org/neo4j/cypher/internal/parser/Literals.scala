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

import org.neo4j.cypher.internal.expressions
import org.neo4j.cypher.internal.expressions.SensitiveParameter
import org.neo4j.cypher.internal.expressions.UnsignedIntegerLiteral
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.CTMap
import org.neo4j.cypher.internal.util.symbols.CTString
import org.parboiled.scala.Parser
import org.parboiled.scala.Rule0
import org.parboiled.scala.Rule1
import org.parboiled.scala.group

import scala.language.postfixOps

trait Literals extends Parser
  with Base with Strings {

  def Expression: Rule1[expressions.Expression]

  def Variable: Rule1[expressions.Variable] =
    rule("a variable") { SymbolicNameString ~~>> (expressions.Variable(_) ) }.memoMismatches

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
    keyword("USE") |
    keyword("WITH") |
    keyword("COPY")

  def ProcedureName: Rule1[expressions.ProcedureName] =
    rule("a procedure name") { SymbolicNameString ~~>> (expressions.ProcedureName(_) ) }.memoMismatches

  def GlobbedProcedureName: Rule1[expressions.ProcedureName] =
    rule("a globbed procedure name") { GlobbedSymbolicNameString ~~>> (expressions.ProcedureName(_) ) }.memoMismatches

  def FunctionName: Rule1[expressions.FunctionName] =
    rule("a function name") { SymbolicNameString ~~>> (expressions.FunctionName(_) ) }.memoMismatches

  def GlobbedFunctionName: Rule1[expressions.FunctionName] =
    rule("a globbed function name") { GlobbedSymbolicNameString ~~>> (expressions.FunctionName(_) ) }.memoMismatches

  def PropertyKeyName: Rule1[expressions.PropertyKeyName] =
    rule("a property key name") { SymbolicNameString ~~>> (expressions.PropertyKeyName(_) ) }.memoMismatches

  def PropertyKeyNames: Rule1[List[expressions.PropertyKeyName]] =
    rule("a list of property key names") {
      (oneOrMore(WS ~~ SymbolicNameString ~~ WS ~~>> (expressions.PropertyKeyName(_) ), separator = ",") memoMismatches).suppressSubnodes
    }

  def LabelName: Rule1[expressions.LabelName] =
    rule("a label name") { SymbolicNameString ~~>> (expressions.LabelName(_) ) }.memoMismatches

  def RelTypeName: Rule1[expressions.RelTypeName] =
    rule("a rel type name") { SymbolicNameString ~~>> (expressions.RelTypeName(_) ) }.memoMismatches

  def LabelOrRelTypeName: Rule1[expressions.LabelOrRelTypeName] =
    rule("a label or a rel type name") { SymbolicNameString ~~>> (expressions.LabelOrRelTypeName(_) ) }.memoMismatches

  def Operator: Rule1[expressions.Variable] = rule {
    OpChar ~ zeroOrMore(OpCharTail) ~>>> (expressions.Variable(_: String)) ~ !OpCharTail
  }

  def MapLiteral: Rule1[expressions.MapExpression] = rule {
    group(
      ch('{') ~~ zeroOrMore(PropertyKeyName ~~ ch(':') ~~ Expression, separator = CommaSep) ~~ ch('}')
    ) ~~>> (expressions.MapExpression(_))
  }

  def LiteralEntry: Rule1[expressions.MapProjectionElement] = rule("literal entry")(
    PropertyKeyName ~~ ch(':') ~~ Expression ~~>> (expressions.LiteralEntry(_, _)))

  def PropertySelector: Rule1[expressions.MapProjectionElement] = rule("property selector")(
    ch('.') ~~ Variable ~~>> (expressions.PropertySelector(_)))

  def VariableSelector: Rule1[expressions.MapProjectionElement] = rule("variable selector")(
    Variable ~~>> (expressions.VariableSelector(_)))

  def AllPropertiesSelector: Rule1[expressions.MapProjectionElement] = rule("all properties selector")(
    ch('.') ~~ ch('*') ~ push(expressions.AllPropertiesSelector()(_)))

  def MapProjection: Rule1[expressions.MapProjection] = rule {
    group(
      Variable ~~ ch('{') ~~ zeroOrMore(LiteralEntry | PropertySelector | VariableSelector | AllPropertiesSelector, CommaSep) ~~ ch('}')
    ) ~~>> ((a, b) => pos => expressions.MapProjection(a, b)(pos))
  }

  def Parameter: Rule1[expressions.Parameter] =
    parameterName ~~>> (expressions.ExplicitParameter(_, CTAny))

  def StringParameter: Rule1[expressions.Parameter] =
    parameterName ~~>> (expressions.ExplicitParameter(_, CTString))

  def MapParameter: Rule1[expressions.Parameter] =
    parameterName ~~>> (expressions.ExplicitParameter(_, CTMap))

  def SensitiveStringParameter: Rule1[expressions.Parameter] =
    parameterName ~~>> (name => pos => new expressions.ExplicitParameter(name, CTString)(pos) with SensitiveParameter)

  private def parameterName: Rule1[String] = rule("a parameter") {
    (ch('$') ~~ (UnescapedSymbolicNameString | EscapedSymbolicNameString | UnsignedDecimalInteger ~> (_.toString))) memoMismatches
  }

  def NumberLiteral: Rule1[expressions.Literal] = rule("a number") (
      DoubleLiteral
    | SignedIntegerLiteral
  ).memoMismatches

  def DoubleLiteral: Rule1[expressions.DecimalDoubleLiteral] = rule("a floating point number") (
      ExponentDecimalReal ~>>> (expressions.DecimalDoubleLiteral(_))
    | RegularDecimalReal ~>>> (expressions.DecimalDoubleLiteral(_))
  )

  def SignedIntegerLiteral: Rule1[expressions.SignedIntegerLiteral] = rule("an integer") (
      HexInteger ~>>> (expressions.SignedHexIntegerLiteral(_))
    | OctalInteger ~>>> (expressions.SignedOctalIntegerLiteral(_))
    | DecimalInteger ~>>> (expressions.SignedDecimalIntegerLiteral(_))
  )

  def UnsignedIntegerLiteral: Rule1[expressions.UnsignedIntegerLiteral] = rule("an unsigned integer") {
    UnsignedDecimalInteger ~>>> (expressions.UnsignedDecimalIntegerLiteral(_))
  }

  def RangeLiteral: Rule1[expressions.Range] = rule (
      group(
        optional(UnsignedIntegerLiteral ~ WS) ~
        ".." ~
        optional(WS ~ UnsignedIntegerLiteral)
      ) ~~>> (expressions.Range(_, _))
    | UnsignedIntegerLiteral ~~>> ((l: UnsignedIntegerLiteral) => expressions.Range(Some(l), Some(l)))
  )

  def NodeLabels: Rule1[Seq[expressions.LabelName]] = rule("node labels") {
    (oneOrMore(NodeLabel, separator = WS) memoMismatches).suppressSubnodes
  }

  def NodeLabel: Rule1[expressions.LabelName] = rule {
    ((operator(":") ~~ LabelName) memoMismatches).suppressSubnodes
  }

  def RelType: Rule1[expressions.RelTypeName] = rule {
    ((operator(":") ~~ RelTypeName) memoMismatches).suppressSubnodes
  }

  def NodeLabelsOrRelTypes: Rule1[Seq[expressions.LabelOrRelTypeName]] = rule("node labels or rel types") {
    (oneOrMore(NodeLabelOrRelType, separator = WS) memoMismatches).suppressSubnodes
  }

  def NodeLabelOrRelType: Rule1[expressions.LabelOrRelTypeName] = rule {
    ((operator(":") ~~ LabelOrRelTypeName) memoMismatches).suppressSubnodes
  }

  def StringLiteral: Rule1[expressions.StringLiteral] = rule("\"...string...\"") {
    (((
       ch('\'') ~ StringCharacters('\'') ~ ch('\'')
     | ch('"') ~ StringCharacters('"') ~ ch('"')
    ) memoMismatches) suppressSubnodes) ~~>> (expressions.StringLiteral(_))
  }

  def SensitiveStringLiteral: Rule1[expressions.SensitiveStringLiteral] = rule("\"...string...\"") {
    (((
      ch('\'') ~ SensitiveStringCharacters('\'') ~ ch('\'')
        | ch('"') ~ SensitiveStringCharacters('"') ~ ch('"')
      ) memoMismatches) suppressSubnodes) ~~>> (expressions.SensitiveStringLiteral(_))
  }

}
