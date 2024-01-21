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
package org.neo4j.cypher.internal.ast.factory.neo4j.privilege

import org.neo4j.cypher.internal.ast.AllGraphsScope
import org.neo4j.cypher.internal.ast.GraphScope
import org.neo4j.cypher.internal.ast.factory.neo4j.AdministrationAndSchemaCommandParserTestBase
import org.neo4j.cypher.internal.ast.prettifier.ExpressionStringifier
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.MapExpression
import org.neo4j.cypher.internal.util.symbols.CTAny

class PropertyPrivilegeAdministrationCommandParserTestBase extends AdministrationAndSchemaCommandParserTestBase {
  case class Scope(graphName: String, graphScope: GraphScope)

  case class LiteralExpression(expression: Expression, expectedAst: Expression)

  object LiteralExpression {
    def apply(expression: Expression): LiteralExpression = LiteralExpression(expression, expression)
  }

  val expressionStringifier: ExpressionStringifier = ExpressionStringifier(preferSingleQuotes = true)

  val literalExpressions: Seq[LiteralExpression] = Seq(
    literalInt(1),
    literalFloat(1.1),
    literalString("s1"),
    trueLiteral,
    falseLiteral,
    parameter("value", CTAny),
    nullLiteral // Semantically invalid
  ).flatMap(l =>
    Seq(
      l,
      listOf(l, l) // Semantically invalid
    ).flatMap(literal =>
      Seq(
        // equals
        LiteralExpression(equals(prop(varFor("n"), "prop1"), literal)), // n.prop1 = value
        LiteralExpression(not(notEquals(prop(varFor("n"), "prop1"), literal))), // NOT n.prop1 <> value
        LiteralExpression(MapExpression(Seq((propName("prop1"), literal)))(pos)), // {prop1: value}
        LiteralExpression(in(prop(varFor("n"), "prop1"), listOf(literal))), // n.prop1 IN [value]

        // not equals
        LiteralExpression(not(in(prop(varFor("n"), "prop1"), listOf(literal)))), // NOT n.prop1 IN [value]
        LiteralExpression(notEquals(prop(varFor("n"), "prop1"), literal)), // n.prop <> value
        LiteralExpression(not(equals(prop(varFor("n"), "prop1"), literal))), // NOT n.prop = value

        // greater than
        LiteralExpression(greaterThan(prop(varFor("n"), "prop1"), literal)), // n.prop1 > value
        LiteralExpression(not(greaterThan(prop(varFor("n"), "prop1"), literal))), // NOT n.prop1 > value

        // greater than or equals
        LiteralExpression(greaterThanOrEqual(prop(varFor("n"), "prop1"), literal)), // n.prop1 >= value
        LiteralExpression(not(greaterThanOrEqual(prop(varFor("n"), "prop1"), literal))), // NOT n.prop1 >= value

        // less than
        LiteralExpression(lessThan(prop(varFor("n"), "prop1"), literal)), // n.prop1 < value
        LiteralExpression(not(lessThan(prop(varFor("n"), "prop1"), literal))), // NOT n.prop1 < value

        // less than or equals
        LiteralExpression(lessThanOrEqual(prop(varFor("n"), "prop1"), literal)), // n.prop1 <= value
        LiteralExpression(not(lessThanOrEqual(prop(varFor("n"), "prop1"), literal))), // NOT n.prop1 <= value

        // Semantic invalid expressions, these should parse correctly to allow them to be rejected in the semantic check with a user-friendly explanation
        LiteralExpression(equals(literal, prop(varFor("n"), "prop1"))), // value = n.prop1
        LiteralExpression(not(notEquals(literal, prop(varFor("n"), "prop1")))), // NOT value <> n.prop1

        LiteralExpression(notEquals(literal, prop(varFor("n"), "prop1"))), // value <> n.prop
        LiteralExpression(not(equals(literal, prop(varFor("n"), "prop1")))), // NOT value = n.prop

        LiteralExpression(and(
          equals(prop(varFor("n"), "prop1"), literal),
          equals(prop(varFor("n"), "prop2"), literal)
        )), // n.prop1 = value AND n.prop2 = value
        LiteralExpression(or(
          equals(prop(varFor("n"), "prop1"), literal),
          equals(prop(varFor("n"), "prop2"), literal)
        )), // n.prop1 = value OR n.prop2 = value

        LiteralExpression(
          MapExpression(Seq((propName("prop1"), literal), (propName("prop2"), literal)))(pos)
        ), // {prop1: value, prop2: value}

        LiteralExpression(not(not(equals(prop(varFor("n"), "prop1"), literal)))), // NOT NOT n.prop = value

        LiteralExpression(equals(prop(varFor("n"), "prop"), add(literal, literal))), // n.prop = value + value
        LiteralExpression(equals(prop(varFor("n"), "prop"), subtract(literal, literal))), // n.prop = value - value

        LiteralExpression(greaterThan(literal, prop(varFor("n"), "prop1"))), // value > n.prop1
        LiteralExpression(not(greaterThan(literal, prop(varFor("n"), "prop1")))), // NOT value > n.prop1

        LiteralExpression(greaterThanOrEqual(literal, prop(varFor("n"), "prop1"))), // value >= n.prop1
        LiteralExpression(not(greaterThanOrEqual(literal, prop(varFor("n"), "prop1")))), // NOT value >= n.prop1

        LiteralExpression(lessThan(literal, prop(varFor("n"), "prop1"))), // value < n.prop1
        LiteralExpression(not(lessThan(literal, prop(varFor("n"), "prop1")))), // NOT value < n.prop1

        LiteralExpression(lessThanOrEqual(literal, prop(varFor("n"), "prop1"))), // value <= n.prop1
        LiteralExpression(not(lessThanOrEqual(literal, prop(varFor("n"), "prop1")))) // NOT value <= n.prop1
      )
    )
  ) ++ Seq(
    // IS NULL, IS NOT NULL
    LiteralExpression(isNull(prop(varFor("n"), "prop1"))), // n.prop1 IS NULL
    LiteralExpression(not(isNotNull(prop(varFor("n"), "prop1")))), // NOT n.prop1 IS NOT NULL
    LiteralExpression(isNotNull(prop(varFor("n"), "prop1"))), // n.prop1 IS NOT NULL
    LiteralExpression(not(isNull(prop(varFor("n"), "prop1")))), // NOT n.prop1 IS NULL
    LiteralExpression(MapExpression(Seq((propName("prop1"), nullLiteral)))(pos)) // {prop1:NULL}
  )

  val invalidSegments: Seq[String] = Seq("ELEMENT", "ELEMENTS", "NODE", "NODES", "RELATIONSHIP", "RELATIONSHIPS", "")
  val graphKeywords: Seq[String] = Seq("GRAPH", "GRAPHS")
  val patternKeyword = "FOR"

  val scopes: Seq[Scope] = Seq(
    Scope("*", AllGraphsScope()(pos)),
    Scope("foo", graphScopeFoo(pos))
  )

  val disallowedPropertyRules: Seq[String] = Seq(
    // node only
    "(n:A)",
    "(:A)",
    "(:A|B)",
    "(n)",

    // cannot combine labels with and
    "(n:A&B) WHERE n.prop1 = 1",
    "(n:A&B WHERE n.prop1 = 1)",
    "(:A&B {prop1 = 1})",

    // cannot combine map and WHERE syntax
    "(n:L {p:1}) WHERE n.p = 1",
    "(n:L {p:1}) WHERE n.p = 2",
    "(n:L {p1:1}) WHERE n.p2 = 2",

    // Relationships
    "(n:A)-[]->(m:B)",
    "(n:A)<-[:R]-(m:B)",
    "(n:A)-[r]-(m:B)",
    "(n:A)-[r:R]-(m:B)",
    "(n:A)-[r:R]-(m:B) WHERE n.prop1 = 1",
    "(:A{prop1:1})-[]-(m)",
    "(n:A{prop1:1})-[]-(m)",
    "()-[r:R]->() WHERE r.p = 1",
    "(:A)-[r:R {p:1}]->(:B)",

    // Valid property rule with extra (foo) after literal
    "(n) WHERE n.prop1 = 1 (foo)",
    """(n) WHERE n.prop1 = "bosse" (foo)""",
    "(n WHERE 1 = n.prop1) (foo)"
  )
}
