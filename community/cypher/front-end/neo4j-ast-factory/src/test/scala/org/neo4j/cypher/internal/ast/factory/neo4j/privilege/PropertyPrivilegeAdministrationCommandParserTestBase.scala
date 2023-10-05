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

  // These should parse correctly to allow them to be rejected in the semantic check with a user-friendly explanation
  val semanticInvalidExpressions: Seq[LiteralExpression] = Seq(
    LiteralExpression(equals(prop(varFor("n"), "prop"), nullLiteral)), // n.prop = null
    LiteralExpression(notEquals(prop(varFor("n"), "prop"), nullLiteral)), // n.prop <> null
    LiteralExpression(not(equals(prop(varFor("n"), "prop"), nullLiteral))), // NOT n.prop = null

    LiteralExpression(
      equals(nullLiteral, prop(varFor("n"), "prop")),
      equals(prop(varFor("n"), "prop"), nullLiteral)
    ), // null = n.prop
    LiteralExpression(
      notEquals(nullLiteral, prop(varFor("n"), "prop")),
      notEquals(prop(varFor("n"), "prop"), nullLiteral)
    ), // null <> n.prop

    LiteralExpression(and(
      equals(prop(varFor("n"), "prop1"), literalInt(1)),
      equals(prop(varFor("n"), "prop2"), literalInt(1))
    )), // n.prop1 = 1 AND n.prop2 = 1
    LiteralExpression(or(
      equals(prop(varFor("n"), "prop1"), literalInt(1)),
      equals(prop(varFor("n"), "prop2"), literalInt(1))
    )), // n.prop1 = 1 OR n.prop2 = 1

    LiteralExpression(
      MapExpression(Seq((propName("prop1"), literalInt(1)), (propName("prop2"), literalInt(1))))(pos)
    ), // {prop1: 1, prop2: 1}

    LiteralExpression(not(not(equals(prop(varFor("n"), "prop1"), literalInt(1))))), // NOT NOT n.prop = 1

    LiteralExpression(equals(prop(varFor("n"), "prop"), add(literalInt(1), literalInt(2)))), // n.prop = 1 + 2
    LiteralExpression(equals(prop(varFor("n"), "prop"), subtract(literalInt(1), literalInt(2)))), // n.prop = 1 - 2

    // List of ints
    LiteralExpression(equals(prop(varFor("n"), "prop1"), listOfInt(1, 2))), // n.prop = [1, 2]
    LiteralExpression(notEquals(prop(varFor("n"), "prop1"), listOfInt(1, 2))), // n.prop <> [1, 2]
    LiteralExpression(MapExpression(Seq((propName("prop1"), listOfInt(1, 2))))(pos)), // {prop1: [1, 2]}
    LiteralExpression(in(prop(varFor("n"), "prop1"), listOfInt(1, 2))), // n.prop IN [1, 2]

    // List of strings
    LiteralExpression(equals(prop(varFor("n"), "prop1"), listOfString("s1", "s2"))), // n.prop = ['s1', 's2']
    LiteralExpression(notEquals(prop(varFor("n"), "prop1"), listOfString("s1", "s2"))), // n.prop <> ['s1', 's2']
    LiteralExpression(MapExpression(Seq((propName("prop1"), listOfString("s1", "s2"))))(pos)), // {prop1: ['s1', 's2']}
    LiteralExpression(in(prop(varFor("n"), "prop1"), listOfString("s1", "s2"))), // n.prop IN ['s1', 's2']

    // List of booleans
    LiteralExpression(equals(prop(varFor("n"), "prop1"), listOf(trueLiteral, falseLiteral))), // n.prop = [true, false]
    LiteralExpression(notEquals(
      prop(varFor("n"), "prop1"),
      listOf(trueLiteral, falseLiteral)
    )), // n.prop <> [true, false]
    LiteralExpression(
      MapExpression(Seq((propName("prop1"), listOf(trueLiteral, falseLiteral))))(pos)
    ), // {prop1: [true, false]}
    LiteralExpression(in(prop(varFor("n"), "prop1"), listOf(trueLiteral, falseLiteral))), // n.prop IN [true, false]

    // List of floats
    LiteralExpression(equals(
      prop(varFor("n"), "prop1"),
      listOf(literalFloat(1.1), literalFloat(2.2))
    )), // n.prop = [1.1, 2.2]
    LiteralExpression(notEquals(
      prop(varFor("n"), "prop1"),
      listOf(literalFloat(1.1), literalFloat(2.2))
    )), // n.prop <> [1.1, 2.2]
    LiteralExpression(
      MapExpression(Seq((propName("prop1"), listOf(literalFloat(1.1), literalFloat(1.2)))))(pos)
    ), // {prop1: [1.1, 2.2]}
    LiteralExpression(in(
      prop(varFor("n"), "prop1"),
      listOf(literalFloat(1.1), literalFloat(2.2))
    )) // n.prop IN [1.1, 2.2]
  )

  val literalExpressions: Seq[LiteralExpression] = Seq(
    // Integer
    LiteralExpression(equals(prop(varFor("n"), "prop1"), literalInt(1))), // n.prop1 = 1
    LiteralExpression(not(notEquals(prop(varFor("n"), "prop1"), literalInt(1)))), // NOT n.prop1 <> 1
    LiteralExpression(MapExpression(Seq((propName("prop1"), literalInt(1))))(pos)), // {prop1: 1}
    LiteralExpression(in(prop(varFor("n"), "prop1"), listOf(literalInt(1)))), // n.prop1 IN [1]
    LiteralExpression(not(in(prop(varFor("n"), "prop1"), listOf(literalInt(1))))), // NOT n.prop1 IN [1]

    // commutated Integer
    LiteralExpression(
      equals(literalInt(1), prop(varFor("n"), "prop1")),
      equals(prop(varFor("n"), "prop1"), literalInt(1))
    ), // 1 = n.prop1
    LiteralExpression(
      not(notEquals(literalInt(1), prop(varFor("n"), "prop1"))),
      not(notEquals(prop(varFor("n"), "prop1"), literalInt(1)))
    ), // NOT 1 <> n.prop1

    // integer inequality
    LiteralExpression(notEquals(prop(varFor("n"), "prop1"), literalInt(1))), // n.prop <> 1
    LiteralExpression(not(equals(prop(varFor("n"), "prop1"), literalInt(1)))), // NOT n.prop = 1

    // commutated integer inequality
    LiteralExpression(
      notEquals(literalInt(1), prop(varFor("n"), "prop1")),
      notEquals(prop(varFor("n"), "prop1"), literalInt(1))
    ), //  1 <> n.prop
    LiteralExpression(
      not(equals(literalInt(1), prop(varFor("n"), "prop1"))),
      not(equals(prop(varFor("n"), "prop1"), literalInt(1)))
    ), //  NOT 1 = n.prop

    // String
    LiteralExpression(equals(prop(varFor("n"), "prop1"), literalString("s1"))), // n.prop = 's1'
    LiteralExpression(not(notEquals(prop(varFor("n"), "prop1"), literalString("s1")))), // NOT n.prop <> 's1'
    LiteralExpression(notEquals(prop(varFor("n"), "prop1"), literalString("s1"))), // n.prop <> 's1'
    LiteralExpression(not(equals(prop(varFor("n"), "prop1"), literalString("s1")))), // NOT n.prop = 's1'
    LiteralExpression(MapExpression(Seq((propName("prop1"), literalString("s1"))))(pos)), // {prop1: 's1'}
    LiteralExpression(in(prop(varFor("n"), "prop1"), listOf(literalString("s1")))), // n.prop IN ['s1']
    LiteralExpression(not(in(prop(varFor("n"), "prop1"), listOf(literalString("s1"))))), // NOT n.prop = 's1'

    // Boolean
    LiteralExpression(equals(prop(varFor("n"), "prop1"), trueLiteral)), // n.prop = true
    LiteralExpression(not(notEquals(prop(varFor("n"), "prop1"), trueLiteral))), // NOT n.prop <> true
    LiteralExpression(notEquals(prop(varFor("n"), "prop1"), trueLiteral)), // n.prop <> true
    LiteralExpression(not(equals(prop(varFor("n"), "prop1"), trueLiteral))), // NOT n.prop = true
    LiteralExpression(MapExpression(Seq((propName("prop1"), trueLiteral)))(pos)), // {prop1: true}
    LiteralExpression(equals(prop(varFor("n"), "prop1"), falseLiteral)), // n.prop = false
    LiteralExpression(not(notEquals(prop(varFor("n"), "prop1"), falseLiteral))), // NOT n.prop <> false
    LiteralExpression(notEquals(prop(varFor("n"), "prop1"), falseLiteral)), // n.prop <> false
    LiteralExpression(not(equals(prop(varFor("n"), "prop1"), falseLiteral))), // n.prop <> false
    LiteralExpression(MapExpression(Seq((propName("prop1"), falseLiteral)))(pos)), // {prop1: false}
    LiteralExpression(in(prop(varFor("n"), "prop1"), listOf(trueLiteral))), // n.prop IN [true]
    LiteralExpression(not(in(prop(varFor("n"), "prop1"), listOf(trueLiteral)))), // NOT n.prop IN [true]

    // Float
    LiteralExpression(equals(prop(varFor("n"), "prop1"), literalFloat(1.1))), // n.prop = 1.1
    LiteralExpression(not(notEquals(prop(varFor("n"), "prop1"), literalFloat(1.1)))), // NOT n.prop <> 1.1
    LiteralExpression(notEquals(prop(varFor("n"), "prop1"), literalFloat(1.1))), // n.prop <> 1.1
    LiteralExpression(not(equals(prop(varFor("n"), "prop1"), literalFloat(1.1)))), // NOT n.prop = 1.1
    LiteralExpression(MapExpression(Seq((propName("prop1"), literalFloat(1.1))))(pos)), // {prop1: 1.1}
    LiteralExpression(in(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1)))), // n.prop IN [1.1]
    LiteralExpression(not(in(prop(varFor("n"), "prop1"), listOf(literalFloat(1.1))))), // NOT n.prop IN [1.1]

    // IS NULL, IS NOT NULL
    LiteralExpression(isNull(prop(varFor("n"), "prop1"))), // n.prop1 IS NULL
    LiteralExpression(not(isNotNull(prop(varFor("n"), "prop1")))), // NOT n.prop1 IS NULL
    LiteralExpression(isNotNull(prop(varFor("n"), "prop1"))), // n.prop1 IS NOT NULL
    LiteralExpression(not(isNull(prop(varFor("n"), "prop1")))), // NOT n.prop1 IS NULL
    LiteralExpression(MapExpression(Seq((propName("prop1"), nullLiteral)))(pos)), // {prop1:NULL}

    // Parameter
    LiteralExpression(equals(prop(varFor("n"), "prop1"), parameter("value", CTAny))), // n.prop1 = $value
    LiteralExpression(not(notEquals(prop(varFor("n"), "prop1"), parameter("value", CTAny)))), // NOT n.prop1 <> $value
    LiteralExpression(MapExpression(Seq((propName("prop1"), parameter("value", CTAny))))(pos)), // {prop1: $value}
    LiteralExpression(in(prop(varFor("n"), "prop1"), listOf(parameter("value", CTAny)))), // n.prop1 IN [$value]
    LiteralExpression(not(in(prop(varFor("n"), "prop1"), listOf(parameter("value", CTAny))))) // NOT n.prop1 IN [$value]
  ) ++ semanticInvalidExpressions

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
