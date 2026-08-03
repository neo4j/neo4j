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
package org.neo4j.cypher.internal.ast.factory.expression

import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions.Equals
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.StringInterpolation
import org.neo4j.cypher.internal.expressions.StringLiteral

class StringInterpolationParserTest extends AstParsingTestBase {

  private def part(value: String): StringLiteral = StringLiteral(value)(pos)

  test("plain double-quoted with lowercase s") {
    "s\"hello\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(part("hello"))
    }
  }

  test("plain single-quoted with lowercase s") {
    "s'hello'" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(part("hello"))
    }
  }

  test("plain double-quoted with uppercase S") {
    "S\"hello\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(part("hello"))
    }
  }

  test("plain single-quoted with uppercase S") {
    "S'hello'" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(part("hello"))
    }
  }

  test("plain empty double-quoted string") {
    "s\"\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(part(""))
    }
  }

  test("plain empty single-quoted string") {
    "s''" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(part(""))
    }
  }

  test("double-quoted with single expression") {
    "s\"hello { n.name }\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(StringInterpolation(Seq(part("hello "), part("")), Seq(prop("n", "name")))(pos))
    }
  }

  test("single-quoted with single expression") {
    "s'hello { n.name }'" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(StringInterpolation(Seq(part("hello "), part("")), Seq(prop("n", "name")))(pos))
    }
  }

  test("uppercase S with single expression") {
    "S\"hello { n.name }\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(StringInterpolation(Seq(part("hello "), part("")), Seq(prop("n", "name")))(pos))
    }
  }

  test("expression at the start of string") {
    "s\"{ n.name }\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(StringInterpolation(Seq(part(""), part("")), Seq(prop("n", "name")))(pos))
    }
  }

  test("expression at the end of string") {
    "s\"hello { n.name }\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(StringInterpolation(Seq(part("hello "), part("")), Seq(prop("n", "name")))(pos))
    }
  }

  test("integer literal as expression") {
    "s\"hello { 42 }\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(StringInterpolation(Seq(part("hello "), part("")), Seq(literalInt(42)))(pos))
    }
  }

  test("variable as expression") {
    "s\"hello { n }\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(StringInterpolation(Seq(part("hello "), part("")), Seq(varFor("n")))(pos))
    }
  }

  test("two property expressions separated by text") {
    "s\"{ n.name } and { m.age }\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(StringInterpolation(
          Seq(part(""), part(" and "), part("")),
          Seq(prop("n", "name"), prop("m", "age"))
        )(pos))
    }
  }

  test("three adjacent expressions") {
    "s\"{ a }{ b }{ c }\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(StringInterpolation(
          Seq(part(""), part(""), part(""), part("")),
          Seq(varFor("a"), varFor("b"), varFor("c"))
        )(pos))
    }
  }

  test("arithmetic expressions") {
    "s\"sum is { n.x + m.y }\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(StringInterpolation(
          Seq(part("sum is "), part("")),
          Seq(add(prop("n", "x"), prop("m", "y")))
        )(pos))
    }
  }

  test("escape sequence in plain string") {
    "s\"hello\\nworld\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(part("hello\nworld"))
    }
  }

  test("escape sequence in string part before expression") {
    "s\"hello\\n{ n.name }\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(StringInterpolation(Seq(part("hello\n"), part("")), Seq(prop("n", "name")))(pos))
    }
  }

  test("escaped open brace is literal in plain double-quoted string") {
    "s\"hello \\{ world\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(part("hello { world"))
    }
  }

  test("escaped close brace is literal in plain double-quoted string") {
    "s\"hello \\} world\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(part("hello } world"))
    }
  }

  test("both braces escaped produce no interpolation") {
    "s\"\\{ n.name \\}\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(part("{ n.name }"))
    }
  }

  test("escaped braces in string part after interpolation") {
    "s\"{ n.name } \\{ not interpolated \\}\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(StringInterpolation(
          Seq(part(""), part(" { not interpolated }")),
          Seq(prop("n", "name"))
        )(pos))
    }
  }

  test("escaped open brace is literal in plain single-quoted string") {
    "s'hello \\{ world'" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(part("hello { world"))
    }
  }

  test("Normal string literals do not need escaping of curly braces") {
    "\"hello \\{ world\"" should parseIn[Expression](_ => _.toAst(StringLiteral("hello \\{ world")(pos)))
  }

  test("double-quoted outer containing single-quoted plain inner") {
    "s\"outer { s'inner' }\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(StringInterpolation(
          Seq(part("outer "), part("")),
          Seq(part("inner"))
        )(pos))
    }
  }

  test("single-quoted outer containing double-quoted plain inner") {
    "s'outer { s\"inner\" }'" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(StringInterpolation(
          Seq(part("outer "), part("")),
          Seq(part("inner"))
        )(pos))
    }
  }

  test("double-quoted outer containing double-quoted plain inner (same quote type)") {
    "s\"outer { s\"inner\" }\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(StringInterpolation(
          Seq(part("outer "), part("")),
          Seq(part("inner"))
        )(pos))
    }
  }

  test("single-quoted outer containing single-quoted plain inner (same quote type)") {
    "s'outer { s'inner' }'" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(StringInterpolation(
          Seq(part("outer "), part("")),
          Seq(part("inner"))
        )(pos))
    }
  }

  test("double-quoted outer containing single-quoted inner with expression") {
    "s\"{ s'inner { x }' }\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(StringInterpolation(
          Seq(part(""), part("")),
          Seq(StringInterpolation(Seq(part("inner "), part("")), Seq(varFor("x")))(pos))
        )(pos))
    }
  }

  test("single-quoted outer containing double-quoted inner with expression") {
    "s'{ s\"inner { x }\" }'" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(StringInterpolation(
          Seq(part(""), part("")),
          Seq(StringInterpolation(Seq(part("inner "), part("")), Seq(varFor("x")))(pos))
        )(pos))
    }
  }

  test("double-quoted outer containing double-quoted inner with expression (same quote type)") {
    "s\"outer { s\"inner { x }\" }\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(StringInterpolation(
          Seq(part("outer "), part("")),
          Seq(StringInterpolation(Seq(part("inner "), part("")), Seq(varFor("x")))(pos))
        )(pos))
    }
  }

  test("map literal in embedded expression, followed by a same-quote nested string literal") {
    "s\"prefix { {a: 1} = \"x\" } suffix\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(StringInterpolation(
          Seq(part("prefix "), part(" suffix")),
          Seq(Equals(mapOfInt("a" -> 1), literal("x"))(pos))
        )(pos))
    }
  }

  test("map literal in embedded expression, followed by a same-quote nested string literal (single-quoted)") {
    "s'prefix { {a: 1} = 'x' } suffix'" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(StringInterpolation(
          Seq(part("prefix "), part(" suffix")),
          Seq(Equals(mapOfInt("a" -> 1), literal("x"))(pos))
        )(pos))
    }
  }

  test("nested map literals in embedded expression") {
    "s\"prefix { {a: {b: 1}}.a.b } suffix\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _ => _.toAst(StringInterpolation(
          Seq(part("prefix "), part(" suffix")),
          Seq(prop(prop(mapOf("a" -> mapOfInt("b" -> 1)), "a"), "b"))
        )(pos))
    }
  }

  test("map literal in embedded expression at the very end of the interpolation") {
    "s\"prefix { {a: 1} }\"" should parseIn[Expression] {
      case Cypher5 => _.withAnyFailure
      case _       => _.toAst(StringInterpolation(Seq(part("prefix "), part("")), Seq(mapOfInt("a" -> 1)))(pos))
    }
  }

  test("wrong prefix letter is not interpolated; double quoted string with no expressions") {
    "RETURN f\"hello\"" should notParse[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '"hello"': expected an expression, ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 9 (offset: 8))
            |"RETURN f"hello""
            |         ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '"hello"': expected an expression, ',', 'AS', 'GROUP BY', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 9 (offset: 8))
            |"RETURN f"hello""
            |         ^""".stripMargin
        )
    }
  }

  test("wrong prefix letter is not interpolated; double quoted string with expressions") {
    "RETURN f\"hello { 2 }\"" should notParse[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '"hello { 2 }"': expected an expression, ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 9 (offset: 8))
            |"RETURN f"hello { 2 }""
            |         ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '"hello { 2 }"': expected an expression, ',', 'AS', 'GROUP BY', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 9 (offset: 8))
            |"RETURN f"hello { 2 }""
            |         ^""".stripMargin
        )
    }
  }

  test("wrong prefix letter is not interpolated; single quoted string with no expressions") {
    "RETURN f'hello'" should notParse[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input ''hello'': expected an expression, ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 9 (offset: 8))
            |"RETURN f'hello'"
            |         ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input ''hello'': expected an expression, ',', 'AS', 'GROUP BY', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 9 (offset: 8))
            |"RETURN f'hello'"
            |         ^""".stripMargin
        )
    }
  }

  test("wrong prefix letter is not interpolated; single quoted string with expressions") {
    "RETURN f'hello { 2 }'" should notParse[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input ''hello { 2 }'': expected an expression, ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 9 (offset: 8))
            |"RETURN f'hello { 2 }'"
            |         ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input ''hello { 2 }'': expected an expression, ',', 'AS', 'GROUP BY', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FILTER', 'FINISH', 'FOR', 'FOREACH', 'INSERT', 'LET', 'LIMIT', 'MATCH', 'MERGE', 'NEXT', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SHOW', 'SKIP', 'TERMINATE', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 9 (offset: 8))
            |"RETURN f'hello { 2 }'"
            |         ^""".stripMargin
        )
    }
  }

  test("empty expression is a syntax error") {
    "RETURN s\"hello {}\"" should notParse[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '"hello {}"': expected an expression, ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 9 (offset: 8))
            |"RETURN s"hello {}""
            |         ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '}': expected an expression (line 1, column 17 (offset: 16))
            |"RETURN s"hello {}""
            |                 ^""".stripMargin
        )
    }
  }

  test("unclosed interpolation expression is a syntax error") {
    "RETURN s\"hello { \"" should notParse[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '"hello { "': expected an expression, ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 9 (offset: 8))
            |"RETURN s"hello { ""
            |         ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Failed to parse string literal. The query must contain an even number of non-escaped quotes. (line 1, column 18 (offset: 17))
            |"RETURN s"hello { ""
            |                  ^""".stripMargin
        )
    }
  }

  test("unopened interpolation expression is a syntax error") {
    "RETURN s\"hello } \"" should notParse[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Invalid input '"hello } "': expected an expression, ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'FOREACH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 9 (offset: 8))
            |"RETURN s"hello } ""
            |         ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '}': expected an interpolated string (line 1, column 16 (offset: 15))
            |"RETURN s"hello } ""
            |                ^""".stripMargin
        )
    }
  }

  test("unclosed string is a syntax error") {
    "RETURN s\"hello" should notParse[Statements].in {
      case Cypher5 => _.withSyntaxError(
          """Failed to parse string literal. The query must contain an even number of non-escaped quotes. (line 1, column 9 (offset: 8))
            |"RETURN s"hello"
            |         ^""".stripMargin
        )
      case _ => _.withSyntaxError(
          """Invalid input '': expected an interpolated string (line 1, column 15 (offset: 14))
            |"RETURN s"hello"
            |               ^""".stripMargin
        )
    }
  }
}
