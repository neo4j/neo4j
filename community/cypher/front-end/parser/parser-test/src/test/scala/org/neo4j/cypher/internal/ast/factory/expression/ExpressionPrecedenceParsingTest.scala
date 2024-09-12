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

import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions.AllPropertiesSelector
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.ListSlice
import org.neo4j.cypher.internal.expressions.MapProjection
import org.neo4j.cypher.internal.expressions.NFCNormalForm
import org.neo4j.cypher.internal.expressions.ShortestPathExpression
import org.neo4j.cypher.internal.expressions.ShortestPathsPatternPart
import org.neo4j.cypher.internal.util.symbols.BooleanType
import org.neo4j.cypher.internal.util.symbols.CTAny
import org.neo4j.cypher.internal.util.symbols.StringType

class ExpressionPrecedenceParsingTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  /**
   * Precedence in Cypher:
   * 12: OR
   * 11: XOR
   * 10: AND
   * 9: NOT
   * 8: =, !=, <>, <, >, <=, >=
   * 7: =~, STARS WITH, ENDS WITH, CONTAINS, IN, IS NULL, IS NOT NULL, IS ::, IS NOT ::, IS NORMALIZED, IS NOT NORMALIZED
   * 6: +, -, ||
   * 5: *, /, %
   * 4: POW
   * 3: +(unary), -(unary)
   * 2: .prop, :Label, [expr], [..]
   * 1: literal, parameter, CASE, COUNT, EXISTS, COLLECT, map projection, list comprehension, pattern comprehension,
   * reduce, all, any, none, single, pattern, shortest path, (expr), functions, variables
   */

  test("precedence 12 vs 11") {
    // (1 XOR 2) OR (3 XOR 4)
    "1 XOR 2 OR 3 XOR 4" should parseTo[Expression](or(
      xor(literalInt(1), literalInt(2)),
      xor(literalInt(3), literalInt(4))
    ))
  }

  test("precedence 11 vs 10") {
    // true XOR (true AND false) XOR false
    "true XOR true AND false XOR false" should parseTo[Expression](xor(
      xor(trueLiteral, and(trueLiteral, falseLiteral)),
      falseLiteral
    ))
  }

  test("precedence 10 vs 9") {
    // (NOT 1) AND (NOT 3) AND 5
    "NOT 1 AND NOT 3 AND 5" should parseTo[Expression](and(
      and(not(literalInt(1)), not(literalInt(3))),
      literalInt(5)
    ))
  }

  test("precedence 9 vs 8") {
    // NOT (1 < 2 = 3 <= (NOT 4) <> 5 >= 6 > 7)
    "NOT 1 < 2 = 3 <= (NOT 4) <> 5 >= 6 > 7" should parseTo[Expression](
      not(ands(
        lessThan(literalInt(1), literalInt(2)),
        eq(literalInt(2), literalInt(3)),
        lessThanOrEqual(literalInt(3), not(literalInt(4))),
        notEquals(not(literalInt(4)), literalInt(5)),
        greaterThanOrEqual(literalInt(5), literalInt(6)),
        greaterThan(literalInt(6), literalInt(7))
      ))
    )
  }

  test("precedence 9 vs 8 - negative") {
    "RETURN 1 = NOT 2" should notParse[Statements]
  }

  test("precedence 8 vs 7") {
    // ('string' STARTS WITH 's') = ('string' =~ 's?') > ('string' ENDS WITH 's') < ('string' IS NULL)
    // >= ('string' CONTAINS 's') <> ('string' IS NOT NULL) <= ('string' IN list) = (y IS TYPED BOOLEAN)
    // = (1 IS NOT TYPED BOOLEAN) = ('string' IS NORMALIZED) = ('string' IS NOT NORMALIZED)
    "'string' STARTS WITH 's' = 'string' =~ 's?' > 'string' ENDS WITH 's' < 'string' IS NULL >= 'string' " +
      "CONTAINS 's' <> 'string' IS NOT NULL <= 'string' IN list = y IS TYPED BOOLEAN = 1 IS NOT TYPED BOOLEAN" +
      " = 'string' IS NORMALIZED = 'string' IS NOT NORMALIZED" should parseTo[Expression](
        ands(
          eq(
            startsWith(literalString("string"), literalString("s")),
            regex(literalString("string"), literalString("s?"))
          ),
          greaterThan(
            regex(literalString("string"), literalString("s?")),
            endsWith(literalString("string"), literalString("s"))
          ),
          lessThan(
            endsWith(literalString("string"), literalString("s")),
            isNull(literalString("string"))
          ),
          greaterThanOrEqual(
            isNull(literalString("string")),
            contains(literalString("string"), literalString("s"))
          ),
          notEquals(
            contains(literalString("string"), literalString("s")),
            isNotNull(literalString("string"))
          ),
          lessThanOrEqual(
            isNotNull(literalString("string")),
            in(literalString("string"), varFor("list"))
          ),
          eq(
            in(literalString("string"), varFor("list")),
            isTyped(varFor("y"), BooleanType(isNullable = true)(pos))
          ),
          eq(
            isTyped(varFor("y"), BooleanType(isNullable = true)(pos)),
            isNotTyped(literalInt(1), BooleanType(isNullable = true)(pos))
          ),
          eq(
            isNotTyped(literalInt(1), BooleanType(isNullable = true)(pos)),
            isNormalized(literalString("string"), NFCNormalForm)
          ),
          eq(
            isNormalized(literalString("string"), NFCNormalForm),
            isNotNormalized(literalString("string"), NFCNormalForm)
          )
        )
      )
  }

  test("precedence 7 - negative") {
    "RETURN 'parse' ENDS WITH 'se' CONTAINS 'e'" should notParse[Statements]
    "RETURN 'ab' STARTS WITH 'a' IS NOT TYPED BOOLEAN" should notParse[Statements]
    "RETURN [1] IS :: LIST<INT> IS :: BOOLEAN" should notParse[Statements]
    "RETURN 'string' IS :: STRING IS NORMALIZED" should notParse[Statements]
  }

  test("precedence 7 vs 6") {
    // ('string' + 'thing') STARTS WITH ('s' + 't')
    "'string' + 'thing' STARTS WITH 's' + 't'" should parseTo[Expression](
      startsWith(
        add(literalString("string"), literalString("thing")),
        add(literalString("s"), literalString("t"))
      )
    )
    // ('string' || 'thing') STARTS WITH ('s' || 't')
    "'string' || 'thing' STARTS WITH 's' || 't'" should parseTo[Expression](
      startsWith(
        concatenate(literalString("string"), literalString("thing")),
        concatenate(literalString("s"), literalString("t"))
      )
    )
    // ([1] || [2]) IN ([3] || [4])
    "[1] || [2] IN [3] || [4]" should parseTo[Expression](
      in(
        concatenate(listOf(literalInt(1)), listOf(literalInt(2))),
        concatenate(listOf(literalInt(3)), listOf(literalInt(4)))
      )
    )

    // ('string' + 'thing') CONTAINS ('ring' - 'ing')
    "'string' + 'thing' CONTAINS 'ring' - 'ing'" should parseTo[Expression](
      contains(
        add(literalString("string"), literalString("thing")),
        subtract(literalString("ring"), literalString("ing"))
      )
    )

    // ('string' - 'ing') ENDS WITH ('s' + 't')
    "'string' - 'ing' ENDS WITH 's' + 't'" should parseTo[Expression](
      endsWith(
        subtract(literalString("string"), literalString("ing")),
        add(literalString("s"), literalString("t"))
      )
    )

    // ('string' - 'ing') =~ ('s?' - 's')
    "'string' - 'ing' =~ 's?' - 's'" should parseTo[Expression](
      regex(
        subtract(literalString("string"), literalString("ing")),
        subtract(literalString("s?"), literalString("s"))
      )
    )

    // ('string' - 'ing') IS NORMALIZED
    "'string' - 'ing' IS NORMALIZED" should parseTo[Expression](
      isNormalized(
        subtract(literalString("string"), literalString("ing")),
        NFCNormalForm
      )
    )

    // (2 + 3) IN [(2 - 1)]
    "2 + 3 IN [2 - 1]" should parse[Expression].toAsts {
      case Cypher5JavaCc =>
        in(add(literalInt(2), literalInt(3)), listOf(subtract(literalInt(2), literalInt(1))))
      case _ =>
        in(add(literalInt(2), literalInt(3)), listOf(subtract(literalInt(2), literalInt(1))))
    }
    // (1 + 2) IS NOT NULL
    "1 + 2 IS NOT NULL" should parseTo[Expression](isNotNull(add(literalInt(1), literalInt(2))))

    // (1 - 2) IS NULL
    "1 - 2 IS NULL" should parseTo[Expression](isNull(subtract(literalInt(1), literalInt(2))))

    //  ([true] + n.p) :: STRING
    " [true] + n.p :: STRING" should parseTo[Expression] {
      isTyped(add(listOf(trueLiteral), prop("n", "p")), StringType(isNullable = true)(pos))
    }

    // (3 - 4) IS NOT TYPED BOOLEAN
    "3 - 4 IS NOT :: BOOLEAN" should parseTo[Expression](
      isNotTyped(subtract(literalInt(3), literalInt(4)), BooleanType(isNullable = true)(pos))
    )
  }

  test("precedence 6 - left-associativity") {
    // ((1 + 2) - 3) + 4
    "1 + 2 - 3 + 4" should parseTo[Expression](
      add(subtract(add(literalInt(1), literalInt(2)), literalInt(3)), literalInt(4))
    )
  }

  test("precedence 6 vs 5") {
    // 1 + (2 / 3 * 4) - (5 % 6)
    "1 + 2 / 3 * 4 - 5 % 6" should parseTo[Expression](
      subtract(
        add(literalInt(1), multiply(divide(literalInt(2), literalInt(3)), literalInt(4))),
        modulo(literalInt(5), literalInt(6))
      )
    )
  }

  test("precedence 5 - left-associativity") {
    // (2 / 3) * 4
    "2 / 3 * 4" should parseTo[Expression](multiply(divide(literalInt(2), literalInt(3)), literalInt(4)))

    // (5 % 4) % 2
    "5 % 4 % 2" should parseTo[Expression](modulo(modulo(literalInt(5), literalInt(4)), literalInt(2)))
  }

  test("precedence 5 vs 4") {
    // 1 * (2^3) / 4
    "1 * 2 ^ 3 / 4" should parseTo[Expression](divide(
      multiply(literalInt(1), pow(literalInt(2), literalInt(3))),
      literalInt(4)
    ))
  }

  test("precedence 4 - left-associativity") {
    // (4 ^3) ^ 2
    "4 ^ 3 ^ 2" should parseTo[Expression](pow(pow(literalInt(4), literalInt(3)), literalInt(2)))
  }

  test("precedence 4 vs 3") {
    // (+1) ^ (-2)
    "+1^-2" should parseTo[Expression](pow(unaryAdd(literalInt(1)), literalInt(-2)))
  }

  test("precedence 3 vs 2") {
    // -(list[+(expr:Label)])
    "-list[+expr:Label]" should parseTo[Expression] {
      unarySubtract(containerIndex(
        varFor("list"),
        unaryAdd(labelExpressionPredicate(varFor("expr"), labelOrRelTypeLeaf("Label")))
      ))
    }

    // +(list[-(x.y)..+(5)])
    "+list[-x.y..+5]" should parseTo[Expression] {
      unaryAdd(ListSlice(
        varFor("list"),
        Some(unarySubtract(prop(varFor("x"), "y"))),
        Some(unaryAdd(literalInt(5)))
      )(pos))
    }
  }

  test("precedence 2 vs 1") {

    // ($list)[(single(x IN y WHERE ('a' + 'b').prop))]
    "$list[single(x IN y WHERE ('a' + 'b').prop)]" should parseTo[Expression] {
      containerIndex(
        parameter("list", CTAny),
        singleInList(varFor("x"), varFor("y"), prop(add(literalString("a"), literalString("b")), "prop"))
      )
    }

    // (all(x IN y WHERE null)):Label
    "all(x IN y WHERE null):Label" should parseTo[Expression] {
      labelExpressionPredicate(
        allInList(varFor("x"), varFor("y"), nullLiteral),
        labelOrRelTypeLeaf("Label")
      )
    }

    // (none(x IN y WHERE true)).prop
    "none(x IN y WHERE false).prop" should parseTo[Expression] {
      prop(
        noneInList(varFor("x"), varFor("y"), falseLiteral),
        "prop"
      )
    }

    // (COLLECT {RETURN 42})[(any(x IN y WHERE (size('str')).prop))..(reduce(x=true, y IN list | x AND y))]
    "COLLECT {RETURN 42}[any(x IN y WHERE size('str').prop)..reduce(x=true, y IN list | x AND y)]" should
      parseTo[Expression](ListSlice(
        CollectExpression(
          singleQuery(
            return_(returnItem(literalInt(42), "42"))
          )
        )(pos, None, None),
        Some(anyInList(varFor("x"), varFor("y"), prop(function("size", literalString("str")), "prop"))),
        Some(reduce(varFor("x"), trueLiteral, varFor("y"), varFor("list"), and(varFor("x"), varFor("y"))))
      )(pos))
    // [(x IN (EXISTS {RETURN 42}) WHERE (x{.*}) = (COUNT {RETURN 42}))][([(n)-->() | n])]
    "[x IN EXISTS {RETURN 42} WHERE x{.*} = COUNT {RETURN 42}][[(n)-->() | n]]" should parseTo[Expression] {
      containerIndex(
        listComprehension(
          varFor("x"),
          ExistsExpression(
            singleQuery(
              return_(returnItem(literalInt(42), "42"))
            )
          )(pos, None, None),
          Some(
            eq(
              MapProjection(varFor("x"), List(AllPropertiesSelector()(pos)))(pos),
              CountExpression(
                singleQuery(
                  return_(returnItem(literalInt(42), "42"))
                )
              )(pos, None, None)
            )
          ),
          None
        ),
        patternComprehension(relationshipChain(nodePat(Some("n")), relPat(), nodePat()), varFor("n"))
      )
    }

    // (shortestPath((a)-->(b)))[(CASE x WHEN true THEN 1 ELSE 2 END)]
    "shortestPath((a)-->(b))[CASE x WHEN true THEN 1 ELSE 2 END]" should parse[Expression].toAst {
      containerIndex(
        ShortestPathExpression(ShortestPathsPatternPart(
          relationshipChain(nodePat(Some("a")), relPat(), nodePat(Some("b"))),
          single = true
        )(pos)),
        caseExpression(Some(varFor("x")), Some(literalInt(2)), (equals(varFor("x"), trueLiteral), literalInt(1)))
      )
    }
  }
}
