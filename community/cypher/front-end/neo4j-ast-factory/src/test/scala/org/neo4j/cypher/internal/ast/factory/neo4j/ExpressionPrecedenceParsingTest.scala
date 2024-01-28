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
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.CollectExpression
import org.neo4j.cypher.internal.ast.CountExpression
import org.neo4j.cypher.internal.ast.ExistsExpression
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
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
   * 6: +, -
   * 5: *, /, %
   * 4: POW
   * 3: +(unary), -(unary)
   * 2: .prop, :Label, [expr], [..]
   * 1: literal, parameter, CASE, COUNT, EXISTS, COLLECT, map projection, list comprehension, pattern comprehension,
   * reduce, all, any, none, single, pattern, shortest path, (expr), functions, variables
   */

  test("precedence 12 vs 11") {
    // (1 XOR 2) OR (3 XOR 4)
    parsing[Expression]("1 XOR 2 OR 3 XOR 4") shouldGive or(
      xor(literalInt(1), literalInt(2)),
      xor(literalInt(3), literalInt(4))
    )
  }

  test("precedence 11 vs 10") {
    // true XOR (true AND false) XOR false
    parsing[Expression]("true XOR true AND false XOR false") shouldGive xor(
      xor(trueLiteral, and(trueLiteral, falseLiteral)),
      falseLiteral
    )
  }

  test("precedence 10 vs 9") {
    // (NOT 1) AND (NOT 3) AND 5
    parsing[Expression]("NOT 1 AND NOT 3 AND 5") shouldGive and(
      and(not(literalInt(1)), not(literalInt(3))),
      literalInt(5)
    )
  }

  test("precedence 9 vs 8") {
    // NOT (1 < 2 = 3 <= (NOT 4) <> 5 >= 6 > 7)
    parsing[Expression]("NOT 1 < 2 = 3 <= (NOT 4) <> 5 >= 6 > 7") shouldGive
      not(ands(
        lessThan(literalInt(1), literalInt(2)),
        eq(literalInt(2), literalInt(3)),
        lessThanOrEqual(literalInt(3), not(literalInt(4))),
        notEquals(not(literalInt(4)), literalInt(5)),
        greaterThanOrEqual(literalInt(5), literalInt(6)),
        greaterThan(literalInt(6), literalInt(7))
      ))
  }

  test("precedence 9 vs 8 - negative") {
    failsToParse[Expression]("1 = NOT 2")
  }

  test("precedence 8 vs 7") {
    // ('string' STARTS WITH 's') = ('string' =~ 's?') > ('string' ENDS WITH 's') < ('string' IS NULL)
    // >= ('string' CONTAINS 's') <> ('string' IS NOT NULL) <= ('string' IN list) = (y IS TYPED BOOLEAN)
    // = (1 IS NOT TYPED BOOLEAN) = ('string' IS NORMALIZED) = ('string' IS NOT NORMALIZED)
    parsing[Expression](
      "'string' STARTS WITH 's' = 'string' =~ 's?' > 'string' ENDS WITH 's' < 'string' IS NULL >= 'string' " +
        "CONTAINS 's' <> 'string' IS NOT NULL <= 'string' IN list = y IS TYPED BOOLEAN = 1 IS NOT TYPED BOOLEAN" +
        " = 'string' IS NORMALIZED = 'string' IS NOT NORMALIZED"
    ) shouldGive
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
  }

  test("precedence 7 - negative") {
    failsToParse[Expression]("'parse' ENDS WITH 'se' CONTAINS 'e'")
    failsToParse[Expression]("'ab' STARTS WITH 'a' IS NOT TYPED BOOLEAN")
    failsToParse[Expression]("RETURN [1] IS :: LIST<INT> IS :: BOOLEAN")
    failsToParse[Expression]("RETURN 'string' IS :: STRING IS NORMALIZED")
  }

  test("precedence 7 vs 6") {
    // ('string' + 'thing') STARTS WITH ('s' + 't')
    parsing[Expression]("'string' + 'thing' STARTS WITH 's' + 't'") shouldGive
      startsWith(
        add(literalString("string"), literalString("thing")),
        add(literalString("s"), literalString("t"))
      )

    // ('string' + 'thing') CONTAINS ('ring' - 'ing')
    parsing[Expression]("'string' + 'thing' CONTAINS 'ring' - 'ing'") shouldGive
      contains(
        add(literalString("string"), literalString("thing")),
        subtract(literalString("ring"), literalString("ing"))
      )

    // ('string' - 'ing') ENDS WITH ('s' + 't')
    parsing[Expression]("'string' - 'ing' ENDS WITH 's' + 't'") shouldGive
      endsWith(
        subtract(literalString("string"), literalString("ing")),
        add(literalString("s"), literalString("t"))
      )

    // ('string' - 'ing') =~ ('s?' - 's')
    parsing[Expression]("'string' - 'ing' =~ 's?' - 's'") shouldGive
      regex(
        subtract(literalString("string"), literalString("ing")),
        subtract(literalString("s?"), literalString("s"))
      )

    // ('string' - 'ing') IS NORMALIZED
    parsing[Expression]("'string' - 'ing' IS NORMALIZED") shouldGive
      isNormalized(
        subtract(literalString("string"), literalString("ing")),
        NFCNormalForm
      )

    // (2 + 3) IN [(2 - 1)]
    parsing[Expression]("2 + 3 IN [2 - 1]") shouldGive
      in(add(literalInt(2), literalInt(3)), listOf(subtract(literalInt(2), literalInt(1))))

    // (1 + 2) IS NOT NULL
    parsing[Expression]("1 + 2 IS NOT NULL") shouldGive isNotNull(add(literalInt(1), literalInt(2)))

    // (1 - 2) IS NULL
    parsing[Expression]("1 - 2 IS NULL") shouldGive isNull(subtract(literalInt(1), literalInt(2)))

    //  ([true] + n.p) :: STRING
    parsing[Expression](" [true] + n.p :: STRING") shouldGive
      isTyped(add(listOf(trueLiteral), prop("n", "p")), StringType(isNullable = true)(pos))

    // (3 - 4) IS NOT TYPED BOOLEAN
    parsing[Expression]("3 - 4 IS NOT :: BOOLEAN") shouldGive
      isNotTyped(subtract(literalInt(3), literalInt(4)), BooleanType(isNullable = true)(pos))
  }

  test("precedence 6 - left-associativity") {
    // ((1 + 2) - 3) + 4
    parsing[Expression]("1 + 2 - 3 + 4") shouldGive
      add(subtract(add(literalInt(1), literalInt(2)), literalInt(3)), literalInt(4))
  }

  test("precedence 6 vs 5") {
    // 1 + (2 / 3 * 4) - (5 % 6)
    parsing[Expression]("1 + 2 / 3 * 4 - 5 % 6") shouldGive
      subtract(
        add(literalInt(1), multiply(divide(literalInt(2), literalInt(3)), literalInt(4))),
        modulo(literalInt(5), literalInt(6))
      )
  }

  test("precedence 5 - left-associativity") {
    // (2 / 3) * 4
    parsing[Expression]("2 / 3 * 4") shouldGive multiply(divide(literalInt(2), literalInt(3)), literalInt(4))

    // (5 % 4) % 2
    parsing[Expression]("5 % 4 % 2") shouldGive modulo(modulo(literalInt(5), literalInt(4)), literalInt(2))
  }

  test("precedence 5 vs 4") {
    // 1 * (2^3) / 4
    parsing[Expression]("1 * 2 ^ 3 / 4") shouldGive divide(
      multiply(literalInt(1), pow(literalInt(2), literalInt(3))),
      literalInt(4)
    )
  }

  test("precedence 4 - left-associativity") {
    // (4 ^3) ^ 2
    parsing[Expression]("4 ^ 3 ^ 2") shouldGive pow(pow(literalInt(4), literalInt(3)), literalInt(2))
  }

  test("precedence 4 vs 3") {
    // (+1) ^ (-2)
    parsing[Expression]("+1^-2") shouldGive pow(unaryAdd(literalInt(1)), literalInt(-2))
  }

  test("precedence 3 vs 2") {
    // -(list[+(expr:Label)])
    parsing[Expression]("-list[+expr:Label]") shouldGive
      unarySubtract(containerIndex(
        varFor("list"),
        unaryAdd(labelExpressionPredicate(varFor("expr"), labelOrRelTypeLeaf("Label")))
      ))

    // +(list[-(x.y)..+(5)])
    parsing[Expression]("+list[-x.y..+5]") shouldGive
      unaryAdd(ListSlice(
        varFor("list"),
        Some(unarySubtract(prop(varFor("x"), "y"))),
        Some(unaryAdd(literalInt(5)))
      )(pos))
  }

  test("precedence 2 vs 1") {

    // ($list)[(single(x IN y WHERE ('a' + 'b').prop))]
    parsing[Expression]("$list[single(x IN y WHERE ('a' + 'b').prop)]") shouldGive
      containerIndex(
        parameter("list", CTAny),
        singleInList(varFor("x"), varFor("y"), prop(add(literalString("a"), literalString("b")), "prop"))
      )

    // (all(x IN y WHERE null)):Label
    parsing[Expression]("all(x IN y WHERE null):Label") shouldGive
      labelExpressionPredicate(allInList(varFor("x"), varFor("y"), nullLiteral), labelOrRelTypeLeaf("Label"))

    // (none(x IN y WHERE true)).prop
    parsing[Expression]("none(x IN y WHERE false).prop") shouldGive
      prop(noneInList(varFor("x"), varFor("y"), falseLiteral), "prop")

    // (COLLECT {RETURN 42})[(any(x IN y WHERE (size('str')).prop))..(reduce(x=true, y IN list | x AND y))]
    parsing[Expression](
      "COLLECT {RETURN 42}[any(x IN y WHERE size('str').prop)..reduce(x=true, y IN list | x AND y)]"
    ) shouldGive
      ListSlice(
        CollectExpression(
          singleQuery(
            return_(returnItem(literalInt(42), "42"))
          )
        )(pos, None, None),
        Some(anyInList(varFor("x"), varFor("y"), prop(function("size", literalString("str")), "prop"))),
        Some(reduce(varFor("x"), trueLiteral, varFor("y"), varFor("list"), and(varFor("x"), varFor("y"))))
      )(pos)

    // [(x IN (EXISTS {RETURN 42}) WHERE (x{.*}) = (COUNT {RETURN 42}))][([(n)-->() | n])]
    parsing[Expression]("[x IN EXISTS {RETURN 42} WHERE x{.*} = COUNT {RETURN 42}][[(n)-->() | n]]") shouldGive
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

    // (shortestPath((a)-->(b)))[(CASE x WHEN true THEN 1 ELSE 2 END)]
    parsing[Expression]("shortestPath((a)-->(b))[CASE x WHEN true THEN 1 ELSE 2 END]") shouldGive
      containerIndex(
        ShortestPathExpression(ShortestPathsPatternPart(
          relationshipChain(nodePat(Some("a")), relPat(), nodePat(Some("b"))),
          single = true
        )(pos)),
        caseExpression(Some(varFor("x")), Some(literalInt(2)), (trueLiteral, literalInt(1)))
      )
  }
}
