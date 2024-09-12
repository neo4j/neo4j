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
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.expressions.InvalidNotEquals
import org.neo4j.cypher.internal.expressions.NFCNormalForm
import org.neo4j.cypher.internal.util.symbols.CTInteger
import org.neo4j.cypher.internal.util.symbols.IntegerType

class CaseExpressionParserTest extends AstParsingTestBase {

  test("CASE WHEN (e) THEN e ELSE null END") {
    parsesTo[Expression] {
      CaseExpression(
        None,
        List(varFor("e") -> varFor("e")),
        Some(nullLiteral)
      )(pos)
    }
  }

  test("CASE WHEN (e) THEN e END") {
    parsesTo[Expression] {
      CaseExpression(
        None,
        List(varFor("e") -> varFor("e")),
        None
      )(pos)
    }
  }

  test("CASE WHEN e=1 THEN 4 WHEN e=2 THEN 6 ELSE 7 END") {
    parsesTo[Expression] {
      CaseExpression(
        None,
        List(equals(varFor("e"), literalInt(1)) -> literalInt(4), equals(varFor("e"), literalInt(2)) -> literalInt(6)),
        Some(literalInt(7))
      )(pos)
    }
  }

  test("CASE a WHEN b THEN c WHEN d THEN e WHEN f THEN g ELSE h END") {
    parsesTo[Expression] {
      CaseExpression(
        Some(varFor("a")),
        Array(
          equals(varFor("a"), varFor("b")) -> varFor("c"),
          equals(varFor("a"), varFor("d")) -> varFor("e"),
          equals(varFor("a"), varFor("f")) -> varFor("g")
        ),
        Some(varFor("h"))
      )(pos)
    }
  }

  test("CASE when(e) WHEN (e) THEN e ELSE null END") {
    parsesTo[Expression] {
      CaseExpression(
        Some(function("when", varFor("e"))),
        List(equals(function("when", varFor("e")), varFor("e")) -> varFor("e")),
        Some(nullLiteral)
      )(pos)
    }
  }

  // Copied from legacy test
  test("some more case expressions") {
    "CASE 1 WHEN 1 THEN 'ONE' END" should parseTo[Expression](
      caseExpression(Some(literal(1)), None, (equals(literal(1), literal(1)), literal("ONE")))
    )
    """CASE 1
         WHEN 1 THEN 'ONE'
         WHEN 2 THEN 'TWO'
       END""" should parseTo[Expression](
      caseExpression(
        Some(literal(1)),
        None,
        (equals(literal(1), literal(1)), literal("ONE")),
        (equals(literal(1), literal(2)), literal("TWO"))
      )
    )
    """CASE 1
           WHEN 1 THEN 'ONE'
           WHEN 2 THEN 'TWO'
           ELSE 'DEFAULT'
       END""" should parseTo[Expression](
      caseExpression(
        Some(literal(1)),
        Some(literal("DEFAULT")),
        (equals(literal(1), literal(1)), literal("ONE")),
        (equals(literal(1), literal(2)), literal("TWO"))
      )
    )
    "CASE WHEN true THEN 'ONE' END" should parseTo[Expression](
      caseExpression(None, None, literal(true) -> literal("ONE"))
    )
    """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
         END""" should parseTo[Expression](
      caseExpression(
        None,
        None,
        (equals(literal(1), literal(2)), literal("ONE")),
        (equals(literal(2), literal("apa")), literal("TWO"))
      )
    )
    """CASE
           WHEN 1=2     THEN 'ONE'
           WHEN 2='apa' THEN 'TWO'
                        ELSE 'OTHER'
         END""" should parseTo[Expression](caseExpression(
      None,
      Some(literal("OTHER")),
      (equals(literal(1), literal(2)), literal("ONE")),
      (equals(literal(2), literal("apa")), literal("TWO"))
    ))
  }

  test("CASE n.eyes WHEN \"blue\" THEN 1 WHEN \"brown\" THEN 2 ELSE 3 END") {
    parsesTo[Expression] {
      CaseExpression(
        Some(prop("n", "eyes")),
        List(
          equals(prop("n", "eyes"), literalString("blue")) -> literalInt(1),
          equals(prop("n", "eyes"), literalString("brown")) -> literalInt(2)
        ),
        Some(literalInt(3))
      )(pos)
    }
  }

  test("CASE n.eyes WHEN \"blue\" THEN 1 WHEN \"brown\" THEN 2 END") {
    parsesTo[Expression] {
      CaseExpression(
        Some(prop("n", "eyes")),
        List(
          equals(prop("n", "eyes"), literalString("blue")) -> literalInt(1),
          equals(prop("n", "eyes"), literalString("brown")) -> literalInt(2)
        ),
        None
      )(pos)
    }
  }

  test("CASE n.eyes WHEN \"blue\", \"green\", \"brown\" THEN 1 WHEN \"red\" THEN 2 END") {
    parsesTo[Expression] {
      CaseExpression(
        Some(prop("n", "eyes")),
        List(
          equals(prop("n", "eyes"), literalString("blue")) -> literalInt(1),
          equals(prop("n", "eyes"), literalString("green")) -> literalInt(1),
          equals(prop("n", "eyes"), literalString("brown")) -> literalInt(1),
          equals(prop("n", "eyes"), literalString("red")) -> literalInt(2)
        ),
        None
      )(pos)
    }
  }

  test(
    """
      |CASE n.eyes
      | WHEN IS NULL THEN 1
      | WHEN IS TYPED INTEGER THEN 2
      | WHEN IS NORMALIZED THEN 3
      | ELSE 4
      |END""".stripMargin
  ) {
    parsesTo[Expression] {
      CaseExpression(
        Some(prop("n", "eyes")),
        List(
          isNull(prop("n", "eyes")) -> literalInt(1),
          isTyped(prop("n", "eyes"), IntegerType(isNullable = true)(pos)) -> literalInt(2),
          isNormalized(prop("n", "eyes"), NFCNormalForm) -> literalInt(3)
        ),
        Some(literalInt(4))
      )(pos)
    }
  }

  test(
    """
      |CASE n.eyes
      | WHEN > 2, = 1, 5 THEN 1
      | WHEN STARTS WITH "gre", ENDS WITH "en" THEN 3
      | ELSE 4
      |END""".stripMargin
  ) {
    parsesTo[Expression] {
      CaseExpression(
        Some(prop("n", "eyes")),
        List(
          greaterThan(prop("n", "eyes"), literalInt(2)) -> literalInt(1),
          equals(prop("n", "eyes"), literalInt(1)) -> literalInt(1),
          equals(prop("n", "eyes"), literalInt(5)) -> literalInt(1),
          startsWith(prop("n", "eyes"), literalString("gre")) -> literalInt(3),
          endsWith(prop("n", "eyes"), literalString("en")) -> literalInt(3)
        ),
        Some(literalInt(4))
      )(pos)
    }
  }

  test(
    """
      |CASE n.eyes
      | WHEN n.eyes > 2 THEN 1
      | ELSE 4
      |END""".stripMargin
  ) {
    parsesTo[Expression] {
      CaseExpression(
        Some(prop("n", "eyes")),
        List(
          equals(prop("n", "eyes"), greaterThan(prop("n", "eyes"), literalInt(2))) -> literalInt(1)
        ),
        Some(literalInt(4))
      )(pos)
    }
  }

  test(
    """CASE n.eyes
      | WHEN in[0] THEN 1
      | ELSE 4
      |END""".stripMargin
  ) {
    parsesTo[Expression] {
      CaseExpression(
        Some(prop("n", "eyes")),
        List(
          equals(prop("n", "eyes"), containerIndex(varFor("in"), literalInt(0))) -> literalInt(1)
        ),
        Some(literalInt(4))
      )(pos)
    }
  }

  test(
    """CASE 2
      |  WHEN contains + 1 THEN 'contains'
      |  ELSE 'else'
      |END""".stripMargin
  ) {
    parsesTo[Expression] {
      CaseExpression(
        Some(literalInt(2)),
        List(
          equals(literalInt(2), add(varFor("contains"), literalInt(1))) -> literalString("contains")
        ),
        Some(literalString("else"))
      )(pos)
    }
  }

  test(
    """CASE 1
      |  WHEN is::INT THEN 'is int'
      |  ELSE 'else'
      |END""".stripMargin
  ) {
    parsesTo[Expression] {
      CaseExpression(
        Some(literalInt(1)),
        List(
          equals(literalInt(1), isTyped(varFor("is"), IntegerType(isNullable = true)(pos))) -> literalString("is int")
        ),
        Some(literalString("else"))
      )(pos)
    }
  }

  // Test all type predicate expressions parse as expected
  test(
    """CASE 1
      |  WHEN IS TYPED INT THEN 1
      |  WHEN IS NOT TYPED INT THEN 2
      |  WHEN :: INT THEN 3
      |  ELSE 'else'
      |END""".stripMargin
  ) {
    parsesTo[Expression] {
      CaseExpression(
        Some(literalInt(1)),
        List(
          isTyped(literalInt(1), IntegerType(isNullable = true)(pos)) -> literalInt(1),
          isNotTyped(literalInt(1), IntegerType(isNullable = true)(pos)) -> literalInt(2),
          isTyped(literalInt(1), IntegerType(isNullable = true)(pos)) -> literalInt(3)
        ),
        Some(literalString("else"))
      )(pos)
    }
  }

  test("RETURN CASE when(v1) + 1 WHEN THEN v2 ELSE null END") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'v2'")
      case _ => _.withSyntaxError(
          """Invalid input 'v2': expected an expression, ',' or 'THEN' (line 1, column 36 (offset: 35))
            |"RETURN CASE when(v1) + 1 WHEN THEN v2 ELSE null END"
            |                                    ^""".stripMargin
        )
    }
  }

  test("RETURN CASE ELSE null END") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'ELSE'")
      case _ => _.withSyntaxError(
          """Invalid input 'ELSE': expected an expression, 'FOREACH', ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 13 (offset: 12))
            |"RETURN CASE ELSE null END"
            |             ^""".stripMargin
        )
    }
  }

  test("RETURN CASE WHEN THEN v2 ELSE null END") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'v2'")
      case _ => _.withSyntaxError(
          """Invalid input 'WHEN': expected an expression, 'FOREACH', ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 13 (offset: 12))
            |"RETURN CASE WHEN THEN v2 ELSE null END"
            |             ^""".stripMargin
        )
    }
  }

  test("RETURN CASE WHEN true, false THEN v2 ELSE null END") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input ','")
      case _ => _.withSyntaxError(
          """Invalid input 'WHEN': expected an expression, 'FOREACH', ',', 'AS', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNION', 'UNWIND', 'USE', 'WITH' or <EOF> (line 1, column 13 (offset: 12))
            |"RETURN CASE WHEN true, false THEN v2 ELSE null END"
            |             ^""".stripMargin
        )
    }
  }

  test("RETURN CASE n WHEN true, false, THEN 1 ELSE null END") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input '1'")
      case _ => _.withSyntaxError(
          """Invalid input '1': expected an expression, ',' or 'THEN' (line 1, column 38 (offset: 37))
            |"RETURN CASE n WHEN true, false, THEN 1 ELSE null END"
            |                                      ^""".stripMargin
        )
    }
  }

  test("case expression combinations") {
    val in = varFor("input")
    val alts = Seq(
      "=~ '.*'" -> regex(in, literal(".*")),
      "starts with 'zzz'" -> startsWith(in, literal("zzz")),
      "ends with 'zzz'" -> endsWith(in, literal("zzz")),
      "is null" -> isNull(in),
      "is not null" -> isNotNull(in),
      "is typed Int" -> isTyped(in, CTInteger),
      "is not typed Int" -> isNotTyped(in, CTInteger),
      ":: Int" -> isTyped(in, CTInteger),
      "= 'x'" -> equals(in, literal("x")),
      "<> 'x'" -> notEquals(in, literal("x")),
      "!= 'x'" -> InvalidNotEquals(in, literal("x"))(pos),
      "< 0" -> lessThan(in, literal(0)),
      "> 0" -> greaterThan(in, literal(0)),
      "<= 0" -> lessThanOrEqual(in, literal(0)),
      ">= 0" -> greaterThanOrEqual(in, literal(0)),
      "a" -> equals(in, varFor("a")),
      "a.p" -> equals(in, prop("a", "p")),
      "a[0]" -> equals(in, containerIndex(varFor("a"), 0)),
      "false" -> equals(in, literal(false))
    )

    alts.foreach { case (cypherWhen, exp) =>
      s"case ${in.name} when $cypherWhen then true end" should parseTo[Expression] {
        caseExpression(Some(in), None, exp -> trueLiteral)
      }
      if (!exp.isInstanceOf[Equals]) {
        s"case when ${in.name} $cypherWhen then true end" should parseTo[Expression] {
          caseExpression(None, None, exp -> trueLiteral)
        }
      }
    }

    for {
      (aCypher, aExp) <- alts
      (bCypher, bExp) <- alts
    } {
      s"""case ${in.name}
         |when $aCypher then 'a'
         |when $bCypher then 'b'
         |else 'c'
         |end
         |""".stripMargin should parseTo[Expression] {
        caseExpression(Some(in), Some(literal("c")), aExp -> literal("a"), bExp -> literal("b"))
      }
      s"""case ${in.name}
         |when $aCypher, $bCypher then 'yes'
         |end
         |""".stripMargin should parseTo[Expression] {
        caseExpression(Some(in), None, aExp -> literal("yes"), bExp -> literal("yes"))
      }
    }

    s"""case ${in.name}
       |when
       |  ${alts.map(_._1).mkString(",\n  ")}
       |then
       |  true
       |end
       |""".stripMargin should parseTo[Expression] {
      caseExpression(Some(in), None, alts.map(a => a._2 -> trueLiteral): _*)
    }
  }
}
