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

import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.LegacyAstParsingTestSupport
import org.neo4j.cypher.internal.expressions.CaseExpression
import org.neo4j.cypher.internal.expressions.NFCNormalForm
import org.neo4j.cypher.internal.util.symbols.IntegerType

class CaseExpressionParserTest extends AstParsingTestBase with LegacyAstParsingTestSupport {

  test("CASE WHEN (e) THEN e ELSE null END") {
    yields {
      CaseExpression(
        None,
        List(varFor("e") -> varFor("e")),
        Some(nullLiteral)
      )
    }
  }

  test("CASE WHEN (e) THEN e END") {
    yields {
      CaseExpression(
        None,
        List(varFor("e") -> varFor("e")),
        None
      )
    }
  }

  test("CASE WHEN e=1 THEN 4 WHEN e=2 THEN 6 ELSE 7 END") {
    yields {
      CaseExpression(
        None,
        List(equals(varFor("e"), literalInt(1)) -> literalInt(4), equals(varFor("e"), literalInt(2)) -> literalInt(6)),
        Some(literalInt(7))
      )
    }
  }

  test("CASE when(e) WHEN (e) THEN e ELSE null END") {
    yields {
      CaseExpression(
        Some(function("when", varFor("e"))),
        List(equals(function("when", varFor("e")), varFor("e")) -> varFor("e")),
        Some(nullLiteral)
      )
    }
  }

  test("CASE n.eyes WHEN \"blue\" THEN 1 WHEN \"brown\" THEN 2 ELSE 3 END") {
    yields {
      CaseExpression(
        Some(prop("n", "eyes")),
        List(
          equals(prop("n", "eyes"), literalString("blue")) -> literalInt(1),
          equals(prop("n", "eyes"), literalString("brown")) -> literalInt(2)
        ),
        Some(literalInt(3))
      )
    }
  }

  test("CASE n.eyes WHEN \"blue\" THEN 1 WHEN \"brown\" THEN 2 END") {
    yields {
      CaseExpression(
        Some(prop("n", "eyes")),
        List(
          equals(prop("n", "eyes"), literalString("blue")) -> literalInt(1),
          equals(prop("n", "eyes"), literalString("brown")) -> literalInt(2)
        ),
        None
      )
    }
  }

  test("CASE n.eyes WHEN \"blue\", \"green\", \"brown\" THEN 1 WHEN \"red\" THEN 2 END") {
    yields {
      CaseExpression(
        Some(prop("n", "eyes")),
        List(
          equals(prop("n", "eyes"), literalString("blue")) -> literalInt(1),
          equals(prop("n", "eyes"), literalString("green")) -> literalInt(1),
          equals(prop("n", "eyes"), literalString("brown")) -> literalInt(1),
          equals(prop("n", "eyes"), literalString("red")) -> literalInt(2)
        ),
        None
      )
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
    yields {
      CaseExpression(
        Some(prop("n", "eyes")),
        List(
          isNull(prop("n", "eyes")) -> literalInt(1),
          isTyped(prop("n", "eyes"), IntegerType(isNullable = true)(pos)) -> literalInt(2),
          isNormalized(prop("n", "eyes"), NFCNormalForm) -> literalInt(3)
        ),
        Some(literalInt(4))
      )
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
    yields {
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
      )
    }
  }

  test(
    """
      |CASE n.eyes
      | WHEN n.eyes > 2 THEN 1
      | ELSE 4
      |END""".stripMargin
  ) {
    yields {
      CaseExpression(
        Some(prop("n", "eyes")),
        List(
          equals(prop("n", "eyes"), greaterThan(prop("n", "eyes"), literalInt(2))) -> literalInt(1)
        ),
        Some(literalInt(4))
      )
    }
  }

  test(
    """CASE n.eyes
      | WHEN in[0] THEN 1
      | ELSE 4
      |END""".stripMargin
  ) {
    yields {
      CaseExpression(
        Some(prop("n", "eyes")),
        List(
          equals(prop("n", "eyes"), containerIndex(varFor("in"), literalInt(0))) -> literalInt(1)
        ),
        Some(literalInt(4))
      )
    }
  }

  test(
    """CASE 2
      |  WHEN contains + 1 THEN 'contains'
      |  ELSE 'else'
      |END""".stripMargin
  ) {
    yields {
      CaseExpression(
        Some(literalInt(2)),
        List(
          equals(literalInt(2), add(varFor("contains"), literalInt(1))) -> literalString("contains")
        ),
        Some(literalString("else"))
      )
    }
  }

  test(
    """CASE 1
      |  WHEN is::INT THEN 'is int'
      |  ELSE 'else'
      |END""".stripMargin
  ) {
    yields {
      CaseExpression(
        Some(literalInt(1)),
        List(
          equals(literalInt(1), isTyped(varFor("is"), IntegerType(isNullable = true)(pos))) -> literalString("is int")
        ),
        Some(literalString("else"))
      )
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
    yields {
      CaseExpression(
        Some(literalInt(1)),
        List(
          isTyped(literalInt(1), IntegerType(isNullable = true)(pos)) -> literalInt(1),
          isNotTyped(literalInt(1), IntegerType(isNullable = true)(pos)) -> literalInt(2),
          isTyped(literalInt(1), IntegerType(isNullable = true)(pos)) -> literalInt(3)
        ),
        Some(literalString("else"))
      )
    }
  }

  test("CASE when(v1) + 1 WHEN THEN v2 ELSE null END") {
    failsToParse[CaseExpression]
  }

  test("CASE WHEN true, false THEN v2 ELSE null END") {
    failsToParse[CaseExpression]
  }

  test("CASE n WHEN true, false, THEN 1 ELSE null END") {
    failsToParse[CaseExpression]
  }
}
