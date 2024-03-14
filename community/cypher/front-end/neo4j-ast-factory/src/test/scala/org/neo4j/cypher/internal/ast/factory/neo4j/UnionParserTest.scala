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

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.ast.factory.neo4j.test.util.ParserSupport.NotAnyAntlr

class UnionParserTest extends AstParsingTestBase {

  test("RETURN 1 AS a UNION RETURN 2 AS a") {
    parsesTo[Statement](NotAnyAntlr)(
      union(
        singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
        singleQuery(return_(aliasedReturnItem(literal(2), "a")))
      )
    )
  }

  test("RETURN 1 AS a UNION ALL RETURN 2 AS a") {
    parsesTo[Statement](NotAnyAntlr)(
      union(
        singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
        singleQuery(return_(aliasedReturnItem(literal(2), "a")))
      ).all
    )
  }

  test("RETURN 1 AS a UNION RETURN 2 AS b") {
    parsesTo[Statement](NotAnyAntlr)(
      union(
        singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
        singleQuery(return_(aliasedReturnItem(literal(2), "b")))
      )
    )
  }

  // invalid Cypher accepted by parser
  test("RETURN 1 AS a UNION ALL RETURN 2 AS b") {
    parsesTo[Statement](NotAnyAntlr)(
      union(
        singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
        singleQuery(return_(aliasedReturnItem(literal(2), "b")))
      ).all
    )
  }

  // invalid Cypher accepted by parser
  test("RETURN 1 AS a UNION FINISH") {
    parsesTo[Statement](NotAnyAntlr)(
      union(
        singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
        singleQuery(finish())
      )
    )
  }

  // invalid Cypher accepted by parser
  test("RETURN 1 AS a UNION ALL FINISH") {
    parsesTo[Statement](NotAnyAntlr)(
      union(
        singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
        singleQuery(finish())
      ).all
    )
  }

  test("FINISH UNION FINISH") {
    parsesTo[Statement](NotAnyAntlr)(
      union(
        singleQuery(finish()),
        singleQuery(finish())
      )
    )
  }

  test("FINISH UNION ALL FINISH") {
    parsesTo[Statement](NotAnyAntlr)(
      union(
        singleQuery(finish()),
        singleQuery(finish())
      ).all
    )
  }

  // invalid Cypher accepted by parser
  test("FINISH UNION RETURN 2 AS a") {
    parsesTo[Statement](NotAnyAntlr)(
      union(
        singleQuery(finish()),
        singleQuery(return_(aliasedReturnItem(literal(2), "a")))
      )
    )
  }

  // invalid Cypher accepted by parser
  test("FINISH UNION ALL RETURN 2 AS a") {
    parsesTo[Statement](NotAnyAntlr)(
      union(
        singleQuery(finish()),
        singleQuery(return_(aliasedReturnItem(literal(2), "a")))
      ).all
    )
  }

  test("RETURN 1 AS a UNION DISTINCT RETURN 2 AS a") {
    failsParsing[Statement](NotAnyAntlr)
  }

  test("RETURN 1 AS a UNION UNION RETURN 2 AS a") {
    failsParsing[Statement](NotAnyAntlr)
  }
}
