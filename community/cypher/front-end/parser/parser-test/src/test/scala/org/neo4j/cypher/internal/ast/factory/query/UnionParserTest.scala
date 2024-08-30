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
package org.neo4j.cypher.internal.ast.factory.query

import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher6
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase

class UnionParserTest extends AstParsingTestBase {

  test("RETURN 1 AS a UNION RETURN 2 AS a") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(return_(aliasedReturnItem(literal(2), "a")))
        ))
      case _ => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(return_(aliasedReturnItem(literal(2), "a"))),
          differentReturnOrderAllowed = true
        ))
    }
  }

  test("RETURN 1 AS a UNION DISTINCT RETURN 2 AS a") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(return_(aliasedReturnItem(literal(2), "a")))
        ))
      case _ => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(return_(aliasedReturnItem(literal(2), "a"))),
          differentReturnOrderAllowed = true
        ))
    }
  }

  test("RETURN 1 AS a UNION ALL RETURN 2 AS a") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(return_(aliasedReturnItem(literal(2), "a")))
        ).all)
      case _ => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(return_(aliasedReturnItem(literal(2), "a"))),
          differentReturnOrderAllowed = true
        ).all)
    }
  }

  // invalid Cypher accepted by parser
  test("RETURN 1 AS a UNION RETURN 2 AS b") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(return_(aliasedReturnItem(literal(2), "b")))
        ))
      case _ => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(return_(aliasedReturnItem(literal(2), "b"))),
          differentReturnOrderAllowed = true
        ))
    }
  }

  // invalid Cypher accepted by parser
  test("RETURN 1 AS a UNION DISTINCT RETURN 2 AS b") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(return_(aliasedReturnItem(literal(2), "b")))
        ))
      case _ => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(return_(aliasedReturnItem(literal(2), "b"))),
          differentReturnOrderAllowed = true
        ))
    }
  }

  // invalid Cypher accepted by parser
  test("RETURN 1 AS a UNION ALL RETURN 2 AS b") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(return_(aliasedReturnItem(literal(2), "b")))
        ).all)
      case _ => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(return_(aliasedReturnItem(literal(2), "b"))),
          differentReturnOrderAllowed = true
        ).all)
    }
  }

  // invalid Cypher accepted by parser
  test("RETURN 1 AS a UNION FINISH") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(finish())
        ))
      case _ => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(finish()),
          differentReturnOrderAllowed = true
        ))
    }
  }

  // invalid Cypher accepted by parser
  test("RETURN 1 AS a UNION DISTINCT FINISH") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(finish())
        ))
      case _ => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(finish()),
          differentReturnOrderAllowed = true
        ))
    }
  }

  // invalid Cypher accepted by parser
  test("RETURN 1 AS a UNION ALL FINISH") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(finish())
        ).all)
      case _ => _.toAst(union(
          singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
          singleQuery(finish()),
          differentReturnOrderAllowed = true
        ).all)
    }
  }

  test("FINISH UNION FINISH") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(union(
          singleQuery(finish()),
          singleQuery(finish())
        ))
      case _ => _.toAst(union(
          singleQuery(finish()),
          singleQuery(finish()),
          differentReturnOrderAllowed = true
        ))
    }
  }

  test("FINISH UNION DISTINCT FINISH") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(union(
          singleQuery(finish()),
          singleQuery(finish())
        ))
      case _ => _.toAst(union(
          singleQuery(finish()),
          singleQuery(finish()),
          differentReturnOrderAllowed = true
        ))
    }
  }

  test("FINISH UNION ALL FINISH") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(union(
          singleQuery(finish()),
          singleQuery(finish())
        ).all)
      case _ => _.toAst(union(
          singleQuery(finish()),
          singleQuery(finish()),
          differentReturnOrderAllowed = true
        ).all)
    }
  }

  // invalid Cypher accepted by parser
  test("FINISH UNION RETURN 2 AS a") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(union(
          singleQuery(finish()),
          singleQuery(return_(aliasedReturnItem(literal(2), "a")))
        ))
      case _ => _.toAst(union(
          singleQuery(finish()),
          singleQuery(return_(aliasedReturnItem(literal(2), "a"))),
          differentReturnOrderAllowed = true
        ))
    }
  }

  // invalid Cypher accepted by parser
  test("FINISH UNION DISTINCT RETURN 2 AS a") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(union(
          singleQuery(finish()),
          singleQuery(return_(aliasedReturnItem(literal(2), "a")))
        ))
      case _ => _.toAst(union(
          singleQuery(finish()),
          singleQuery(return_(aliasedReturnItem(literal(2), "a"))),
          differentReturnOrderAllowed = true
        ))
    }
  }

  // invalid Cypher accepted by parser
  test("FINISH UNION ALL RETURN 2 AS a") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(union(
          singleQuery(finish()),
          singleQuery(return_(aliasedReturnItem(literal(2), "a")))
        ).all)
      case _ => _.toAst(union(
          singleQuery(finish()),
          singleQuery(return_(aliasedReturnItem(literal(2), "a"))),
          differentReturnOrderAllowed = true
        ).all)
    }
  }

  test("RETURN 1 AS a UNION UNION RETURN 2 AS a") {
    failsParsing[Statements].in {
      case Cypher5JavaCc => _.withMessageStart("Invalid input 'UNION'")
      case _ => _.withSyntaxError(
          """Invalid input 'UNION': expected 'FOREACH', 'ORDER BY', 'CALL', 'CREATE', 'LOAD CSV', 'DELETE', 'DETACH', 'FINISH', 'INSERT', 'LIMIT', 'MATCH', 'MERGE', 'NODETACH', 'OFFSET', 'OPTIONAL', 'REMOVE', 'RETURN', 'SET', 'SKIP', 'UNWIND', 'USE' or 'WITH' (line 1, column 21 (offset: 20))
            |"RETURN 1 AS a UNION UNION RETURN 2 AS a"
            |                     ^""".stripMargin
        )
    }
  }

  test("RETURN 1 AS a UNION RETURN 2 AS a UNION RETURN 3 AS a") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a")))
            ),
            singleQuery(return_(aliasedReturnItem(literal(3), "a")))
          )
        )
      case _ => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a"))),
              differentReturnOrderAllowed = true
            ),
            singleQuery(return_(aliasedReturnItem(literal(3), "a"))),
            differentReturnOrderAllowed = true
          )
        )
    }
  }

  test("RETURN 1 AS a UNION DISTINCT RETURN 2 AS a UNION RETURN 3 AS a") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a")))
            ),
            singleQuery(return_(aliasedReturnItem(literal(3), "a")))
          )
        )
      case _ => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a"))),
              differentReturnOrderAllowed = true
            ),
            singleQuery(return_(aliasedReturnItem(literal(3), "a"))),
            differentReturnOrderAllowed = true
          )
        )
    }
  }

  test("RETURN 1 AS a UNION RETURN 2 AS a UNION DISTINCT RETURN 3 AS a") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a")))
            ),
            singleQuery(return_(aliasedReturnItem(literal(3), "a")))
          )
        )
      case _ => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a"))),
              differentReturnOrderAllowed = true
            ),
            singleQuery(return_(aliasedReturnItem(literal(3), "a"))),
            differentReturnOrderAllowed = true
          )
        )
    }
  }

  test("RETURN 1 AS a UNION DISTINCT RETURN 2 AS a UNION DISTINCT RETURN 3 AS a") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a")))
            ),
            singleQuery(return_(aliasedReturnItem(literal(3), "a")))
          )
        )
      case _ => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a"))),
              differentReturnOrderAllowed = true
            ),
            singleQuery(return_(aliasedReturnItem(literal(3), "a"))),
            differentReturnOrderAllowed = true
          )
        )
    }
  }

  test("RETURN 1 AS a UNION ALL RETURN 2 AS a UNION ALL RETURN 3 AS a") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a")))
            ).all,
            singleQuery(return_(aliasedReturnItem(literal(3), "a")))
          ).all
        )
      case _ => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a"))),
              differentReturnOrderAllowed = true
            ).all,
            singleQuery(return_(aliasedReturnItem(literal(3), "a"))),
            differentReturnOrderAllowed = true
          ).all
        )
    }
  }

  // invalid Cypher accepted by parser
  test("RETURN 1 AS a UNION RETURN 2 AS a UNION ALL RETURN 3 AS a") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a")))
            ),
            singleQuery(return_(aliasedReturnItem(literal(3), "a")))
          ).all
        )
      case _ => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a"))),
              differentReturnOrderAllowed = true
            ),
            singleQuery(return_(aliasedReturnItem(literal(3), "a"))),
            differentReturnOrderAllowed = true
          ).all
        )
    }
  }

  // invalid Cypher accepted by parser
  test("RETURN 1 AS a UNION DISTINCT RETURN 2 AS a UNION ALL RETURN 3 AS a") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a")))
            ),
            singleQuery(return_(aliasedReturnItem(literal(3), "a")))
          ).all
        )
      case _ => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a"))),
              differentReturnOrderAllowed = true
            ),
            singleQuery(return_(aliasedReturnItem(literal(3), "a"))),
            differentReturnOrderAllowed = true
          ).all
        )
    }
  }

  // invalid Cypher accepted by parser
  test("RETURN 1 AS a UNION ALL RETURN 2 AS a UNION RETURN 3 AS a") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a")))
            ).all,
            singleQuery(return_(aliasedReturnItem(literal(3), "a")))
          )
        )
      case _ => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a"))),
              differentReturnOrderAllowed = true
            ).all,
            singleQuery(return_(aliasedReturnItem(literal(3), "a"))),
            differentReturnOrderAllowed = true
          )
        )
    }
  }

  // invalid Cypher accepted by parser
  test("RETURN 1 AS a UNION ALL RETURN 2 AS a UNION DISTINCT RETURN 3 AS a") {
    parsesIn[Statement] {
      case Cypher6 => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a")))
            ).all,
            singleQuery(return_(aliasedReturnItem(literal(3), "a")))
          )
        )
      case _ => _.toAst(
          union(
            union(
              singleQuery(return_(aliasedReturnItem(literal(1), "a"))),
              singleQuery(return_(aliasedReturnItem(literal(2), "a"))),
              differentReturnOrderAllowed = true
            ).all,
            singleQuery(return_(aliasedReturnItem(literal(3), "a"))),
            differentReturnOrderAllowed = true
          )
        )
    }
  }
}
