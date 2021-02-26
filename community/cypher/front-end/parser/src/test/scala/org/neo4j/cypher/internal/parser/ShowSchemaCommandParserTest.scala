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

import org.neo4j.cypher.internal.ast
import org.neo4j.cypher.internal.ast.AllConstraints
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.ExistsConstraints
import org.neo4j.cypher.internal.ast.NodeExistsConstraints
import org.neo4j.cypher.internal.ast.NodeKeyConstraints
import org.neo4j.cypher.internal.ast.RelExistsConstraints
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.parboiled.scala.Rule1

class ShowSchemaCommandParserTest
  extends ParserAstTest[ast.Statement]
    with Statement
    with AstConstructionTestSupport {

  implicit val parser: Rule1[ast.Statement] = Statement

    // Show indexes

    Seq("INDEX", "INDEXES").foreach { indexKeyword =>

      // No explicit output

      test(s"SHOW $indexKeyword") {
        yields(_ => query(ShowIndexesClause(all = true, brief = false, verbose = false, None, hasYield = false)(pos)))
      }

      test(s"SHOW ALL $indexKeyword") {
        yields(_ => query(ShowIndexesClause(all = true, brief = false, verbose = false, None, hasYield = false)(pos)))
      }

      test(s"SHOW BTREE $indexKeyword") {
        yields(_ => query(ShowIndexesClause(all = false, brief = false, verbose = false, None, hasYield = false)(pos)))
      }

      test(s"USE db SHOW $indexKeyword") {
        yields(_ => query(use(varFor("db")), ShowIndexesClause(all = true, brief = false, verbose = false, None, hasYield = false)(pos)))
      }

      // Brief output (deprecated)

      test(s"SHOW $indexKeyword BRIEF") {
        yields(_ => query(ShowIndexesClause(all = true, brief = true, verbose = false, None, hasYield = false)(pos)))
      }

      test(s"SHOW $indexKeyword BRIEF OUTPUT") {
        yields(_ => query(ShowIndexesClause(all = true, brief = true, verbose = false, None, hasYield = false)(pos)))
      }

      test(s"SHOW ALL $indexKeyword BRIEF") {
        yields(_ => query(ShowIndexesClause(all = true, brief = true, verbose = false, None, hasYield = false)(pos)))
      }

      test(s"SHOW  ALL $indexKeyword BRIEF OUTPUT") {
        yields(_ => query(ShowIndexesClause(all = true, brief = true, verbose = false, None, hasYield = false)(pos)))
      }

      test(s"SHOW BTREE $indexKeyword BRIEF") {
        yields(_ => query(ShowIndexesClause(all = false, brief = true, verbose = false, None, hasYield = false)(pos)))
      }

      // Verbose output (deprecated)

      test(s"SHOW $indexKeyword VERBOSE") {
        yields(_ => query(ShowIndexesClause(all = true, brief = false, verbose = true, None, hasYield = false)(pos)))
      }

      test(s"SHOW ALL $indexKeyword VERBOSE") {
        yields(_ => query(ShowIndexesClause(all = true, brief = false, verbose = true, None, hasYield = false)(pos)))
      }

      test(s"SHOW BTREE $indexKeyword VERBOSE OUTPUT") {
        yields(_ => query(ShowIndexesClause(all = false, brief = false, verbose = true, None, hasYield = false)(pos)))
      }
    }

    // Show indexes filtering

    test("SHOW INDEX WHERE uniqueness = 'UNIQUE'") {
      yields(_ => query(ShowIndexesClause(all = true, brief = false, verbose = false, Some(where(equals(varFor("uniqueness"), literalString("UNIQUE")))), hasYield = false)(pos)))
    }

    test("SHOW INDEXES YIELD populationPercent") {
      yields(_ => query(ShowIndexesClause(all = true, brief = false, verbose = false, None, hasYield = true)(pos), yieldClause(returnItems(variableReturnItem("populationPercent")))))
    }

    test("SHOW BTREE INDEXES YIELD *") {
      yields(_ => query(ShowIndexesClause(all = false, brief = false, verbose = false, None, hasYield = true)(pos), yieldClause(returnAllItems)))
    }

    test("USE db SHOW BTREE INDEXES YIELD name, populationPercent AS pp WHERE pp < 50.0 RETURN name") {
      yields(_ => query(
        use(varFor("db")),
        ShowIndexesClause(all = false, brief = false, verbose = false, None, hasYield = true)(pos),
        yieldClause(returnItems(variableReturnItem("name"), aliasedReturnItem("populationPercent", "pp")),
          where = Some(where(lessThan(varFor("pp"), literalFloat(50.0))))),
        return_(variableReturnItem("name"))
      ))
    }

    test("SHOW INDEXES YIELD name AS INDEX, type AS OUTPUT") {
      yields(_ => query(ShowIndexesClause(all = true, brief = false, verbose = false, None, hasYield = true)(pos),
        yieldClause(returnItems(aliasedReturnItem("name", "INDEX"), aliasedReturnItem("type", "OUTPUT")))))
    }

    test("SHOW INDEXES WHERE name = 'GRANT'") {
      yields(_ => query(ShowIndexesClause(all = true, brief = false, verbose = false,
        Some(where(equals(varFor("name"), literalString("GRANT")))), hasYield = false)(pos)))
    }

    // Negative tests for show indexes

    test("SHOW ALL BTREE INDEXES") {
      failsToParse
    }

    test("SHOW INDEX OUTPUT") {
      failsToParse
    }

    test("SHOW INDEX YIELD") {
      failsToParse
    }

    test("SHOW INDEX VERBOSE BRIEF OUTPUT") {
      failsToParse
    }

    test("SHOW INDEXES BRIEF YIELD *") {
      failsToParse
    }

    test("SHOW INDEXES VERBOSE YIELD *") {
      failsToParse
    }

    test("SHOW INDEXES BRIEF WHERE uniqueness = 'UNIQUE'") {
      failsToParse
    }

    test("SHOW INDEXES VERBOSE WHERE uniqueness = 'UNIQUE'") {
      failsToParse
    }

    test("SHOW INDEXES YIELD * YIELD *") {
      failsToParse
    }

    test("SHOW INDEXES WHERE uniqueness = 'UNIQUE' YIELD *") {
      failsToParse
    }

    test("SHOW INDEXES YIELD a b RETURN *") {
      failsToParse
    }

  // Show constraints

  Seq("CONSTRAINT", "CONSTRAINTS").foreach {
    constraintKeyword =>

      Seq(
        ("", false),
        (" BRIEF", false),
        (" BRIEF OUTPUT", false),
        (" VERBOSE", true),
        (" VERBOSE OUTPUT", true)
      ).foreach {
        case (output, verbose) =>

          test(s"SHOW $constraintKeyword$output") {
            yields(ast.ShowConstraints(constraintType = AllConstraints, verbose = verbose))
          }

          test(s"SHOW ALL $constraintKeyword$output") {
            yields(ast.ShowConstraints(constraintType = AllConstraints, verbose = verbose))
          }

          test(s"SHOW UNIQUE $constraintKeyword$output") {
            yields(ast.ShowConstraints(constraintType = UniqueConstraints, verbose = verbose))
          }

          test(s"SHOW EXIST $constraintKeyword$output") {
            yields(ast.ShowConstraints(constraintType = ExistsConstraints, verbose = verbose))
          }

          test(s"SHOW EXISTS $constraintKeyword$output") {
            yields(ast.ShowConstraints(constraintType = ExistsConstraints, verbose = verbose))
          }

          test(s"SHOW NODE EXIST $constraintKeyword$output") {
            yields(ast.ShowConstraints(constraintType = NodeExistsConstraints, verbose = verbose))
          }

          test(s"SHOW NODE EXISTS $constraintKeyword$output") {
            yields(ast.ShowConstraints(constraintType = NodeExistsConstraints, verbose = verbose))
          }

          test(s"SHOW RELATIONSHIP EXIST $constraintKeyword$output") {
            yields(ast.ShowConstraints(constraintType = RelExistsConstraints, verbose = verbose))
          }

          test(s"SHOW RELATIONSHIP EXISTS $constraintKeyword$output") {
            yields(ast.ShowConstraints(constraintType = RelExistsConstraints, verbose = verbose))
          }

          test(s"SHOW NODE KEY $constraintKeyword$output") {
            yields(ast.ShowConstraints(constraintType = NodeKeyConstraints, verbose = verbose))
          }
      }
  }

  // Negative tests for show constraints

  test("SHOW ALL EXISTS CONSTRAINTS") {
    failsToParse
  }

  test("SHOW UNIQUENESS CONSTRAINTS") {
    failsToParse
  }

  test("SHOW NODE CONSTRAINTS") {
    failsToParse
  }

  test("SHOW EXISTS NODE CONSTRAINTS") {
    failsToParse
  }

  test("SHOW NODES EXIST CONSTRAINTS") {
    failsToParse
  }

  test("SHOW RELATIONSHIP CONSTRAINTS") {
    failsToParse
  }

  test("SHOW EXISTS RELATIONSHIP CONSTRAINTS") {
    failsToParse
  }

  test("SHOW RELATIONSHIPS EXIST CONSTRAINTS") {
    failsToParse
  }

  test("SHOW KEY CONSTRAINTS") {
    failsToParse
  }

  test("SHOW CONSTRAINTS OUTPUT") {
    failsToParse
  }

  test("SHOW CONSTRAINTS VERBOSE BRIEF OUTPUT") {
    failsToParse
  }

  // Show constraints filtering is not supported

  test("SHOW CONSTRAINTS WHERE uniqueness = 'UNIQUE'") {
    failsToParse
  }

  test("SHOW ALL CONSTRAINTS YIELD populationPercent") {
    failsToParse
  }

  test("SHOW EXISTS CONSTRAINTS VERBOSE YIELD *") {
    failsToParse
  }

}
