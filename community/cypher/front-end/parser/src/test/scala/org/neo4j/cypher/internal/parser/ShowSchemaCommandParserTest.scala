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
import org.neo4j.cypher.internal.ast.UniqueConstraints
import org.parboiled.scala.Rule1

class ShowSchemaCommandParserTest
  extends ParserAstTest[ast.SchemaCommand]
    with SchemaCommand
    with AstConstructionTestSupport {

  implicit val parser: Rule1[ast.SchemaCommand] = SchemaCommand

  // Show indexes

  Seq("INDEX", "INDEXES").foreach { indexKeyword =>

    // Default values

    test(s"SHOW $indexKeyword") {
      yields(ast.ShowIndexes(all = true, verbose = false))
    }

    test(s"SHOW ALL $indexKeyword") {
      yields(ast.ShowIndexes(all = true, verbose = false))
    }

    test(s"SHOW $indexKeyword BRIEF") {
      yields(ast.ShowIndexes(all = true, verbose = false))
    }

    test(s"SHOW $indexKeyword BRIEF OUTPUT") {
      yields(ast.ShowIndexes(all = true, verbose = false))
    }

    test(s"SHOW ALL $indexKeyword BRIEF") {
      yields(ast.ShowIndexes(all = true, verbose = false))
    }

    test(s"SHOW  ALL $indexKeyword BRIEF OUTPUT") {
      yields(ast.ShowIndexes(all = true, verbose = false))
    }

    // Non-default values

    test(s"SHOW BTREE $indexKeyword") {
      yields(ast.ShowIndexes(all = false, verbose = false))
    }

    test(s"SHOW BTREE $indexKeyword BRIEF") {
      yields(ast.ShowIndexes(all = false, verbose = false))
    }

    test(s"SHOW $indexKeyword VERBOSE") {
      yields(ast.ShowIndexes(all = true, verbose = true))
    }

    test(s"SHOW ALL $indexKeyword VERBOSE") {
      yields(ast.ShowIndexes(all = true, verbose = true))
    }

    test(s"SHOW BTREE $indexKeyword VERBOSE OUTPUT") {
      yields(ast.ShowIndexes(all = false, verbose = true))
    }
  }

  // Negative tests for show indexes

  test("SHOW ALL BTREE INDEXES") {
    failsToParse
  }

  test("SHOW INDEX OUTPUT") {
    failsToParse
  }

  test("SHOW INDEX VERBOSE BRIEF OUTPUT") {
    failsToParse
  }

  // Show indexes filtering is not supported

  test("SHOW INDEX WHERE uniqueness = 'UNIQUE'") {
    failsToParse
  }

  test("SHOW INDEXES YIELD populationPercent") {
    failsToParse
  }

  test("SHOW BTREE INDEXES YIELD *") {
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
        case (indexOutput, verbose) =>

          test(s"SHOW $constraintKeyword$indexOutput") {
            yields(ast.ShowConstraints(constraintType = AllConstraints, verbose = verbose))
          }

          test(s"SHOW ALL $constraintKeyword$indexOutput") {
            yields(ast.ShowConstraints(constraintType = AllConstraints, verbose = verbose))
          }

          test(s"SHOW UNIQUE $constraintKeyword$indexOutput") {
            yields(ast.ShowConstraints(constraintType = UniqueConstraints, verbose = verbose))
          }

          test(s"SHOW EXIST $constraintKeyword$indexOutput") {
            yields(ast.ShowConstraints(constraintType = ExistsConstraints, verbose = verbose))
          }

          test(s"SHOW EXISTS $constraintKeyword$indexOutput") {
            yields(ast.ShowConstraints(constraintType = ExistsConstraints, verbose = verbose))
          }

          test(s"SHOW NODE EXIST $constraintKeyword$indexOutput") {
            yields(ast.ShowConstraints(constraintType = NodeExistsConstraints, verbose = verbose))
          }

          test(s"SHOW NODE EXISTS $constraintKeyword$indexOutput") {
            yields(ast.ShowConstraints(constraintType = NodeExistsConstraints, verbose = verbose))
          }

          test(s"SHOW RELATIONSHIP EXIST $constraintKeyword$indexOutput") {
            yields(ast.ShowConstraints(constraintType = RelExistsConstraints, verbose = verbose))
          }

          test(s"SHOW RELATIONSHIP EXISTS $constraintKeyword$indexOutput") {
            yields(ast.ShowConstraints(constraintType = RelExistsConstraints, verbose = verbose))
          }

          test(s"SHOW NODE KEY $constraintKeyword$indexOutput") {
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
