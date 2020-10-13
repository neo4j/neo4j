/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
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

    test(s"SHOW  $indexKeyword BRIEF OUTPUT") {
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

    test(s"SHOW $indexKeyword VERBOSE") {
      yields(ast.ShowIndexes(all = true, verbose = true))
    }

    test(s"SHOW BTREE $indexKeyword VERBOSE OUTPUT") {
      yields(ast.ShowIndexes(all = false, verbose = true))
    }
  }

  // Negative tests

  test("SHOW ALL BTREE INDEXES") {
    failsToParse
  }

  test("SHOW INDEX OUTPUT") {
    failsToParse
  }

  test("SHOW INDEX VERBOSE BRIEF OUTPUT") {
    failsToParse
  }

  // Filtering is not supported

  test("SHOW INDEX WHERE uniqueness = 'UNIQUE'") {
    failsToParse
  }

  test("SHOW INDEXES YIELD populationPercent") {
    failsToParse
  }

  test("SHOW BTREE INDEXES YIELD *") {
    failsToParse
  }
}
