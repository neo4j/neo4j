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

import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher6
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase

class PeriodicCommitParserTest extends AstParsingTestBase {

  val message =
    "The PERIODIC COMMIT query hint is no longer supported. Please use CALL { ... } IN TRANSACTIONS instead. (line 1, column 7 (offset: 6))"

  test("USING PERIODIC COMMIT LOAD CSV FROM 'foo' AS l RETURN l") {
    parsesIn[Statements] {
      case Cypher6 =>
        _.withSyntaxError("""Invalid input 'USING': expected 'FOREACH', 'ALTER', 'CALL', 'CREATE', 'LOAD CSV', 'START DATABASE', 'STOP DATABASE', 'DEALLOCATE', 'DELETE', 'DENY', 'DETACH', 'DROP', 'DRYRUN', 'FINISH', 'GRANT', 'INSERT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REALLOCATE', 'REMOVE', 'RENAME', 'RETURN', 'REVOKE', 'ENABLE SERVER', 'SET', 'SHOW', 'TERMINATE', 'UNWIND', 'USE' or 'WITH' (line 1, column 1 (offset: 0))
                            |"USING PERIODIC COMMIT LOAD CSV FROM 'foo' AS l RETURN l"
                            | ^""".stripMargin)
      case _ => _.withMessageStart(message)
    }
  }

  test("USING PERIODIC COMMIT 200 LOAD CSV FROM 'foo' AS l RETURN l") {
    parsesIn[Statements] {
      case Cypher6 =>
        _.withSyntaxError("""Invalid input 'USING': expected 'FOREACH', 'ALTER', 'CALL', 'CREATE', 'LOAD CSV', 'START DATABASE', 'STOP DATABASE', 'DEALLOCATE', 'DELETE', 'DENY', 'DETACH', 'DROP', 'DRYRUN', 'FINISH', 'GRANT', 'INSERT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REALLOCATE', 'REMOVE', 'RENAME', 'RETURN', 'REVOKE', 'ENABLE SERVER', 'SET', 'SHOW', 'TERMINATE', 'UNWIND', 'USE' or 'WITH' (line 1, column 1 (offset: 0))
                            |"USING PERIODIC COMMIT 200 LOAD CSV FROM 'foo' AS l RETURN l"
                            | ^""".stripMargin)
      case _ => _.withMessageStart(message)
    }
  }

  test("USING PERIODIC COMMIT RETURN 1") {
    parsesIn[Statements] {
      case Cypher6 =>
        _.withSyntaxError("""Invalid input 'USING': expected 'FOREACH', 'ALTER', 'CALL', 'CREATE', 'LOAD CSV', 'START DATABASE', 'STOP DATABASE', 'DEALLOCATE', 'DELETE', 'DENY', 'DETACH', 'DROP', 'DRYRUN', 'FINISH', 'GRANT', 'INSERT', 'MATCH', 'MERGE', 'NODETACH', 'OPTIONAL', 'REALLOCATE', 'REMOVE', 'RENAME', 'RETURN', 'REVOKE', 'ENABLE SERVER', 'SET', 'SHOW', 'TERMINATE', 'UNWIND', 'USE' or 'WITH' (line 1, column 1 (offset: 0))
                            |"USING PERIODIC COMMIT RETURN 1"
                            | ^""".stripMargin)
      case _ => _.withMessageStart(message)
    }
  }
}
