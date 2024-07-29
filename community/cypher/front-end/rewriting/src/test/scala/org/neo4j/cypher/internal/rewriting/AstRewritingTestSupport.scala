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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.CypherVersion
import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.parser.AstParserFactory
import org.neo4j.cypher.internal.util.CypherExceptionFactory

trait AstRewritingTestSupport extends AstConstructionTestSupport {

  def parse(query: String, exceptionFactory: CypherExceptionFactory): Statement = {
    val defaultStatement = parse(CypherVersion.Default, query, exceptionFactory)

    // Quick and dirty hack to try to make sure we have sufficient coverage of all cypher versions.
    // Feel free to improve ¯\_(ツ)_/¯.
    CypherVersion.values().foreach { version =>
      if (version != CypherVersion.Default) {
        val otherStatement = parse(version, query, exceptionFactory)
        if (otherStatement != defaultStatement) {
          throw new AssertionError(
            s"""Query parse differently in $version
               |Default statement: $defaultStatement
               |$version statement: $otherStatement
               |""".stripMargin
          )
        }
      }
    }
    defaultStatement
  }

  def parse(version: CypherVersion, query: String, exceptionFactory: CypherExceptionFactory): Statement = {
    AstParserFactory(version)(query, exceptionFactory, None).singleStatement()
  }
}
