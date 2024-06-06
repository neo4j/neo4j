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
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingIndexHint.SeekOnly
import org.neo4j.cypher.internal.ast.UsingIndexHint.SeekOrScan
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingAnyIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingPointIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingRangeIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingTextIndexType
import org.neo4j.cypher.internal.ast.factory.neo4j.Neo4jASTConstructionException
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5JavaCc
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.exceptions.SyntaxException

class HintsParserTest extends AstParsingTestBase {

  test("MATCH (n) USING INDEX n:N(p)") {
    parses[Statements].containing[UsingIndexHint](
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingAnyIndexType)(pos)
    )
  }

  test("MATCH (n) USING INDEX SEEK n:N(p)") {
    parses[Statements].containing[UsingIndexHint](
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingAnyIndexType)(pos)
    )
  }

  test("MATCH (n) USING BTREE INDEX n:N(p)") {
    failsParsing[Statements]
      .withMessageStart("Index type BTREE is no longer supported for USING index hint. Use TEXT, RANGE or POINT")
      .in {
        case Cypher5JavaCc => _.throws[Neo4jASTConstructionException]
        case _             => _.throws[SyntaxException]
      }
  }

  test("MATCH (n) USING BTREE INDEX SEEK n:N(p)") {
    failsParsing[Statements]
      .withMessageStart("Index type BTREE is no longer supported for USING index hint. Use TEXT, RANGE or POINT")
      .in {
        case Cypher5JavaCc => _.throws[Neo4jASTConstructionException]
        case _             => _.throws[SyntaxException]
      }
  }

  test("MATCH (n) USING RANGE INDEX n:N(p)") {
    parses[Statements].containing[UsingIndexHint](
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingRangeIndexType)(pos)
    )
  }

  test("MATCH (n) USING RANGE INDEX SEEK n:N(p)") {
    parses[Statements].containing[UsingIndexHint](
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingRangeIndexType)(pos)
    )
  }

  test("MATCH (n) USING POINT INDEX n:N(p)") {
    parses[Statements].containing[UsingIndexHint](
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingPointIndexType)(pos)
    )
  }

  test("MATCH (n) USING POINT INDEX SEEK n:N(p)") {
    parses[Statements].containing[UsingIndexHint](
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingPointIndexType)(pos)
    )
  }

  test("MATCH (n) USING TEXT INDEX n:N(p)") {
    parses[Statements].containing[UsingIndexHint](
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingTextIndexType)(pos)
    )
  }

  test("MATCH (n) USING TEXT INDEX SEEK n:N(p)") {
    parses[Statements].containing[UsingIndexHint](
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingTextIndexType)(pos)
    )
  }

  test("can parse multiple hints") {
    """MATCH (n)
      |USING INDEX n:N(p)
      |USING INDEX SEEK n:N(p)
      |USING TEXT INDEX n:N(p)
      |USING TEXT INDEX SEEK n:N(p)
      |USING RANGE INDEX n:N(p)
      |USING RANGE INDEX SEEK n:N(p)
      |USING POINT INDEX n:N(p)
      |USING POINT INDEX SEEK n:N(p)
      |""".stripMargin should
      parse[Statements].containing[UsingIndexHint](
        UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingAnyIndexType)(pos),
        UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingAnyIndexType)(pos),
        UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingTextIndexType)(pos),
        UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingTextIndexType)(pos),
        UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingRangeIndexType)(pos),
        UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingRangeIndexType)(pos),
        UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingPointIndexType)(pos),
        UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingPointIndexType)(pos)
      )
  }
}
