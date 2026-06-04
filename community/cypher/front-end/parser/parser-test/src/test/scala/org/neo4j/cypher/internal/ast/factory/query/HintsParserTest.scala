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

import org.neo4j.cypher.internal.ast.ExpandHintAll
import org.neo4j.cypher.internal.ast.ExpandHintInto
import org.neo4j.cypher.internal.ast.ExpandStep
import org.neo4j.cypher.internal.ast.Statements
import org.neo4j.cypher.internal.ast.UsingExpandHint
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingIndexHint.SeekOnly
import org.neo4j.cypher.internal.ast.UsingIndexHint.SeekOrScan
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingAnyIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingPointIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingRangeIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint.UsingTextIndexType
import org.neo4j.cypher.internal.ast.test.util.AstParsing.Cypher5
import org.neo4j.cypher.internal.ast.test.util.AstParsingTestBase
import org.neo4j.cypher.internal.util.NonEmptyList
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
    failsParsing[Statements].in {
      case Cypher5 =>
        _.withOldSyntax(
          "Index type BTREE is no longer supported for USING index hint. Use TEXT, RANGE or POINT instead."
        )
      case _ => _.throws[SyntaxException].withMessageStart("Invalid input 'BTREE'")
    }
  }

  test("MATCH (n) USING BTREE INDEX SEEK n:N(p)") {
    failsParsing[Statements].in {
      case Cypher5 =>
        _.withOldSyntax(
          "Index type BTREE is no longer supported for USING index hint. Use TEXT, RANGE or POINT instead."
        )
      case _ => _.throws[SyntaxException].withMessageStart("Invalid input 'BTREE'")
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

  test("MATCH (a)-->(b) USING EXPAND FROM a TO b") {
    parses[Statements].containing[UsingExpandHint](
      UsingExpandHint(NonEmptyList(ExpandStep.byEndpoints(varFor("a"), varFor("b"), mode = None)(pos)))(pos)
    )
  }

  test("MATCH (a)-->(b) USING EXPAND ALL FROM a TO b") {
    parses[Statements].containing[UsingExpandHint](
      UsingExpandHint(NonEmptyList(ExpandStep.byEndpoints(
        varFor("a"),
        varFor("b"),
        mode = Some(ExpandHintAll)
      )(pos)))(pos)
    )
  }

  test("MATCH (a)-->(b) USING EXPAND INTO FROM a TO b") {
    parses[Statements].containing[UsingExpandHint](
      UsingExpandHint(NonEmptyList(ExpandStep.byEndpoints(
        varFor("a"),
        varFor("b"),
        mode = Some(ExpandHintInto)
      )(pos)))(pos)
    )
  }

  test("MATCH (a)-->(b)-->(c) USING EXPAND FROM a TO b, FROM b TO c") {
    parses[Statements].containing[UsingExpandHint](
      UsingExpandHint(NonEmptyList(
        ExpandStep.byEndpoints(varFor("a"), varFor("b"), mode = None)(pos),
        ExpandStep.byEndpoints(varFor("b"), varFor("c"), mode = None)(pos)
      ))(pos)
    )
  }

  test("MATCH (expand)-->(into) RETURN expand, into") {
    // accepts expand and into as identifiers
    parses[Statements]
  }

  test("MATCH (a)-[r]->(b) USING EXPAND FROM a TO b VIA r") {
    parses[Statements].containing[UsingExpandHint](
      UsingExpandHint(NonEmptyList(
        ExpandStep(Some(varFor("a")), Some(varFor("b")), Some(varFor("r")), None)(pos)
      ))(pos)
    )
  }

  test("MATCH (a)-[r]->(b) USING EXPAND VIA r") {
    parses[Statements].containing[UsingExpandHint](
      UsingExpandHint(NonEmptyList(
        ExpandStep(None, None, Some(varFor("r")), None)(pos)
      ))(pos)
    )
  }

  test("MATCH (a)-[r]->(b) USING EXPAND INTO VIA r") {
    parses[Statements].containing[UsingExpandHint](
      UsingExpandHint(NonEmptyList(
        ExpandStep(None, None, Some(varFor("r")), Some(ExpandHintInto))(pos)
      ))(pos)
    )
  }

  test("MATCH (a)-[r]->(b) USING EXPAND ALL VIA r") {
    parses[Statements].containing[UsingExpandHint](
      UsingExpandHint(NonEmptyList(
        ExpandStep(None, None, Some(varFor("r")), Some(ExpandHintAll))(pos)
      ))(pos)
    )
  }

  test("MATCH (a)-[r]->(b)-[s]->(c) USING EXPAND FROM a TO b VIA r, VIA s") {
    parses[Statements].containing[UsingExpandHint](
      UsingExpandHint(NonEmptyList(
        ExpandStep(Some(varFor("a")), Some(varFor("b")), Some(varFor("r")), None)(pos),
        ExpandStep(None, None, Some(varFor("s")), None)(pos)
      ))(pos)
    )
  }

  test("MATCH (via)-->(a) RETURN via, a") {
    // VIA is a non-reserved keyword in both Cypher 5 and Cypher 25
    parses[Statements]
  }

  test("MATCH (a)-->(b) USING EXPAND ALL RETURN *") {
    // Grammar requires at least one of FROM/TO or VIA per expandHintStep,
    // so `USING EXPAND ALL` on its own is rejected at parse time.
    failsParsing[Statements]
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
