/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.ast.factory.neo4j

import org.neo4j.cypher.internal.ast.AstConstructionTestSupport
import org.neo4j.cypher.internal.ast.SeekOnly
import org.neo4j.cypher.internal.ast.SeekOrScan
import org.neo4j.cypher.internal.ast.UsingAnyIndexType
import org.neo4j.cypher.internal.ast.UsingBtreeIndexType
import org.neo4j.cypher.internal.ast.UsingIndexHint
import org.neo4j.cypher.internal.ast.UsingPointIndexType
import org.neo4j.cypher.internal.ast.UsingRangeIndexType
import org.neo4j.cypher.internal.ast.UsingTextIndexType
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

import scala.reflect.ClassTag

class HintsParserTest extends CypherFunSuite with TestName with AstConstructionTestSupport {

  test("MATCH (n) USING INDEX n:N(p)") {
    parseAndFind[UsingIndexHint](testName) shouldBe Seq(
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingAnyIndexType)(pos)
    )
  }

  test("MATCH (n) USING INDEX SEEK n:N(p)") {
    parseAndFind[UsingIndexHint](testName) shouldBe Seq(
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingAnyIndexType)(pos)
    )
  }

  test("MATCH (n) USING BTREE INDEX n:N(p)") {
    parseAndFind[UsingIndexHint](testName) shouldBe Seq(
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingBtreeIndexType)(pos)
    )
  }

  test("MATCH (n) USING BTREE INDEX SEEK n:N(p)") {
    parseAndFind[UsingIndexHint](testName) shouldBe Seq(
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingBtreeIndexType)(pos)
    )
  }

  test("MATCH (n) USING RANGE INDEX n:N(p)") {
    parseAndFind[UsingIndexHint](testName) shouldBe Seq(
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingRangeIndexType)(pos)
    )
  }

  test("MATCH (n) USING RANGE INDEX SEEK n:N(p)") {
    parseAndFind[UsingIndexHint](testName) shouldBe Seq(
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingRangeIndexType)(pos)
    )
  }

  test("MATCH (n) USING POINT INDEX n:N(p)") {
    parseAndFind[UsingIndexHint](testName) shouldBe Seq(
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingPointIndexType)(pos)
    )
  }

  test("MATCH (n) USING POINT INDEX SEEK n:N(p)") {
    parseAndFind[UsingIndexHint](testName) shouldBe Seq(
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingPointIndexType)(pos)
    )
  }

  test("MATCH (n) USING TEXT INDEX n:N(p)") {
    parseAndFind[UsingIndexHint](testName) shouldBe Seq(
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingTextIndexType)(pos)
    )
  }

  test("MATCH (n) USING TEXT INDEX SEEK n:N(p)") {
    parseAndFind[UsingIndexHint](testName) shouldBe Seq(
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingTextIndexType)(pos)
    )
  }

  test("can parse multiple hints") {
    parseAndFind[UsingIndexHint](
      """MATCH (n)
        |USING INDEX n:N(p)
        |USING INDEX SEEK n:N(p)
        |USING BTREE INDEX n:N(p)
        |USING BTREE INDEX SEEK n:N(p)
        |USING TEXT INDEX n:N(p)
        |USING TEXT INDEX SEEK n:N(p)
        |USING RANGE INDEX n:N(p)
        |USING RANGE INDEX SEEK n:N(p)
        |USING POINT INDEX n:N(p)
        |USING POINT INDEX SEEK n:N(p)
        |""".stripMargin
    ) shouldBe Seq(
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingAnyIndexType)(pos),
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingAnyIndexType)(pos),
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingBtreeIndexType)(pos),
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingBtreeIndexType)(pos),
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingTextIndexType)(pos),
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingTextIndexType)(pos),
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingRangeIndexType)(pos),
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingRangeIndexType)(pos),
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOrScan, UsingPointIndexType)(pos),
      UsingIndexHint(varFor("n"), labelOrRelTypeName("N"), Seq(propName("p")), SeekOnly, UsingPointIndexType)(pos),
    )
  }

  private val exceptionFactory = OpenCypherExceptionFactory(None)

  private def parseAndFind[T: ClassTag](query: String): Seq[T] = {
    val ast = JavaCCParser.parse(query, exceptionFactory, new AnonymousVariableNameGenerator())
    ast.findAllByClass[T]
  }
}
