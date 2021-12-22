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
import org.neo4j.cypher.internal.expressions.NodePattern
import org.neo4j.cypher.internal.util.AnonymousVariableNameGenerator
import org.neo4j.cypher.internal.util.OpenCypherExceptionFactory
import org.neo4j.cypher.internal.util.test_helpers.CypherFunSuite
import org.neo4j.cypher.internal.util.test_helpers.TestName

class NodeLabelExpressionsJavaCcParserTest extends CypherFunSuite with TestName with AstConstructionTestSupport {

  test("MATCH (n)") {
    parseNodePatterns(testName) shouldBe Seq(
      NodePattern(
        Some(varFor("n")),
        Seq.empty,
        None,
        None,
        None
      )(pos)
    )
  }

  test("MATCH (n:A)") {
    parseNodePatterns(testName) shouldBe Seq(
      NodePattern(
        Some(varFor("n")),
        Seq(labelName("A")),
        None,
        None,
        None
      )(pos)
    )
  }

  test("MATCH (n:A&B)") {
    parseNodePatterns(testName) shouldBe Seq(
      NodePattern(
        Some(varFor("n")),
        Seq.empty,
        Some(
          labelConjunction(
            labelAtom("A"),
            labelAtom("B")
          )
        ),
        None,
        None
      )(pos)
    )
  }

  test("MATCH (n:A&B|C)") {
    parseNodePatterns(testName) shouldBe Seq(
      NodePattern(
        Some(varFor("n")),
        Seq.empty,
        Some(
          labelDisjunction(
            labelConjunction(
              labelAtom("A"),
              labelAtom("B")
            ),
            labelAtom("C")
          ),
        ),
        None,
        None
      )(pos)
    )
  }

  test("MATCH (n:A|B&C)") {
    parseNodePatterns(testName) shouldBe Seq(
      NodePattern(
        Some(varFor("n")),
        Seq.empty,
        Some(
          labelDisjunction(
            labelAtom("A"),
            labelConjunction(
              labelAtom("B"),
              labelAtom("C"),
            ),
          ),
        ),
        None,
        None
      )(pos)
    )
  }

  test("MATCH (n:!(A))") {
    parseNodePatterns(testName) shouldBe Seq(
      NodePattern(
        Some(varFor("n")),
        Seq.empty,
        Some(
          labelNegation(
            labelAtom("A"),
          ),
        ),
        None,
        None
      )(pos)
    )
  }

  test("MATCH (:A&B)") {
    parseNodePatterns(testName) shouldBe Seq(
      NodePattern(
        None,
        Seq.empty,
        Some(
          labelConjunction(
            labelAtom("A"),
            labelAtom("B")
          )
        ),
        None,
        None
      )(pos)
    )
  }

  test("MATCH (n:A|B)") {
    parseNodePatterns(testName) shouldBe Seq(
      NodePattern(
        Some(varFor("n")),
        Seq.empty,
        Some(
          labelDisjunction(
            labelAtom("A"),
            labelAtom("B")
          )
        ),
        None,
        None
      )(pos)
    )
  }

  test("MATCH (n:!A)") {
    parseNodePatterns(testName) shouldBe Seq(
      NodePattern(
        Some(varFor("n")),
        Seq.empty,
        Some(
          labelNegation(
            labelAtom("A")
          )
        ),
        None,
        None
      )(pos)
    )
  }

  test("MATCH (n:A&B&C)") {
    parseNodePatterns(testName) shouldBe Seq(
      NodePattern(
        Some(varFor("n")),
        Seq.empty,
        Some(
          labelConjunction(
            labelConjunction(
              labelAtom("A"),
              labelAtom("B"),
            ),
            labelAtom("C")
          )
        ),
        None,
        None
      )(pos)
    )
  }

  test("MATCH (n:!A&B)") {
    parseNodePatterns(testName) shouldBe Seq(
      NodePattern(
        Some(varFor("n")),
        Seq.empty,
        Some(
          labelConjunction(
            labelNegation(labelAtom("A")),
            labelAtom("B"),
          )
        ),
        None,
        None
      )(pos)
    )
  }

  test("MATCH (n:A&(B&C))") {
    parseNodePatterns(testName) shouldBe Seq(
      NodePattern(
        Some(varFor("n")),
        Seq.empty,
        Some(
          labelConjunction(
            labelAtom("A"),
            labelConjunction(
              labelAtom("B"),
              labelAtom("C"),
            )
          )
        ),
        None,
        None
      )(pos)
    )
  }

  test("MATCH (n:!(A&B))") {
    parseNodePatterns(testName) shouldBe Seq(
      NodePattern(
        Some(varFor("n")),
        Seq.empty,
        Some(
          labelNegation(
            labelConjunction(
              labelAtom("A"),
              labelAtom("B"),
            )
          )
        ),
        None,
        None
      )(pos)
    )
  }

  test("MATCH (n:(A&B)|C)") {
    parseNodePatterns(testName) shouldBe Seq(
      NodePattern(
        Some(varFor("n")),
        Seq.empty,
        Some(
          labelDisjunction(
            labelConjunction(
              labelAtom("A"),
              labelAtom("B"),
            ),
            labelAtom("C")
          )
        ),
        None,
        None
      )(pos)
    )
  }

  private val exceptionFactory = OpenCypherExceptionFactory(None)

  private def parseNodePatterns(query: String): Seq[NodePattern] = {
    val ast = JavaCCParser.parse(query, exceptionFactory, new AnonymousVariableNameGenerator())
    ast.findAllByClass[NodePattern]
  }
}
