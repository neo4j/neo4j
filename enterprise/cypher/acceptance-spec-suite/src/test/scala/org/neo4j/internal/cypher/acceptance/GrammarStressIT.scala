/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport, PatternGen}
import org.scalacheck.{Arbitrary, Gen, Shrink}

/*
 * Tests so that the compiled runtime behaves in the same way as the interpreted runtime for randomized Cypher
 * statements
 */
class GrammarStressIT extends ExecutionEngineFunSuite with PatternGen with NewPlannerTestSupport {

  private val DEPTH: Int = 10
  //we don't want scala check to shrink patterns here since it will lead to invalid cypher
  //e.g. RETURN {, RETURN [, etc
  implicit val dontShrink: Shrink[String] = Shrink(s => Stream.empty)

  test("literal stress test") {
    forAll(literal) { l =>
      whenever(l.nonEmpty) {
        withClue(s"failing on literal: $l") {
          assertQuery(s"RETURN $l")
        }
      }
    }
  }

  //Check that interpreted and compiled gives the same results, it might be the case
  //that the compiled runtime fallbacks to the interpreted but that is ok here just as
  //long as we fallback gracefully.
  private def assertQuery(query: String) = {
    val resultCompiled = innerExecute(s"CYPHER runtime=compiled $query")
    val resultInterpreted = innerExecute(s"CYPHER runtime=interpreted $query")
    assertResultsAreSame(resultCompiled, resultInterpreted, query,
                         "Diverging results between interpreted and compiled runtime")
  }

  def literal(implicit d: Int = DEPTH): Gen[String] =
    if (d == 0) Gen.oneOf(floatLiteral, stringLiteral, intLiteral, boolLiteral)
    else Gen.oneOf(floatLiteral, stringLiteral, intLiteral, boolLiteral, mapLiteral(d), listLiteral(d))

  def floatLiteral: Gen[String] = Arbitrary.arbitrary[Double].map(_.toString)

  def intLiteral: Gen[String] = Arbitrary.arbitrary[Long].map(_.toString)

  def boolLiteral: Gen[String] = Arbitrary.arbitrary[Boolean].map(_.toString)

  //remove non-printable characters
  def stringLiteral: Gen[String] = Arbitrary.arbitrary[String]
    .map(s => "'" + s.takeWhile(c => c != '\'' && Character.isISOControl(c)) + "'")

  def keyLiteral: Gen[String] = Gen.identifier

  def listLiteral(d: Int): Gen[String] = Gen.listOf(literal(d - 1)).map(_.mkString("[", ", ", "]"))

  def mapLiteral(d: Int): Gen[String] = Gen.mapOf(Gen.zip(keyLiteral, literal(d - 1)))
    .map(_.toList.map(kv => s"${kv._1}: ${kv._2}").mkString("{", ", ", "}"))

  override protected def numberOfTestRuns: Int = 100

  override def relGen: Gen[Relationship] = Gen.oneOf(emptyRelGen, emptyRelWithLengthGen, namedRelGen,
                                                     namedRelWithLengthGen, typedRelGen, typedRelWithLengthGen,
                                                     namedTypedRelGen, namedTypedRelWithLengthGen,
                                                     typedWithPropertiesRelGen, namedTypedWithPropertiesRelGen)

  override def nodeGen: Gen[Node] = Gen.oneOf(emptyNodeGen, namedNodeGen, labeledNodeGen, namedLabeledNodeGen,
                                              labeledWithPropertiesNodeGen, namedLabeledWithPropertiesNodeGen)
}
