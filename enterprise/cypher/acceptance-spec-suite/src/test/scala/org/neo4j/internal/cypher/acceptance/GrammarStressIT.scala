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

import org.neo4j.cypher.{ExecutionEngineFunSuite, NewPlannerTestSupport}
import org.neo4j.graphdb.Node
import org.scalacheck.{Arbitrary, Gen, Shrink}
import org.scalatest.prop.PropertyChecks

/*
 * Tests so that the compiled runtime behaves in the same way as the interpreted runtime for randomized Cypher
 * statements
 */
class GrammarStressIT extends ExecutionEngineFunSuite with PropertyChecks with NewPlannerTestSupport {

  private val DEPTH: Int = 10
  private def minPatternLength = 2
  private def maxPatternLength = 8
  private def numberOfTestRuns = 100
  private def maxDiscardedInputs = 500
  private def maxSize = 10

  val NODES_PER_LAYER = 10
  val NUM_LAYERS = 3

  override implicit val generatorDrivenConfig = PropertyCheckConfig(
    minSuccessful = numberOfTestRuns, maxDiscarded = maxDiscardedInputs, maxSize = maxSize
  )

  case class Pattern(startNode:NodePattern, tail:Option[(RelPattern, Pattern)]) {

    override def toString: String = {
      val tailString = tail match {
        case None => ""
        case Some((r,p)) => s"$r$p"
      }

      s"$startNode$tailString"
    }
  }
  case class NodePattern(name:Option[String], labels:Set[String], properties:Map[String, Any]) {

    override def toString: String = {

      val propString = if (properties.isEmpty) None else
        Some(properties.toList.map(kv => s"${kv._1}: ${kv._2}").mkString("{", ", ", "}"))
      val labelString = if (labels.isEmpty) None else Some(labels.mkString(":", ":", ""))

      (name ++ labelString ++ propString).mkString("(", " ", ")")
    }
  }

  case class RelPattern(
                         name:Option[String],
                         relType:Set[String],
                         properties:Map[String, Any],
                         length:LengthPattern ) {

    override def toString: String = {
      val propString = if (properties.isEmpty) None else
        Some(properties.toList.map(kv => s"${kv._1}: ${kv._2}").mkString("{", ", ", "}"))
      val relTypeString = if (relType.isEmpty) None else Some(relType.mkString(":", "|", ""))

      (name ++ relTypeString ++ propString).mkString("-[", " ", "]->")
    }
  }

  trait LengthPattern
  case object DefaultLength extends LengthPattern
  case class MinLength(value:Int) extends LengthPattern
  case class MaxLength(value:Int) extends LengthPattern
  case class MinMaxLength(min:Int, max:Int) extends LengthPattern

  def patterns:Gen[Pattern] = Gen.choose(1, NUM_LAYERS)
                                  .flatMap(depth => patternGen(depth, Gen.const(Set("L"+depth))))

  def patternGen(d:Int, labelGen:Gen[Set[String]]):Gen[Pattern] =
    for {
      nodeName <- Gen.option(Gen.identifier.map("n"+_))
      labels <- labelGen
      props <- Gen.mapOf(Gen.zip(Gen.const("p" + d), Gen.choose(1, NODES_PER_LAYER)))
      tail <- tailPatternGen(d+1)
    } yield Pattern(NodePattern(nodeName, labels, props), tail)

  def tailPatternGen(d:Int):Gen[Option[(RelPattern, Pattern)]] =
    if (d < NUM_LAYERS)
      Gen.zip(relPatternGen(d-1), patternGen(d, maybeWrongLabelGen(d))).map(Some(_))
    else
      Gen.const(None)

  def relPatternGen(d:Int):Gen[RelPattern] =
    for {
      relName <- Gen.option(Gen.identifier.map("r"+_))
      relType <- Gen.listOf(Gen.frequency(100 -> Gen.const("T" + d), 1 -> Gen.const("Y" + d))).map(_.toSet)
      props <- Gen.mapOf(Gen.zip(Gen.const("p" + d), Gen.frequency(100 -> Gen.const(d), 1 -> Gen.const("'x'"))))
    } yield RelPattern(relName, relType, props, DefaultLength)

  private def maybeWrongLabelGen(d:Int) =
    Gen.listOf(
      Gen.frequency(
        100 -> Gen.const("L"+d),
        1 -> Gen.const("X"+d)
      )).map(_.toSet)

  override protected def initTest(): Unit = {
    super.initTest()

    graph.inTx {
      val nodes: Seq[IndexedSeq[Node]] =
        for (i <- 1 to NUM_LAYERS) yield {
          for (j <- 1 to NODES_PER_LAYER) yield {
            val node = createLabeledNode(Map("p" + i -> j), "L" + i)
            node
          }
        }
      for (i <- 1 until NUM_LAYERS) {
        val thisLayer = nodes(i-1)
        val nextLayer = nodes(i)
        for {
          n1 <- thisLayer
          n2 <- nextLayer
        } {
          relate(n1, n2, s"T$i", Map("p"+i -> i))
        }
      }
    }
  }
  //(:L1)-[:T1]->(:L2)-....

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

  test("match pattern") {
    forAll(patterns) { pattern =>
      assertQuery(s"MATCH $pattern RETURN 42")
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
    resultCompiled
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

}
