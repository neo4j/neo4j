/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.internal.cypher.acceptance

import org.neo4j.cypher.internal.util.v3_4.test_helpers.CypherFunSuite
import org.scalacheck._

class Neo4jValueComparisonTest extends CypherFunSuite {

  trait TestDifficulty extends ((Any, Any) => (Any, Any)) {
    def applyChallenge(x: Any, y: Any): Option[(Any, Any)]

    override def apply(v1: Any, v2: Any): (Any, Any) =
    // Try (L,R), then (R,L) and lastly fall back to identity
      (applyChallenge(v1, v2) match {
        case None => applyChallenge(v2, v1)
        case y => y
      }).getOrElse((v1, v2))
  }

  object Identity extends TestDifficulty {
    override def applyChallenge(v1: Any, v2: Any): Option[(Any, Any)] = Some((v1, v2))
  }

  object ChangePrecision extends TestDifficulty {
    def applyChallenge(x: Any, y: Any): Option[(Any, Any)] = (x, y) match {
      case (a: Double, _) => Some((a, a.toFloat))
      case (a: Float, _) => Some((a, a.toDouble))
      case (a: Int, _) => Some((a, a.toLong))
      case (a: Long, _) => Some((a, a.toInt))
      case _ => None
    }
  }

  object SlightlyMore extends TestDifficulty {
    def applyChallenge(x: Any, y: Any): Option[(Any, Any)] = (x, y) match {
      case (a: Double, _) => Some((a, Math.nextUp(a)))
      case (a: Float, _) => Some((a, Math.nextUp(a)))
      case (a: Int, _) => Some((a, a + 1))
      case (a: Long, _) => Some((a, a + 1))
      case _ => None
    }
  }

  object IntegerFloatMix extends TestDifficulty {

    val LARGEST_EXACT_LONG_IN_DOUBLE = 1L << 53
    val LARGEST_EXACT_INT_IN_FLOAT = 1L << 24

    def canFitInInt(a: Double) = -Int.MinValue < a && a < Int.MaxValue
    def canFitInLong(a: Double) = -Long.MinValue < a && a < Long.MaxValue
    def canBeExactlyAnIntegerD(a: Double) = -LARGEST_EXACT_LONG_IN_DOUBLE < a && a < LARGEST_EXACT_LONG_IN_DOUBLE
    def canBeExactlyAnIntegerF(a: Float) = -LARGEST_EXACT_INT_IN_FLOAT < a && a < LARGEST_EXACT_INT_IN_FLOAT

    def applyChallenge(x: Any, y: Any): Option[(Any, Any)] = (x, y) match {
      case (a: Double, _) if canFitInInt(a) => Some((Math.rint(a), a.toInt))
      case (a: Double, _) if canBeExactlyAnIntegerD(a) => Some((Math.floor(a), a.toLong))
      case (a: Float, _) if canBeExactlyAnIntegerF(a) => Some((Math.floor(a), a.toInt))
      case (a: Int, _) if canBeExactlyAnIntegerF(a) => Some((a, a.toFloat))
      case (a: Long, _) if canBeExactlyAnIntegerF(a) => Some((a, a.toFloat))
      case (a: Long, _) if canBeExactlyAnIntegerD(a) => Some((a, a.toDouble))
      case _ => None
    }
  }

  val testDifficulties = Gen.oneOf(
    Identity,
    ChangePrecision,
    SlightlyMore,
    IntegerFloatMix
  )

  val arbAnyVal: Gen[Any] = Gen.oneOf(
    Gen.const(null), Arbitrary.arbitrary[String],
    Arbitrary.arbitrary[Boolean], Arbitrary.arbitrary[Char], Arbitrary.arbitrary[Byte],
    Arbitrary.arbitrary[Short], Arbitrary.arbitrary[Int], Arbitrary.arbitrary[Long], Arbitrary.arbitrary[Float],
    Arbitrary.arbitrary[Double]
  )

  val testCase: Gen[ValuePair] = for {
    a <- arbAnyVal
    b <- arbAnyVal
    t <- testDifficulties
  } yield {
    val (x, y) = t(a, b)
    ValuePair(x, y)
  }

  case class ValuePair(a: Any, b: Any) {
    override def toString: String = s"(a: ${str(a)}, b: ${str(b)})"

    private def str(x: Any) = if (x == null) "NULL" else s"${x.getClass.getSimpleName} $x"
  }

  def notKnownLuceneBug(a: Any, b: Any): Boolean = a != -0.0 && b != -0.0
}
