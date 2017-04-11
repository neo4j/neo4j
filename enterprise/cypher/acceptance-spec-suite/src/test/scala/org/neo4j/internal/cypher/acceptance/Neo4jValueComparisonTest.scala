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

import org.neo4j.cypher.internal.codegen.CompiledEquivalenceUtils
import org.neo4j.cypher.internal.compiler.v3_2.commands.predicates.Equivalent
import org.neo4j.cypher.internal.frontend.v3_2.test_helpers.CypherFunSuite
import org.neo4j.kernel.api.impl.schema.LuceneDocumentStructure
import org.neo4j.kernel.api.properties.Property
import org.scalacheck._
import org.scalatest.prop.GeneratorDrivenPropertyChecks

class Neo4jValueComparisonTest extends CypherFunSuite {

  trait TestTricks extends ((Any, Any) => (Any, Any))

  object TwoIndependentValues extends TestTricks {
    override def apply(v1: Any, v2: Any): (Any, Any) = (v1, v2)
  }

  object ChangePrecision extends TestTricks {
    override def apply(v1: Any, v2: Any): (Any, Any) = {

      def fix(x: Any, y: Any): Option[(Any, Any)] = (x, y) match {
        case (a: Double, _) => Some((a, a.toFloat))
        case (a: Float, _) => Some((a, a.toDouble))
        case (a: Int, _) => Some((a, a.toLong))
        case (a: Long, _) => Some((a, a.toInt))
        case _ => None
      }

      // Try (L,R), then (R,L) and lastly fall back to identity
      (fix(v1, v2) match {
        case None => fix(v2, v1)
        case y => y
      }).getOrElse((v1, v2))
    }
  }

  object SlightlyMore extends TestTricks {
    override def apply(v1: Any, v2: Any): (Any, Any) = {

      def fix(x: Any, y: Any): Option[(Any, Any)] = (x, y) match {
        case (a: Double, _) => Some((a, Math.nextUp(a)))
        case (a: Float, _) => Some((a, Math.nextUp(a)))
        case (a: Int, _) => Some((a, a + 1))
        case (a: Long, _) => Some((a, a + 1))
        case _ => None
      }

      // Try (L,R), then (R,L) and lastly fall back to identity
      (fix(v1, v2) match {
        case None => fix(v2, v1)
        case y => y
      }).getOrElse((v1, v2))
    }
  }

  object IntegerFloatMix extends TestTricks {
    override def apply(v1: Any, v2: Any): (Any, Any) = {

      def fix(x: Any, y: Any): Option[(Any, Any)] = (x, y) match {
        case (a: Double, _) => Some((a, Math.nextUp(a)))
        case (a: Float, _) => Some((a, Math.nextUp(a)))
        case (a: Int, _) => Some((a, a + 1))
        case (a: Long, _) => Some((a, a + 1))
        case _ => None
      }

      // Try (L,R), then (R,L) and lastly fall back to identity
      (fix(v1, v2) match {
        case None => fix(v2, v1)
        case y => y
      }).getOrElse((v1, v2))
    }
  }

  val testTricks = Gen.oneOf(
    TwoIndependentValues,
    ChangePrecision,
    SlightlyMore
  )

  val arbAnyVal: Gen[Any] = Gen.oneOf(
    Gen.const(null), Arbitrary.arbitrary[String],
    Arbitrary.arbitrary[Boolean], Arbitrary.arbitrary[Char], Arbitrary.arbitrary[Byte],
    Arbitrary.arbitrary[Short], Arbitrary.arbitrary[Int], Arbitrary.arbitrary[Long], Arbitrary.arbitrary[Float],
    Arbitrary.arbitrary[Double]
  )

  val oftenEqual: Gen[Values] = for {
    a <- arbAnyVal
    b <- arbAnyVal
    t <- testTricks
  } yield {
    val (x, y) = t(a, b)
    Values(x, y)
  }

  case class Values(a: Any, b: Any) {
    override def toString: String = s"(a: ${str(a)}, b: ${str(b)})"

    private def str(x: Any) = if (x == null) "NULL" else s"${x.getClass.getSimpleName} $x"
  }

  test("compare equality between modules") {
    GeneratorDrivenPropertyChecks.forAll(oftenEqual) { (x: Values) =>
      val Values(a, b) = x
      val compiledA2B = CompiledEquivalenceUtils.equals(a, b)
      val compiledB2A = CompiledEquivalenceUtils.equals(b, a)
      val interpretedA2B = Equivalent(a).equals(b)
      val interpretedB2A = Equivalent(b).equals(a)

      if (compiledA2B != compiledB2A) fail(s"compiled (a = b)[$compiledA2B] = (b = a)[$compiledB2A]")
      if (interpretedA2B != interpretedB2A) fail(s"interpreted (a = b)[$interpretedA2B] = (b = a)[$interpretedB2A]")
      if (compiledA2B != interpretedA2B) fail(s"compiled[$compiledA2B] = interpreted[$interpretedA2B]")

      if (a != null && b != null) {
        val propertyA2B = Property.property(-1, a).valueEquals(b)
        val propertyB2A = Property.property(-1, b).valueEquals(a)
        if (propertyA2B != propertyB2A) fail(s"property (a = b)[$propertyA2B] = (b = a)[$propertyB2A]")
        if (propertyA2B != interpretedA2B) fail(s"property[$propertyA2B] = interpreted[$interpretedA2B]")

        if (propertyA2B) {
          val value1 = LuceneDocumentStructure.encodeValueField(a).stringValue()
          val value2 = LuceneDocumentStructure.encodeValueField(b).stringValue()
          val index = value1.equals(value2)

          if (!index)
            fail(s"if property comparison yields true ($propertyA2B), the index string comparison must also yield true ($index)")
        }
      }

    }
  }
}
