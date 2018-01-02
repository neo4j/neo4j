/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.cypher.internal.compiler.v2_3.helpers

trait TypeSafeMathSupport {
  def plus(left: Any, right: Any): Any = {
    (left, right) match {
      case (null, _) => null
      case (_, null) => null

      case (l: Byte, r: Byte)   => l + r
      case (l: Byte, r: Double) => l + r
      case (l: Byte, r: Float)  => l + r
      case (l: Byte, r: Int)    => l + r.toLong
      case (l: Byte, r: Long)   => l + r
      case (l: Byte, r: Short)  => l + r

      case (l: Double, r: Byte)   => l + r
      case (l: Double, r: Double) => l + r
      case (l: Double, r: Float)  => l + r
      case (l: Double, r: Int)    => l + r
      case (l: Double, r: Long)   => l + r
      case (l: Double, r: Short)  => l + r

      case (l: Float, r: Byte)   => l + r
      case (l: Float, r: Double) => l + r
      case (l: Float, r: Float)  => l + r
      case (l: Float, r: Int)    => l + r
      case (l: Float, r: Long)   => l + r
      case (l: Float, r: Short)  => l + r

      case (l: Int, r: Byte)   => l.toLong + r
      case (l: Int, r: Double) => l + r
      case (l: Int, r: Float)  => l + r
      case (l: Int, r: Int)    => l.toLong + r.toLong
      case (l: Int, r: Long)   => l + r
      case (l: Int, r: Short)  => l + r

      case (l: Long, r: Byte)   => l + r
      case (l: Long, r: Double) => l + r
      case (l: Long, r: Float)  => l + r
      case (l: Long, r: Int)    => l + r
      case (l: Long, r: Long)   => l + r
      case (l: Long, r: Short)  => l + r

      case (l: Short, r: Byte)   => l + r
      case (l: Short, r: Double) => l + r
      case (l: Short, r: Float)  => l + r
      case (l: Short, r: Int)    => l + r
      case (l: Short, r: Long)   => l + r
      case (l: Short, r: Short)  => l + r

    }
  }

  def divide(left: Any, right: Any): Any = {
    (left, right) match {
      case (null, _) => null
      case (_, null) => null

      case (l: Byte, r: Byte)   => l / r
      case (l: Byte, r: Double) => l / r
      case (l: Byte, r: Float)  => l / r
      case (l: Byte, r: Int)    => l / r
      case (l: Byte, r: Long)   => l / r
      case (l: Byte, r: Short)  => l / r

      case (l: Double, r: Byte)   => l / r
      case (l: Double, r: Double) => l / r
      case (l: Double, r: Float)  => l / r
      case (l: Double, r: Int)    => l / r
      case (l: Double, r: Long)   => l / r
      case (l: Double, r: Short)  => l / r

      case (l: Float, r: Byte)   => l / r
      case (l: Float, r: Double) => l / r
      case (l: Float, r: Float)  => l / r
      case (l: Float, r: Int)    => l / r
      case (l: Float, r: Long)   => l / r
      case (l: Float, r: Short)  => l / r

      case (l: Int, r: Byte)   => l / r
      case (l: Int, r: Double) => l / r
      case (l: Int, r: Float)  => l / r
      case (l: Int, r: Int)    => l / r
      case (l: Int, r: Long)   => l / r
      case (l: Int, r: Short)  => l / r

      case (l: Long, r: Byte)   => l / r
      case (l: Long, r: Double) => l / r
      case (l: Long, r: Float)  => l / r
      case (l: Long, r: Int)    => l / r
      case (l: Long, r: Long)   => l / r
      case (l: Long, r: Short)  => l / r

      case (l: Short, r: Byte)   => l / r
      case (l: Short, r: Double) => l / r
      case (l: Short, r: Float)  => l / r
      case (l: Short, r: Int)    => l / r
      case (l: Short, r: Long)   => l / r
      case (l: Short, r: Short)  => l / r

    }
  }

  def minus(left: Any, right: Any): Any = {
    (left, right) match {
      case (null, _) => null
      case (_, null) => null

      case (l: Byte, r: Byte)   => l - r
      case (l: Byte, r: Double) => l - r
      case (l: Byte, r: Float)  => l - r
      case (l: Byte, r: Int)    => l - r.toLong
      case (l: Byte, r: Long)   => l - r
      case (l: Byte, r: Short)  => l - r

      case (l: Double, r: Byte)   => l - r
      case (l: Double, r: Double) => l - r
      case (l: Double, r: Float)  => l - r
      case (l: Double, r: Int)    => l - r
      case (l: Double, r: Long)   => l - r
      case (l: Double, r: Short)  => l - r

      case (l: Float, r: Byte)   => l - r
      case (l: Float, r: Double) => l - r
      case (l: Float, r: Float)  => l - r
      case (l: Float, r: Int)    => l - r
      case (l: Float, r: Long)   => l - r
      case (l: Float, r: Short)  => l - r

      case (l: Int, r: Byte)   => l - r
      case (l: Int, r: Double) => l - r
      case (l: Int, r: Float)  => l - r
      case (l: Int, r: Int)    => l.toLong - r.toLong
      case (l: Int, r: Long)   => l - r
      case (l: Int, r: Short)  => l - r

      case (l: Long, r: Byte)   => l - r
      case (l: Long, r: Double) => l - r
      case (l: Long, r: Float)  => l - r
      case (l: Long, r: Int)    => l - r
      case (l: Long, r: Long)   => l - r
      case (l: Long, r: Short)  => l - r

      case (l: Short, r: Byte)   => l - r
      case (l: Short, r: Double) => l - r
      case (l: Short, r: Float)  => l - r
      case (l: Short, r: Int)    => l - r.toLong
      case (l: Short, r: Long)   => l - r
      case (l: Short, r: Short)  => l - r

    }
  }

  def multiply(left: Any, right: Any): Any = {
    (left, right) match {
      case (null, _) => null
      case (_, null) => null

      case (l: Byte, r: Byte)   => l * r
      case (l: Byte, r: Double) => l * r
      case (l: Byte, r: Float)  => l * r
      case (l: Byte, r: Int)    => l * r.toLong
      case (l: Byte, r: Long)   => l * r
      case (l: Byte, r: Short)  => l * r

      case (l: Double, r: Byte)   => l * r
      case (l: Double, r: Double) => l * r
      case (l: Double, r: Float)  => l * r
      case (l: Double, r: Int)    => l * r
      case (l: Double, r: Long)   => l * r
      case (l: Double, r: Short)  => l * r

      case (l: Float, r: Byte)   => l * r
      case (l: Float, r: Double) => l * r
      case (l: Float, r: Float)  => l * r
      case (l: Float, r: Int)    => l * r
      case (l: Float, r: Long)   => l * r
      case (l: Float, r: Short)  => l * r

      case (l: Int, r: Byte)   => l * r
      case (l: Int, r: Double) => l * r
      case (l: Int, r: Float)  => l * r
      case (l: Int, r: Int)    => l.toLong * r.toLong
      case (l: Int, r: Long)   => l * r
      case (l: Int, r: Short)  => l.toLong * r.toLong

      case (l: Long, r: Byte)   => l * r
      case (l: Long, r: Double) => l * r
      case (l: Long, r: Float)  => l * r
      case (l: Long, r: Int)    => l * r
      case (l: Long, r: Long)   => l * r
      case (l: Long, r: Short)  => l * r

      case (l: Short, r: Byte)   => l.toLong * r
      case (l: Short, r: Double) => l * r
      case (l: Short, r: Float)  => l * r
      case (l: Short, r: Int)    => l * r.toLong
      case (l: Short, r: Long)   => l * r
      case (l: Short, r: Short)  => l * r

    }
  }

  /**
   * Serves for a type-safe, overflow-aware summation.
   * That is, tries to keep the sum type as narrow as possible, widening its value if necessary.
   * Initial result type is Int. If a Double is added, the result becomes Double.
   * If Long is added, the result becomes Long with the possibility to widen to Double later.
   *
   * @note  For the summation of floating-point numbers this class encapsulates Kahan's algorithm
   *        for error minimization (https://en.wikipedia.org/wiki/Kahan_summation_algorithm)
   */
  sealed trait OverflowAwareSum[T] {
    protected var sum: T = _
    def value = sum

    /**
     * @param next  next number to add to the total sum
     * @return      current value of the sum variable
     * @note        sticks to integral type for as long as possible
     */
    def add(next: Any): OverflowAwareSum[_]
  }

  case class DoubleSum(private val doubleValue: Double) extends OverflowAwareSum[Double] {
    sum = doubleValue
    def add(next: Any) = {
      next match {
        case (x: Byte)    => sum += x
        case (x: Short)   => sum += x
        case (x: Char)    => sum += x
        case (x: Int)     => sum += x
        case (x: Long)    => sum += x
        case (x: Float)   => sum += x
        case (x: Double)  => sum += x
        case _ =>
      }
      this
    }
  }

  case class LongSum(private val longValue: Long) extends OverflowAwareSum[Long] {
    sum = longValue
    import OverflowAwareSum._

    private def addLong(next: Long): OverflowAwareSum[_] = addExact(sum, next) match {
      case doubleValue: Double => DoubleSum(doubleValue)
      case other => sum =
        other.asInstanceOf[Long]
        this
    }

    override def add(next: Any): OverflowAwareSum[_] = next match {
      case (x: Byte)    => addLong(x)
      case (x: Short)   => addLong(x)
      case (x: Char)    => addLong(x)
      case (x: Int)     => addLong(x)
      case (x: Long)    => addLong(x)
      case (x: Float)   => DoubleSum(sum).add(next)
      case (x: Double)  => DoubleSum(sum).add(next)
      case _ => this
    }
  }

  case class IntSum(private val intValue: Int) extends OverflowAwareSum[Int] {
    sum = intValue
    import OverflowAwareSum._

    private def addInt(next: Int): OverflowAwareSum[_] = addExact(sum, next) match {
      case doubleValue: Double => DoubleSum(doubleValue)
      case longValue: Long => LongSum(longValue)
      case other =>
        sum = other.asInstanceOf[Int]
        this
    }

    override def add(next: Any): OverflowAwareSum[_] = next match {
      case (x: Byte)    => addInt(x)
      case (x: Short)   => addInt(x)
      case (x: Char)    => addInt(x)
      case (x: Int)     => addInt(x)
      case (x: Long)    => LongSum(sum).add(x)
      case (x: Float)   => DoubleSum(sum).add(x)
      case (x: Double)  => DoubleSum(sum).add(x)
      case _ => this
    }
  }

  /**
   * Contains overloaded constructors for all available OverflowAwareSum subclasses.
   * Also provides own analogs of JDK8's Math#addExact() methods that differ in that
   * they don't throw exceptions on overflow, but rather widen the type
   * of the value returned (int -> long -> double).
   */
  object OverflowAwareSum {

    def apply(x: Int)     = IntSum(x)
    def apply(x: Long)    = LongSum(x)
    def apply(x: Double)  = DoubleSum(x)

    def addExact(x: Int, y: Int): Any = {
      val r: Int = x + y
      if (((x ^ r) & (y ^ r)) >= 0) {
        r
      } else {    // integer overflow
        addExact(x.toLong, y.toLong)
      }
    }

    def addExact(x: Long, y: Long): Any= {
      val r: Long = x + y
      if (((x ^ r) & (y ^ r)) >= 0) {
        r
      } else {    // long overflow
        x.toDouble + y.toDouble
      }
    }
  }
}
