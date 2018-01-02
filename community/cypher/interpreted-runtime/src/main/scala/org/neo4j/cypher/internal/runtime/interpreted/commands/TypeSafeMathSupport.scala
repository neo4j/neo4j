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
package org.neo4j.cypher.internal.runtime.interpreted.commands

import org.neo4j.cypher.internal.util.v3_4.{ArithmeticException, InternalException}
import org.neo4j.values.AnyValue
import org.neo4j.values.storable._

trait TypeSafeMathSupport {

  def asNumberValue(a: Any): NumberValue = a match {
    case i: Int => Values.intValue(i)
    case l: Long => Values.longValue(l)
    case n: Number => Values.doubleValue(n.doubleValue())
    case _ => throw new InternalException(s"$a is not a number")
  }

  def plus(left: NumberValue, right: NumberValue): AnyValue = {

    try {
      (left, right) match {
        case (x, y) if x == Values.NO_VALUE || y == Values.NO_VALUE => Values.NO_VALUE

        case (l: ByteValue, r: ByteValue) => Values.intValue(l.value() + r.value())
        case (l: ByteValue, r: DoubleValue) => Values.doubleValue(l.value() + r.value())
        case (l: ByteValue, r: FloatValue) => Values.floatValue(l.value() + r.value())
        case (l: ByteValue, r: IntValue) => Values.longValue(l.value() + r.value().toLong)
        case (l: ByteValue, r: LongValue) => Values.longValue(Math.addExact(l.value(),r.value()))
        case (l: ByteValue, r: ShortValue) => Values.intValue(l.value() + r.value())

        case (l: DoubleValue, r: ByteValue) => Values.doubleValue(l.value() + r.value())
        case (l: DoubleValue, r: DoubleValue) => Values.doubleValue(l.value() + r.value())
        case (l: DoubleValue, r: FloatValue) => Values.doubleValue(l.value() + r.value())
        case (l: DoubleValue, r: IntValue) => Values.doubleValue(l.value() + r.value())
        case (l: DoubleValue, r: LongValue) => Values.doubleValue(l.value() + r.value())
        case (l: DoubleValue, r: ShortValue) => Values.doubleValue(l.value() + r.value())

        case (l: FloatValue, r: ByteValue) => Values.floatValue(l.value() + r.value())
        case (l: FloatValue, r: DoubleValue) => Values.doubleValue(l.value() + r.value())
        case (l: FloatValue, r: FloatValue) => Values.floatValue(l.value() + r.value())
        case (l: FloatValue, r: IntValue) => Values.floatValue(l.value() + r.value())
        case (l: FloatValue, r: LongValue) => Values.floatValue(l.value() + r.value())
        case (l: FloatValue, r: ShortValue) => Values.floatValue(l.value() + r.value())

        case (l: IntValue, r: ByteValue) => Values.intValue(l.value() + r.value())
        case (l: IntValue, r: DoubleValue) => Values.doubleValue(l.value() + r.value())
        case (l: IntValue, r: FloatValue) => Values.floatValue(l.value() + r.value())
        case (l: IntValue, r: IntValue) => Values.longValue(l.longValue() + r.longValue())
        case (l: IntValue, r: LongValue) => Values.longValue(Math.addExact(l.value(), r.value()))
        case (l: IntValue, r: ShortValue) => Values.intValue(l.value() + r.value())

        case (l: LongValue, r: ByteValue) => Values.longValue(Math.addExact(l.value(),r.value()))
        case (l: LongValue, r: DoubleValue) => Values.doubleValue(l.value() + r.value())
        case (l: LongValue, r: FloatValue) => Values.floatValue(l.value() + r.value())
        case (l: LongValue, r: IntValue) => Values.longValue(Math.addExact(l.value(),r.value()))
        case (l: LongValue, r: LongValue) => Values.longValue(Math.addExact(l.value(),r.value()))
        case (l: LongValue, r: ShortValue) => Values.longValue(Math.addExact(l.value(),r.value()))

        case (l: ShortValue, r: ByteValue) => Values.intValue(l.value() + r.value())
        case (l: ShortValue, r: DoubleValue) => Values.doubleValue(l.value() + r.value())
        case (l: ShortValue, r: FloatValue) => Values.floatValue(l.value() + r.value())
        case (l: ShortValue, r: IntValue) => Values.longValue(l.longValue() + r.longValue())
        case (l: ShortValue, r: LongValue) => Values.longValue(Math.addExact(l.value(),r.value()))
        case (l: ShortValue, r: ShortValue) => Values.intValue(l.value() + r.value())
      }
    } catch {
      case e: java.lang.ArithmeticException =>
        throw new ArithmeticException(s"result of $left + $right cannot be represented as an integer")
    }
  }

  def divide(left: NumberValue, right: NumberValue): AnyValue = {
    (left, right) match {
      case (x, y) if x == Values.NO_VALUE || y == Values.NO_VALUE => Values.NO_VALUE

      case (l: ByteValue, r: ByteValue) => Values.intValue(l.value() / r.value())
      case (l: ByteValue, r: DoubleValue) => Values.doubleValue(l.value() / r.value())
      case (l: ByteValue, r: FloatValue) => Values.floatValue(l.value() / r.value())
      case (l: ByteValue, r: IntValue) => Values.longValue(l.value() / r.value().toLong)
      case (l: ByteValue, r: LongValue) => Values.longValue(l.value() / r.value())
      case (l: ByteValue, r: ShortValue) => Values.intValue(l.value() / r.value())

      case (l: DoubleValue, r: ByteValue) => Values.doubleValue(l.value() / r.value())
      case (l: DoubleValue, r: DoubleValue) => Values.doubleValue(l.value() / r.value())
      case (l: DoubleValue, r: FloatValue) => Values.doubleValue(l.value() / r.value())
      case (l: DoubleValue, r: IntValue) => Values.doubleValue(l.value() / r.value())
      case (l: DoubleValue, r: LongValue) => Values.doubleValue(l.value() / r.value())
      case (l: DoubleValue, r: ShortValue) => Values.doubleValue(l.value() / r.value())

      case (l: FloatValue, r: ByteValue) => Values.floatValue(l.value() / r.value())
      case (l: FloatValue, r: DoubleValue) => Values.doubleValue(l.value() / r.value())
      case (l: FloatValue, r: FloatValue) => Values.floatValue(l.value() / r.value())
      case (l: FloatValue, r: IntValue) => Values.floatValue(l.value() / r.value())
      case (l: FloatValue, r: LongValue) => Values.floatValue(l.value() / r.value())
      case (l: FloatValue, r: ShortValue) => Values.floatValue(l.value() / r.value())

      case (l: IntValue, r: ByteValue) => Values.intValue(l.value() / r.value())
      case (l: IntValue, r: DoubleValue) => Values.doubleValue(l.value() / r.value())
      case (l: IntValue, r: FloatValue) => Values.floatValue(l.value() / r.value())
      case (l: IntValue, r: IntValue) => Values.intValue(l.value() / r.value())
      case (l: IntValue, r: LongValue) => Values.longValue(l.value() /  r.value())
      case (l: IntValue, r: ShortValue) => Values.intValue(l.value() / r.value())

      case (l: LongValue, r: ByteValue) => Values.longValue(l.value() / r.value())
      case (l: LongValue, r: DoubleValue) => Values.doubleValue(l.value() / r.value())
      case (l: LongValue, r: FloatValue) => Values.floatValue(l.value() / r.value())
      case (l: LongValue, r: IntValue) => Values.longValue(l.value() / r.value())
      case (l: LongValue, r: LongValue) => Values.longValue(l.value() / r.value())
      case (l: LongValue, r: ShortValue) => Values.longValue(l.value() / r.value())

      case (l: ShortValue, r: ByteValue) => Values.intValue(l.value() / r.value())
      case (l: ShortValue, r: DoubleValue) => Values.doubleValue(l.value() / r.value())
      case (l: ShortValue, r: FloatValue) => Values.floatValue(l.value() / r.value())
      case (l: ShortValue, r: IntValue) => Values.longValue(l.longValue() / r.longValue())
      case (l: ShortValue, r: LongValue) => Values.longValue(l.value() / r.value())
      case (l: ShortValue, r: ShortValue) => Values.intValue(l.value() / r.value())

    }
  }

  def minus(left: NumberValue, right: NumberValue): AnyValue = {
    try {
      (left, right) match {
        case (x, y) if x == Values.NO_VALUE || y == Values.NO_VALUE => Values.NO_VALUE
        case (l: ByteValue, r: ByteValue) => Values.intValue(l.value() - r.value())
        case (l: ByteValue, r: DoubleValue) => Values.doubleValue(l.value() - r.value())
        case (l: ByteValue, r: FloatValue) => Values.floatValue(l.value() - r.value())
        case (l: ByteValue, r: IntValue) => Values.longValue(l.value() - r.value().toLong)
        case (l: ByteValue, r: LongValue) => Values.longValue(Math.subtractExact(l.value(),r.value()))
        case (l: ByteValue, r: ShortValue) => Values.intValue(l.value() - r.value())

        case (l: DoubleValue, r: ByteValue) => Values.doubleValue(l.value() - r.value())
        case (l: DoubleValue, r: DoubleValue) => Values.doubleValue(l.value() - r.value())
        case (l: DoubleValue, r: FloatValue) => Values.doubleValue(l.value() - r.value())
        case (l: DoubleValue, r: IntValue) => Values.doubleValue(l.value() - r.value())
        case (l: DoubleValue, r: LongValue) => Values.doubleValue(l.value() - r.value())
        case (l: DoubleValue, r: ShortValue) => Values.doubleValue(l.value() - r.value())

        case (l: FloatValue, r: ByteValue) => Values.floatValue(l.value() - r.value())
        case (l: FloatValue, r: DoubleValue) => Values.doubleValue(l.value() - r.value())
        case (l: FloatValue, r: FloatValue) => Values.floatValue(l.value() - r.value())
        case (l: FloatValue, r: IntValue) => Values.floatValue(l.value() - r.value())
        case (l: FloatValue, r: LongValue) => Values.floatValue(l.value() - r.value())
        case (l: FloatValue, r: ShortValue) => Values.floatValue(l.value() - r.value())

        case (l: IntValue, r: ByteValue) => Values.intValue(l.value() - r.value())
        case (l: IntValue, r: DoubleValue) => Values.doubleValue(l.value() - r.value())
        case (l: IntValue, r: FloatValue) => Values.floatValue(l.value() - r.value())
        case (l: IntValue, r: IntValue) => Values.longValue(l.longValue() - r.longValue())
        case (l: IntValue, r: LongValue) => Values.longValue(Math.subtractExact(l.value(), r.value()))
        case (l: IntValue, r: ShortValue) => Values.intValue(l.value() - r.value())

        case (l: LongValue, r: ByteValue) => Values.longValue(Math.subtractExact(l.value(),r.value()))
        case (l: LongValue, r: DoubleValue) => Values.doubleValue(l.value() - r.value())
        case (l: LongValue, r: FloatValue) => Values.floatValue(l.value() - r.value())
        case (l: LongValue, r: IntValue) => Values.longValue(Math.subtractExact(l.value(),r.value()))
        case (l: LongValue, r: LongValue) => Values.longValue(Math.subtractExact(l.value(),r.value()))
        case (l: LongValue, r: ShortValue) => Values.longValue(Math.subtractExact(l.value(),r.value()))

        case (l: ShortValue, r: ByteValue) => Values.intValue(l.value() - r.value())
        case (l: ShortValue, r: DoubleValue) => Values.doubleValue(l.value() - r.value())
        case (l: ShortValue, r: FloatValue) => Values.floatValue(l.value() - r.value())
        case (l: ShortValue, r: IntValue) => Values.longValue(l.longValue() - r.longValue())
        case (l: ShortValue, r: LongValue) => Values.longValue(Math.subtractExact(l.value(),r.value()))
        case (l: ShortValue, r: ShortValue) => Values.intValue(l.value() - r.value())

      }
    } catch {
      case e: java.lang.ArithmeticException  =>
        throw new ArithmeticException(s"result of $left - $right cannot be represented as an integer")
    }
  }

  def multiply(left: NumberValue, right: NumberValue): AnyValue = {
    try {
      (left, right) match {
        case (x, y) if x == Values.NO_VALUE || y == Values.NO_VALUE => Values.NO_VALUE

        case (l: ByteValue, r: ByteValue) => Values.intValue(l.value() * r.value())
        case (l: ByteValue, r: DoubleValue) => Values.doubleValue(l.value() * r.value())
        case (l: ByteValue, r: FloatValue) => Values.floatValue(l.value() * r.value())
        case (l: ByteValue, r: IntValue) => Values.longValue(l.value() * r.value().toLong)
        case (l: ByteValue, r: LongValue) => Values.longValue(Math.multiplyExact(l.value(),r.value()))
        case (l: ByteValue, r: ShortValue) => Values.intValue(l.value() * r.value())

        case (l: DoubleValue, r: ByteValue) => Values.doubleValue(l.value() * r.value())
        case (l: DoubleValue, r: DoubleValue) => Values.doubleValue(l.value() * r.value())
        case (l: DoubleValue, r: FloatValue) => Values.doubleValue(l.value() * r.value())
        case (l: DoubleValue, r: IntValue) => Values.doubleValue(l.value() * r.value())
        case (l: DoubleValue, r: LongValue) => Values.doubleValue(l.value() * r.value())
        case (l: DoubleValue, r: ShortValue) => Values.doubleValue(l.value() * r.value())

        case (l: FloatValue, r: ByteValue) => Values.floatValue(l.value() * r.value())
        case (l: FloatValue, r: DoubleValue) => Values.doubleValue(l.value() * r.value())
        case (l: FloatValue, r: FloatValue) => Values.floatValue(l.value() * r.value())
        case (l: FloatValue, r: IntValue) => Values.floatValue(l.value() * r.value())
        case (l: FloatValue, r: LongValue) => Values.floatValue(l.value() * r.value())
        case (l: FloatValue, r: ShortValue) => Values.floatValue(l.value() * r.value())

        case (l: IntValue, r: ByteValue) => Values.intValue(l.value() * r.value())
        case (l: IntValue, r: DoubleValue) => Values.doubleValue(l.value() * r.value())
        case (l: IntValue, r: FloatValue) => Values.floatValue(l.value() * r.value())
        case (l: IntValue, r: IntValue) => Values.longValue(l.longValue() * r.longValue())
        case (l: IntValue, r: LongValue) => Values.longValue(Math.multiplyExact(l.value(), r.value()))
        case (l: IntValue, r: ShortValue) => Values.intValue(l.value() * r.value())

        case (l: LongValue, r: ByteValue) => Values.longValue(Math.multiplyExact(l.value(),r.value()))
        case (l: LongValue, r: DoubleValue) => Values.doubleValue(l.value() * r.value())
        case (l: LongValue, r: FloatValue) => Values.floatValue(l.value() * r.value())
        case (l: LongValue, r: IntValue) => Values.longValue(Math.multiplyExact(l.value(),r.value()))
        case (l: LongValue, r: LongValue) => Values.longValue(Math.multiplyExact(l.value(),r.value()))
        case (l: LongValue, r: ShortValue) => Values.longValue(Math.multiplyExact(l.value(),r.value()))

        case (l: ShortValue, r: ByteValue) => Values.intValue(l.value() * r.value())
        case (l: ShortValue, r: DoubleValue) => Values.doubleValue(l.value() * r.value())
        case (l: ShortValue, r: FloatValue) => Values.floatValue(l.value() * r.value())
        case (l: ShortValue, r: IntValue) => Values.longValue(l.longValue() * r.longValue())
        case (l: ShortValue, r: LongValue) => Values.longValue(Math.multiplyExact(l.value(),r.value()))
        case (l: ShortValue, r: ShortValue) => Values.intValue(l.value() * r.value())

      }
    } catch {
      case e: java.lang.ArithmeticException  =>
        throw new ArithmeticException(s"result of $left * $right cannot be represented as an integer")
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
    def add(next: AnyValue): OverflowAwareSum[_]
  }

  case class DoubleSum(private val doubleValue: Double) extends OverflowAwareSum[Double] {
    sum = doubleValue
    def add(next: AnyValue) = {
      next match {
        case (x: ByteValue)    => sum += x.value()
        case (x: ShortValue)   => sum += x.value()
        case (x: CharValue)    => sum += x.value()
        case (x: IntValue)     => sum += x.value()
        case (x: LongValue)    => sum += x.value()
        case (x: FloatValue)   => sum += x.value()
        case (x: DoubleValue)  => sum += x.value()
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

    override def add(next: AnyValue): OverflowAwareSum[_] = next match {
      case (x: ByteValue)    => addLong(x.longValue())
      case (x: ShortValue)   => addLong(x.longValue())
      case (x: CharValue)    => addLong(x.value())
      case (x: IntValue)     => addLong(x.longValue())
      case (x: LongValue)    => addLong(x.longValue())
      case (_: FloatValue)   => DoubleSum(sum).add(next)
      case (_: DoubleValue)  => DoubleSum(sum).add(next)
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

    override def add(next: AnyValue): OverflowAwareSum[_] = next match {
      case (x: ByteValue)    => addInt(x.value())
      case (x: ShortValue)   => addInt(x.value())
      case (x: CharValue)    => addInt(x.value())
      case (x: IntValue)     => addInt(x.value())
      case (x: LongValue)    => LongSum(sum).add(x)
      case (x: FloatValue)   => DoubleSum(sum).add(x)
      case (x: DoubleValue)  => DoubleSum(sum).add(x)
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
