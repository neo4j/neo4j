/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
      case (l: Byte, r: Int)    => l + r
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

      case (l: Int, r: Byte)   => l + r
      case (l: Int, r: Double) => l + r
      case (l: Int, r: Float)  => l + r
      case (l: Int, r: Int)    => l + r
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
      case (l: Byte, r: Int)    => l - r
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
      case (l: Int, r: Int)    => l - r
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
      case (l: Short, r: Int)    => l - r
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
      case (l: Byte, r: Int)    => l * r
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
      case (l: Int, r: Int)    => l * r
      case (l: Int, r: Long)   => l * r
      case (l: Int, r: Short)  => l * r

      case (l: Long, r: Byte)   => l * r
      case (l: Long, r: Double) => l * r
      case (l: Long, r: Float)  => l * r
      case (l: Long, r: Int)    => l * r
      case (l: Long, r: Long)   => l * r
      case (l: Long, r: Short)  => l * r

      case (l: Short, r: Byte)   => l * r
      case (l: Short, r: Double) => l * r
      case (l: Short, r: Float)  => l * r
      case (l: Short, r: Int)    => l * r
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
  final class OverflowAwareSum {
    import OverflowAwareSum._

    private var adder: Adder[_] = IntAdder(0)
    def value = adder.value

    /**
     * @param next  next number to add to the total sum
     * @return      current value of the sum variable
     * @note        sticks to integral type for as long as possible
     */
    def add(next: Any) = {
      adder = adder.add(next)
    }
  }

  object OverflowAwareSum {
    abstract sealed class Adder[T](var value: T) {
      def add(x: Any): Adder[_]
    }

    abstract sealed class IntegralAdder[T, N](_value: T) extends Adder[T](_value) {
      protected val asDoubleAdder: T => DoubleAdder
      protected val widerAdder: T => Adder[N]
      protected val adderFunction: Any => T

      override def add(x: Any): Adder[_] = {
        if (x.isInstanceOf[Double]) {
          asDoubleAdder(value).add(x)
        } else {
          try {
            value = adderFunction(x)
            this
          } catch {
            case e: ArithmeticException => widerAdder(value).add(x)
            case anyOther: Exception => throw anyOther
          }
        }
      }
    }

    final case class DoubleAdder(v: Double) extends Adder(v) {
      private var c: Double = 0d
      /**
       * Performs a single Kahan's algorithm step
       * @param x value to add to the current sum
       * @return  value of the sum after the addition
       * @note    modifies [[c]] as a side-effect
       */
      override def add(x: Any): Adder[_] = {
        val y = minus(x, c).asInstanceOf[Double]
        val t = plus(value, y).asInstanceOf[Double]
        c = minus(minus(t, value), y).asInstanceOf[Double]
        value = t
        this
      }
    }

    final case class LongAdder(v: Long) extends IntegralAdder[Long, Double](v) {
      override val asDoubleAdder = (x: Long) => DoubleAdder(x)
      override val widerAdder = (x: Long) => DoubleAdder(x)
      override val adderFunction = (x: Any) => Math.addExact(value, x.asInstanceOf[Number].longValue())
    }

    final case class IntAdder(v: Int) extends IntegralAdder[Int, Long](v) {
      override val asDoubleAdder = (x: Int) => DoubleAdder(x)
      override val widerAdder = (x: Int) => LongAdder(x)
      override val adderFunction = (x: Any) => Math.addExact(value, x.asInstanceOf[Number].intValue())
    }
  }
}
