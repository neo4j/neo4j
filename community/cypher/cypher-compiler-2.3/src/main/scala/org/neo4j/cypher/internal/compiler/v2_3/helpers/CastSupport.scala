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

import org.neo4j.cypher.internal.frontend.v2_3.CypherTypeException

object CastSupport {

  // TODO Switch to using ClassTag once we decide to depend on the reflection api

  /**
   * Filter sequence by type
   */
  def sift[A : Manifest](seq: Seq[Any]): Seq[A] = seq.collect(erasureCast)

  /**
   * Casts input to A if possible according to type erasure, discards input otherwise
   */
  def erasureCast[A : Manifest]: PartialFunction[Any, A] = { case value: A => value }

  def castOrFail[A](value: Any)(implicit ev: Manifest[A]): A = value match {
    case v: A => v
    case _    => throw new CypherTypeException(
      s"Expected $value to be a ${ev.runtimeClass.getName}, but it was a ${value.getClass.getName}")
  }

  /*
  This method takes two values and finds the type both values could be represented in.

  The produced value is strictly meant to be used as a type carrier
   */
  def merge(a: Any, b: Any): Any = {
    (a, b) match {
      case (_: String, _: String)   => a
      case (_: Char, _: Char)       => a
      case (_: Boolean, _: Boolean) => a

      case (_: Byte, _: Byte)   => a
      case (_: Byte, _: Short)  => b
      case (_: Byte, _: Int)    => b
      case (_: Byte, _: Long)   => b
      case (_: Byte, _: Float)  => b
      case (_: Byte, _: Double) => b

      case (_: Short, _: Int)    => b
      case (_: Short, _: Long)   => b
      case (_: Short, _: Float)  => b
      case (_: Short, _: Double) => b
      case (_: Short, _: Number) => a

      case (_: Int, _: Long)   => b
      case (_: Int, _: Float)  => b
      case (_: Int, _: Double) => b
      case (_: Int, _: Number) => a

      case (_: Long, _: Float)  => b
      case (_: Long, _: Double) => b
      case (_: Long, _: Number) => a

      case (_: Float, _: Double) => b
      case (_: Float, _: Number) => a

      case (_: Double, _: Number) => a

      case (a, b) if a == null || b == null => throw new CypherTypeException("Collections containing null values can not be stored in properties.")

      case (a, b) if a.isInstanceOf[Seq[_]] || b.isInstanceOf[Seq[_]] => throw new CypherTypeException("Collections containing collections can not be stored in properties.")

      case _ => throw new CypherTypeException("Collections containing mixed types can not be stored in properties.")
    }
  }

  /*Instances of this class can be gotten from @CastSupport.getConverter*/
  sealed case class Converter(valueConverter: Any => Any, arrayConverter: Seq[Any] => Array[_])

  /*Returns a converter given a type value*/
  def getConverter(x: Any): Converter = x match {
    case _: String  => Converter(x => x.asInstanceOf[String], x => x.asInstanceOf[Seq[String]].toArray[String])
    case _: Char    => Converter(x => x.asInstanceOf[Char], x => x.asInstanceOf[Seq[Char]].toArray[Char])
    case _: Boolean => Converter(x => x.asInstanceOf[Boolean], x => x.asInstanceOf[Seq[Boolean]].toArray[Boolean])
    case _: Byte    => Converter(x => x.asInstanceOf[Number].byteValue(), x => x.asInstanceOf[Seq[Byte]].toArray[Byte])
    case _: Short   => Converter(x => x.asInstanceOf[Number].shortValue(), x => x.asInstanceOf[Seq[Short]].toArray[Short])
    case _: Int     => Converter(x => x.asInstanceOf[Number].intValue(), x => x.asInstanceOf[Seq[Int]].toArray[Int])
    case _: Long    => Converter(x => x.asInstanceOf[Number].longValue(), x => x.asInstanceOf[Seq[Long]].toArray[Long])
    case _: Float   => Converter(x => x.asInstanceOf[Number].floatValue(), x => x.asInstanceOf[Seq[Float]].toArray[Float])
    case _: Double  => Converter(x => x.asInstanceOf[Number].doubleValue(), x => x.asInstanceOf[Seq[Double]].toArray[Double])
    case _          => throw new CypherTypeException("Property values can only be of primitive types or arrays thereof")
  }
}
