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
package org.neo4j.cypher.internal.helpers

// TODO: This is only here for accomodating cypher-compiler-1.9. Do not touch, do not import, purge it with fire post 2.3
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
}
