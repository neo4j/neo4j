/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.symbols

import java.lang.String

object AnyType {

  def fromJava(obj:Any):AnyType = {
    if(obj.isInstanceOf[String] || obj.isInstanceOf[Char])
      return StringType()

    if(obj.isInstanceOf[Number])
      return NumberType()
    
    if(obj.isInstanceOf[Boolean])
      return BooleanType()
    
    if(obj.isInstanceOf[Seq[_]] || obj.isInstanceOf[Array[_]])
      return AnyIterableType()
    
    ScalarType()
  }

  val instance = new AnyType()

  def apply() = instance
}

class AnyType {
  override def equals(other: Any) = if (other == null)
    false
  else
    other match {
      case x: AnyRef => x.getClass == this.getClass
      case _ => false
    }

  def isAssignableFrom(other: AnyType): Boolean = this.getClass.isAssignableFrom(other.getClass)

  override def toString: String = this.getClass.getSimpleName
}






















