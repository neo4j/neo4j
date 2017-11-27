/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.v3_4.expressions

import org.neo4j.cypher.internal.util.v3_4.InputPosition

class Property(val map: Expression, val propertyKey: PropertyKeyName)(val position: InputPosition) extends Expression {
  override def asCanonicalStringVal = s"${map.asCanonicalStringVal}.${propertyKey.asCanonicalStringVal}"

  //--------------------------------------------------------------------------------------------------
  // The methods below are what we would get automatically if this was a case class
  //--------------------------------------------------------------------------------------------------
  def copy(map: Expression = this.map, propertyKey: PropertyKeyName = this.propertyKey)(position: InputPosition): Property =
    new Property(map, propertyKey)(position)

  override def productElement(n: Int) =
    n match {
      case 0 => map
      case 1 => propertyKey
      case _ => throw new java.lang.IndexOutOfBoundsException(n.toString)
    }

  override def productArity = 2

  override def canEqual(that: Any) = that.isInstanceOf[Property]

  override def toString: String = s"Property($map, $propertyKey)"

  override def hashCode(): Int = runtime.ScalaRunTime._hashCode(Property.this)

  override def equals(obj: scala.Any): Boolean = runtime.ScalaRunTime._equals(Property.this, obj)
}

object Property {
  def apply(map: Expression, propertyKey: PropertyKeyName)(position: InputPosition) = new Property(map, propertyKey)(position)

  def unapply(p: Property): Option[(Expression, PropertyKeyName)] = Some((p.map, p.propertyKey))
}
