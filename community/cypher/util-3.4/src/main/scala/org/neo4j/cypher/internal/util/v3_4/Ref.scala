/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.util.v3_4

object Ref {
  def apply[T <: AnyRef](v: T) = new Ref[T](v)
}

final class Ref[+T <: AnyRef](val value: T) {
  if (value == null)
    throw new InternalException("Attempt to instantiate Ref(null)")

  def toIdString = Integer.toHexString(java.lang.System.identityHashCode(value))

  override def toString = s"Ref@$toIdString($value)"

  override def hashCode = java.lang.System.identityHashCode(value)

  override def equals(that: Any) = that match {
    case other: Ref[_] => value eq other.value
    case _ => false
  }
}
