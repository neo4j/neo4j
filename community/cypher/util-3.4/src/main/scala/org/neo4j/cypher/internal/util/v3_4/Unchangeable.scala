/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

// Freely after the ATOM idea presented by Daniel Spiewak (@djspiewak) in
// the video "Functional Compilers: From CFG to EXE"
// This is a wrapper that allows values to be set multiple times, but can be trusted to never change once seen.
class Unchangeable[A]() {
  private var _seen = false
  private var _value: Option[A] = None

  def hasValue: Boolean = _value.isDefined

  // Getter
  def value: A = {
    val result = _value.getOrElse(throw new InternalException("Value still not set"))
    _seen = true
    result
  }

  // Setter
  def value_=(newValue: A): Unit = {
    if (_seen) throw new InternalException("Can't change a seen value")
    _value = Some(newValue)
  }

  // Copy from another Unchangeable[A] iff set
  def copyFrom(other: Unchangeable[A]) = if(other.hasValue)
    value_=(other.value)

  override def toString: String = s"Unchangeable(${_value.getOrElse("NOT SET")})"
}