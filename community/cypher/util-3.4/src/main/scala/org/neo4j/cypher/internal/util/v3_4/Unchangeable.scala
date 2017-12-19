/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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