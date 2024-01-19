/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.cypher.internal.options

import magnolia1.CaseClass
import magnolia1.Magnolia

import scala.language.experimental.macros

/**
 * Creates default values for options
 */
trait OptionDefault[T] {
  def default: T
}

object OptionDefault {

  def create[T](value: T): OptionDefault[T] = new OptionDefault[T] {
    override def default: T = value
  }

  // Magnolia generic derivation
  // Check out the tutorial at https://propensive.com/opensource/magnolia/tutorial

  type Typeclass[T] = OptionDefault[T]

  /**
   * Generic OptionDefault for any case class (given that there are OptionDefault:s for all its parameter types)
   * that gives each parameter its default value
   */
  def join[T](caseClass: CaseClass[OptionDefault, T]): OptionDefault[T] = {
    val value = caseClass.construct(_.typeclass.default)
    OptionDefault.create(value)
  }

  def derive[T]: OptionDefault[T] = macro Magnolia.gen[T]
}
