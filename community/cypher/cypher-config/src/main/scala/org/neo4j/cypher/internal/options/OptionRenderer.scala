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
 * Renders options as string for inclusion in rendered queries
 */
trait OptionRenderer[T] {
  def render(value: T): String
}

object OptionRenderer {

  def create[T](func: T => String): OptionRenderer[T] =
    (value: T) => func(value)

  // Magnolia generic derivation
  // Check out the tutorial at https://propensive.com/opensource/magnolia/tutorial

  type Typeclass[T] = OptionRenderer[T]

  /**
   * Generic OptionRenderer for any case class (given that there are OptionRenderer:s for all its parameter types)
   * that combines smaller rendered strings into a space-separated string
   */
  def join[T](caseClass: CaseClass[OptionRenderer, T]): OptionRenderer[T] =
    (value: T) =>
      caseClass.parameters
        .map(p => p.typeclass.render(p.dereference(value)))
        .filterNot(_.isBlank)
        .mkString(" ")

  def derive[T]: OptionRenderer[T] = macro Magnolia.gen[T]
}
