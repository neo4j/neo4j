/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.options

import magnolia.CaseClass
import magnolia.Magnolia

import scala.language.experimental.macros

/**
 * Creates logical plan cache key strings from values
 */
trait OptionLogicalPlanCacheKey[T] {
  def logicalPlanCacheKey(value: T): String
}

object OptionLogicalPlanCacheKey {

  def create[T](func: T => String): OptionLogicalPlanCacheKey[T] =
    (value: T) => func(value)

  // Magnolia generic derivation
  // Check out the tutorial at https://propensive.com/opensource/magnolia/tutorial

  type Typeclass[T] = OptionLogicalPlanCacheKey[T]

  /**
   * Generic OptionLogicalPlanCacheKey for any case class (given that there are OptionLogicalPlanCacheKey:s for all its parameter types)
   * that combines smaller cache keys into a space-separated string
   */
  def combine[T](caseClass: CaseClass[OptionLogicalPlanCacheKey, T]): OptionLogicalPlanCacheKey[T] =
    (value: T) =>
      caseClass.parameters
        .map(param => param.typeclass.logicalPlanCacheKey(param.dereference(value)))
        .filterNot(_.isBlank)
        .mkString(" ")

  def derive[T]: OptionLogicalPlanCacheKey[T] = macro Magnolia.gen[T]
}
