/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.cypher

import javacompat.{PlanDescription => JPlanDescription}

/**
 * Abstract description of an execution plan
 */
trait PlanDescription {
  def name: String

  /**
   * Render this plan description and all predecessor step descriptions to builder using the default separator
   *
   * @param builder StringBuilder to be used
   */
  def render(builder: StringBuilder)

  /**
   * Render this plan description and all predecessor step descriptions to builder
   *
   * @param builder StringBuilder to be used
   * @param separator separator to be inserted between predecessor step descriptions
   * @param levelSuffix separator suffix to be added per child nesting level
   */
  def render(builder: StringBuilder, separator: String, levelSuffix: String)

  def asJava: JPlanDescription
}
