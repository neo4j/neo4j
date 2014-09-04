/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_2.perty

/**
 * This package provides a mechanism for the step-wise
 * conversion of values of type I into values of type O
 *
 * Conversions happens using instances of class Digger
 * that convert input in a "layered" fashion.
 * Each layer is converted using a given extractor
 * function but also passed another extractor function
 * for converting it's members (child nodes) of type M.
 *
 * @see Digger
 **/
package object bling {
  /** Type of an "unapply-like" conversion function */
  type Extractor[-I, +O] = I => Option[O]

  /**
   * A layering extractor returns an extractor
   * if given another extractor that is used for
   * "un-applying" it's members (child nodes)
   */
  type LayerExtractor[+M, O] = Extractor[Extractor[M, O], O]

  /**
   * A drill selects a layer extractor
   * for a given input value
   */
  type Drill[-A, +M, O] = A => LayerExtractor[M, O]

  /**
   * Type of error handling functions that can lift
   * an layer extractor like total function to a
   * layer extractor
   *
   * This is useful for keeping failure to extract
   * a member (child node) an orthogonal concern
   * of an outer layer extractor
   */
  type ExtractionFailureHandler[M, O] = ((M => O) => O) => LayerExtractor[M, O]
}
