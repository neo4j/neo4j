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
package org.neo4j.cypher.internal.compiler.v2_2.perty.bling

import scala.language.higherKinds

import scala.reflect.runtime.universe.TypeTag

/**
 * A layered extractor converts value of type A into value of type O.
 * It can be specialized over how to convert inner (child) values by
 * providing it with an inner extractor.
 *
 * When used as a regular extractor, layered extractors are
 * specialized using their own fix point
 */
abstract class LayeredExtractor[-I : TypeTag, O : TypeTag] extends Extractor[I, O] {
  self =>

  override type Self[-U, V] <: LayeredExtractor[U, V]

  def apply[ X <: I : TypeTag](x: X): Option[O] = fixPoint(x)

  def fixPoint: Extractor[I, O]

  def fix(inner: Extractor[Any, O]): Extractor[I, O]
}

