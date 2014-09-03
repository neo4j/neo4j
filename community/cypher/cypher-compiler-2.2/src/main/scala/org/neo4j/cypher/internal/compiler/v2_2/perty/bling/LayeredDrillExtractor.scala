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

abstract class LayeredDrillExtractor[-I : TypeTag, O : TypeTag] extends LayeredExtractor[I, O] {
  self =>

  type Self[-U, V] <: LayeredDrillExtractor[U, V]

  def drill: Drill[I, O]

  def fix(inner: Extractor[Any, O]): Extractor[I, O] = drill(inner)

  def mapInput[U : TypeTag, H <: I : TypeTag](f: Extractor[U, H]): Self[U, O] =
    mapDrill[U, O] { (drill: Drill[I, O]) =>
      (extractor: Extractor[Any, O]) => drill(extractor).mapInput(f)
    }

  def mapOutput[H <: I : TypeTag](f: Extractor[O, O]): Self[H, O] =
    mapDrill[H, O] { (drill: Drill[I, O]) =>
      (extractor: Extractor[Any, O]) => drill(extractor).mapOutput(f)
    }

  def orElse[H <: I : TypeTag](first: Extractor[H, O]): Extractor[H, O] =
    fixPoint.orElse(first)

  def andThen[P >: O : TypeTag, V : TypeTag](other: Extractor[P, V]): Extractor[I, V] =
    fixPoint.andThen(other)

  def mapDrill[U : TypeTag, V : TypeTag](f: Drill[I, O] => Drill[U, V]): Self[U, V]
}
