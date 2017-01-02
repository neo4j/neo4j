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
package org.neo4j.cypher.internal.compiler.v2_2.helpers

object CollectionFrosting {

  implicit class RichTraversable[T](inner: Traversable[T]) {
    /**
     * If a single element matching the description is found, a Right(Some(x)) is returned, with x being the element.
     *
     * If no elements matching are found, a Right(None) is returned. If multiple matching elements are found,
     * a Left(Seq(...)) with all the matching elements is returned
     * @param pf the partial function to use to match and transform elements
     * @tparam B the type of the produced projections
     */
    def collectSingle[B](pf: PartialFunction[T, B]): Either[Seq[B], Option[B]] = {
      val collect = inner.collect(pf)
      collect.toList match {
        case Nil      => Right(None)
        case x :: Nil => Right(Some(x))
        case matches  => Left(matches)
      }
    }
  }

}
