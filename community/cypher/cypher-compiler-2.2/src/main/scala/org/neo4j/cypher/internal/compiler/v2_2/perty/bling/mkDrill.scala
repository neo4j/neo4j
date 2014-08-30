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

case object mkDrill {
  def apply[A, M, O]
    (implicit handler: ExtractionFailureHandler[M, O] = propagateExtractionFailure[M, O]())
  : (PartialFunction[A, ((M) => O) => O]) => Drill[A, M, O] =
    (f: PartialFunction[A, (M => O) => O]) =>
      (v: A) =>
        (extractor: Extractor[M, O]) =>
          f.lift(v).map(handler).flatMap( layered => layered(extractor) )
}
