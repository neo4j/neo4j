/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v2_3.perty.handler

import org.neo4j.cypher.internal.frontend.v2_3.perty._
import org.neo4j.cypher.internal.frontend.v2_3.perty.gen.{docStructureDocGen, scalaDocGen, toStringDocGen}

// Like DefaultDocHandler just without supporting Pretty.toDocOps
//
// This is helpful when reflectively printing scala case classes whose toDoc leaves out information
//
case object SimpleDocHandler extends CustomDocHandler[Any] {
  val docGen: DocGenStrategy[Any] =
    // pretty printing the structure of docs themselves
    docStructureDocGen.lift[Any] orElse
    // pretty printing anything common scala value (product, array, primitive)
    scalaDocGen orElse
    // pretty printing by falling back to toString()
    toStringDocGen
}
