/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.docgen

import org.neo4j.cypher.internal.frontend.v2_3.perty._
import org.neo4j.cypher.internal.frontend.v2_3.perty.handler.{SimpleDocHandler, DefaultDocHandler}

// Doc builder for printing any internal class
case object InternalDocHandler extends CustomDocHandler[Any] {

  // Remove all except for DefaultDocHandler if you hit any problems with pretty printing
  val docGen: DocGenStrategy[Any] =
    // Hook in to see ast nodes both using Cypher syntax and using scalaDocGen
//    AstStructureDocGen.lift[Any] orElse
    AstDocHandler.docGen.lift[Any] orElse
    logicalPlanDocGen.lift[Any] orElse
    plannerDocGen orElse
    DefaultDocHandler.docGen
}
