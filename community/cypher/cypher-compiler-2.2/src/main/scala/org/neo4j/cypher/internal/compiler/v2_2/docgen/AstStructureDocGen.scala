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
package org.neo4j.cypher.internal.compiler.v2_2.docgen

import org.neo4j.cypher.internal.compiler.v2_2.ast.{SymbolicName, LabelName, ASTParticle, ASTNode}
import org.neo4j.cypher.internal.compiler.v2_2.perty.handler.SimpleDocHandler
import org.neo4j.cypher.internal.compiler.v2_2.perty.{DocDrill, mkDocDrill, CustomDocGen}

// Doc builder for printing any kind of ast node together with it's structure
case object AstStructureDocGen extends CustomDocGen[ASTNode] {

  import org.neo4j.cypher.internal.compiler.v2_2.perty.Doc._

  def drill =
    ???
//  {
//    case particle: ASTParticle =>
//      inner => astParticleDocGen(inner)(particle)
//
//    case astNode: ASTNode =>
//      inner =>
//        val ast = AstDocHandlerWithFallback.docGen(inner)(astNode)
//        val simple = SimpleDocHandler.docGen(inner)(astNode)
//        Some(nest(group(group(comment("ast") :/: ast) :/: group(comment("val") :/: simple))))
//
//    case _ =>
//      inner => None
//  }
}
