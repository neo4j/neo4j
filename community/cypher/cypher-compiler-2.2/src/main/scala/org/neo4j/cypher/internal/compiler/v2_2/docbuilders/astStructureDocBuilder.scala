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
package org.neo4j.cypher.internal.compiler.v2_2.docbuilders

import org.neo4j.cypher.internal.compiler.v2_2.ast.{ASTParticle, ASTNode}
import org.neo4j.cypher.internal.compiler.v2_2.perty.docbuilders.simpleDocBuilder
import org.neo4j.cypher.internal.compiler.v2_2.perty.{Doc, CustomDocBuilder, DocGenerator}

// Doc builder for printing any kind of ast node together with it's structure
case object astStructureDocBuilder extends CustomDocBuilder[Any] {

  import org.neo4j.cypher.internal.compiler.v2_2.perty.Doc._

  def newDocGenerator: DocGenerator[Any] = DocGenerator[Any] {
    case astNode: ASTNode with ASTParticle => (inner) =>
      astParticleDocBuilder.docGenerator.applyWithInner(inner)(astNode)

    case astNode: ASTNode => (inner) =>
      astDocBuilder
        .docGenerator
        .andThen { (astDoc: Doc) =>
          val simpleDoc: Doc = simpleDocBuilder.docGenerator(astNode)
          nest(group(group(comment("ast") :/: astDoc) :/: group(comment("val") :/: simpleDoc)))
        }
        .applyOrElse[ASTNode, Doc](astNode, simpleDocBuilder.docGenerator.applyWithInner(inner))
  }
}
