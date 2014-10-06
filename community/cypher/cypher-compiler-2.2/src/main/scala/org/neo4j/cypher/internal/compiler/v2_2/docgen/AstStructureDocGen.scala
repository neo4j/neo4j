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

import org.neo4j.cypher.internal.compiler.v2_2.ast.{ASTNode, ASTParticle}
import org.neo4j.cypher.internal.compiler.v2_2.perty._
import org.neo4j.cypher.internal.compiler.v2_2.perty.handler.SimpleDocHandler
import org.neo4j.cypher.internal.compiler.v2_2.perty.recipe.DocRecipe.strategyExpander
import org.neo4j.cypher.internal.compiler.v2_2.perty.recipe.{Pretty, RecipeAppender}
import org.neo4j.cypher.internal.compiler.v2_2.perty.step.AddPretty

import scala.reflect.runtime.universe.TypeTag

// Doc builder for printing any kind of ast node together with it's structure
case object astStructureDocGen extends CustomDocGen[ASTNode] {

  import Pretty._

  val simpleDocGen = SimpleDocHandler.docGen
  val astDocGen = InternalDocHandler.docGen
  val astExpander = strategyExpander[ASTNode, Any](astDocGen)

  def apply[X <: Any : TypeTag](x: X): Option[DocRecipe[Any]] = x match {
//    case particle: ASTParticle =>
//      astParticleDocGen(particle)

    case astNode: ASTNode =>
      val astRecipe = astExpander.expand(Seq(AddPretty(astNode)))
//      val result = simpleDocGen(astNode).map { simpleDoc =>
//        nest(group(group(comment("ast") :/: astDoc) :/: group(comment("val") :/: splice(simpleDoc))))
//      }.getOrElse(astDoc)
//      Pretty(result)
      Some(astRecipe)

    case _ =>
      None
  }
}
