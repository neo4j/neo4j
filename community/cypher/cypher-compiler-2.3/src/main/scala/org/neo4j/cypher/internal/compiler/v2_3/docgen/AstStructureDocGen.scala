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

import org.neo4j.cypher.internal.frontend.v2_3.ast.{ASTNode, ASTParticle}
import org.neo4j.cypher.internal.frontend.v2_3.perty._
import org.neo4j.cypher.internal.frontend.v2_3.perty.handler.SimpleDocHandler
import org.neo4j.cypher.internal.frontend.v2_3.perty.recipe.DocRecipe.strategyExpander
import org.neo4j.cypher.internal.frontend.v2_3.perty.recipe.Pretty
import org.neo4j.cypher.internal.frontend.v2_3.perty.step.AddPretty

import scala.reflect.runtime.universe.TypeTag

// Doc builder for printing any kind of ast node together with it's structure
case object astStructureDocGen extends CustomDocGen[ASTNode] {

  import Pretty._

  val astExpander = strategyExpander[ASTNode, Any](InternalDocHandler.docGen)
  val simpleExpander = strategyExpander[ASTNode, Any](SimpleDocHandler.docGen)

  def apply[X <: Any : TypeTag](x: X): Option[DocRecipe[Any]] = x match {
    case particle: ASTParticle =>
      astParticleDocGen(particle)

    case astNode: ASTNode =>
      val pretties: Seq[AddPretty[ASTNode]] = Seq(AddPretty(astNode))
      val astRecipe = astExpander.expandForQuoting(pretties)
      val simpleRecipe = simpleExpander.expandForQuoting(pretties)
      val result = nest(group(group(comment("ast") :/: quote(astRecipe)) :/: group(comment("val") :/: quote(simpleRecipe))))
      Pretty(result)

    case _ =>
      None
  }
}
