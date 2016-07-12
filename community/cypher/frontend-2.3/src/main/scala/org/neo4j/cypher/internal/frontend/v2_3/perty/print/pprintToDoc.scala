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
package org.neo4j.cypher.internal.frontend.v2_3.perty.print

import org.neo4j.cypher.internal.frontend.v2_3.perty._
import org.neo4j.cypher.internal.frontend.v2_3.perty.handler.DefaultDocHandler
import org.neo4j.cypher.internal.frontend.v2_3.perty.recipe.{PrintableDocRecipe, DocRecipe}
import org.neo4j.cypher.internal.frontend.v2_3.perty.step.AddPretty

import scala.reflect.runtime.universe._

object pprintToDoc {
  def apply[T : TypeTag, S >: T : TypeTag](value: T)(docGen: DocGenStrategy[S] = DefaultDocHandler.docGen): Doc =
    PrintableDocRecipe.evalUsingStrategy[T, S](docGen).apply(Seq(AddPretty(value)))
}
