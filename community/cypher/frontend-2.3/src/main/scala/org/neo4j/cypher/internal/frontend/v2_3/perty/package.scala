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
package org.neo4j.cypher.internal.frontend.v2_3

import org.neo4j.cypher.internal.frontend.v2_3.perty.print.PrintCommand
import org.neo4j.cypher.internal.frontend.v2_3.perty.step.{DocStep, PrintableDocStep}

import scala.collection.mutable

package object perty {
  // DocRecipe is a description of how to construct a Doc
  // by executing a sequence of DocSteps. DocSteps
  // may contain content that still needs to be converted
  // to a Doc (i.e. it includes un-rendered parts).
  //
  // You should build these using the Pretty DSL
  //
  type DocRecipe[+T] = Seq[DocStep[T]]

  // DocGenStrategy is an extractor that may produce
  // a DocRecipe for a given value (that could be used
  // to produce a Doc for that value)
  type DocGenStrategy[-T] = Extractor[T, DocRecipe[Any]]

  // PrintableDocRecipe is a DocRecipe that does NOT
  // contain content that still needs to be converted to a Doc
  // (i.e. is ready to be formatted and printed)
  type PrintableDocRecipe = Seq[PrintableDocStep]

  // A PrintableDocGenStrategy is a DocGenStrategy that
  // produces PrintableDocRecipes.
  type PrintableDocGenStrategy[-T] = Extractor[T, PrintableDocRecipe]

  // DocFormatters layout a given doc as a series of print commands
  type DocFormatter = Doc => Seq[PrintCommand]

  // PrintingConverters turn a sequence of print commands into
  // a result of type T (usually String)
  type PrintingConverter[+T] = mutable.Builder[PrintCommand, T]
}


