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
package cypher.cucumber.reporter

import com.novus.salat.annotations.{Ignore, Key, Persist}
import org.neo4j.cypher.internal.compiler.v2_3.ast.QueryTag

import scala.annotation.meta.getter

object Outcome {
  def from(value: String) = value match {
    case "passed" => Success
    case _ => Failure
  }
}

sealed trait Outcome

object Success extends Outcome {
  override def toString = "success"
}

object Failure extends Outcome {
  override def toString = "failure"
}

case class JsonResult(query: String, @Ignore tags: Set[QueryTag], @Ignore outcome: Outcome) {
  @Key("tags")
  @(Persist@getter)
  val prettyTags: Set[String] = tags.map(_.toString)

  @Key("outcome")
  @(Persist@getter)
  val prettyOutcome = outcome.toString
}
