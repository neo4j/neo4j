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
package cypher.cucumber.classifier

import com.novus.salat.annotations._
import org.neo4j.cypher.internal.compiler.v2_3.ast.QueryTag

import scala.annotation.meta.getter

case class Scenario(featureName: String, name: String, @Ignore attributes: Seq[Attribute]) {
  @Key("steps")
  @(Persist@getter)
  val prettySteps: Map[String, Attribute] = attributes.map {m => m.prefix -> m }.toMap
}

sealed trait Attribute {
  self: Product =>

  def prefix = self.productPrefix.toLowerCase
}

case class Init(query: String, docString: Option[String]) extends Attribute
case class Using(database: String, docString: Option[String]) extends Attribute
case class Run(query: String, @Ignore tags: Set[QueryTag], params: Option[Map[String,AnyRef]], docString: Option[String]) extends Attribute {
  @Key("tags")
  @(Persist@getter)
  val prettyTags: Set[String] = tags.map(_.toString)
}
case class Result(data: List[Map[String, String]], docString: Option[String]) extends Attribute
