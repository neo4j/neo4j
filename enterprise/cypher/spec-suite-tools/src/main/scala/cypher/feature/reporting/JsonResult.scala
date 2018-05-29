/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package cypher.feature.reporting


import com.novus.salat.annotations.{Ignore, Key, Persist}
import org.neo4j.cypher.internal.compiler.v3_2.ast.QueryTag

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
