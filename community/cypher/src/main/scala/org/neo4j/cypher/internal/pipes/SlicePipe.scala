/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.cypher.internal.pipes

import org.neo4j.cypher.internal.commands.Expression
import java.lang.String
import org.neo4j.helpers.ThisShouldNotHappenError
import collection.mutable.Map

class SlicePipe(source:Pipe, skip:Option[Expression], limit:Option[Expression]) extends Pipe {
  val symbols = source.symbols

  //TODO: Make this nicer. I'm sure it's expensive and silly.
  def createResults[U](params: Map[String, Any]): Traversable[Map[String, Any]] = {
    val sourceTraversable = source.createResults(params)

    if(sourceTraversable.isEmpty)
      return Seq()

    val first: Map[String, Any] = sourceTraversable.head

    def asInt(v:Expression)=v(first).asInstanceOf[Int]

    (skip, limit) match {
      case (Some(x), None) => sourceTraversable.drop(asInt(x))
      case (None, Some(x)) => sourceTraversable.take(asInt(x))
      case (Some(startAt), Some(count)) => {
        val start = asInt(startAt)
        sourceTraversable.slice(start, start + asInt(count))
      }
      case (None, None)=>throw new ThisShouldNotHappenError("Andres Taylor", "A slice pipe that doesn't slice should never exist.")
    }
  }

  override def executionPlan(): String = {

    val info = (skip, limit) match {
      case (None, Some(l)) => "Limit: " + l.toString()
      case (Some(s), None) => "Skip: " + s.toString()
      case (Some(s), Some(l)) => "Skip: " + s.toString() + ", " + "Limit: " + l.toString()
      case (None, None)=>throw new ThisShouldNotHappenError("Andres Taylor", "A slice pipe that doesn't slice should never exist.")
    }
    source.executionPlan() + "\r\n" + "Slice(" + info + ")"
  }
}