/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.sunshine.pipes

import java.lang.String

/**
 * Created by Andres Taylor
 * Date: 4/18/11
 * Time: 21:00 
 */

abstract class Pipe extends Traversable[Map[String, Any]] {
//  var input: Option[Pipe] = None

//  protected def getInput: Pipe = input match {
//    case None => throw new RuntimeException("No input defined yet")
//    case Some(x) => x
//  }

//  def dependsOn: List[String]

  def ++(other: Pipe): Pipe = new JoinPipe(this, other)

//  def setInput(pipe: Pipe) {
//    input = Some(pipe)
//  }

  def columnNames: List[String]


//  def childrenNames: String =
//    input match {
//      case None => ""
//      case Some(x) => ".." + x.toString
//    }


//  override def toString(): String =
//    this.getClass.getSimpleName + "(deps:" + dependsOn + " gives:" + columnNames + ")" + childrenNames

}