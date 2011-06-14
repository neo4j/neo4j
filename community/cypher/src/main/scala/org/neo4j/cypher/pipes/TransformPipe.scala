/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.cypher.pipes

import java.lang.String
import org.neo4j.cypher.SymbolTable
import org.neo4j.graphdb.{NotFoundException, PropertyContainer}
import org.neo4j.cypher.commands._
import collection.Seq

class TransformPipe(returnItems: Seq[ReturnItem], source: Pipe) extends Pipe {
  type MapTransformer = Map[String, Any] => Map[String, Any]

  def getSymbolType(item: ReturnItem): Identifier = item.identifier
  val returnIdentifiers = returnItems.map(x => x.identifier.name -> x.identifier).toMap
  val symbols: SymbolTable = new SymbolTable(returnIdentifiers)

  var transformers: Seq[MapTransformer] = createMapTransformers(returnItems, symbols)

  def createMapTransformers(returnItems: Seq[ReturnItem], symbolTable: SymbolTable): Seq[MapTransformer] = {

    returnItems.map((selectItem) => {
      selectItem match {
        case PropertyOutput(nodeName, propName) => {
          source.symbols.assertHas(nodeName)
          nodePropertyOutput(nodeName, propName) _
        }

        case NullablePropertyOutput(nodeName, propName) => {
          source.symbols.assertHas(nodeName)
          nullablePropertyOutput(nodeName, propName) _
        }

        case EntityOutput(nodeName) => {
          source.symbols.assertHas(nodeName)
          entityOutput(nodeName) _
        }
      }
    })
  }

  def entityOutput(column: String)(m: Map[String, Any]): Map[String, Any] = Map(column -> m.getOrElse(column, throw new NotFoundException))

  def nodePropertyOutput(column: String, propName: String)(m: Map[String, Any]): Map[String, Any] = {
    val node = m.getOrElse(column, throw new NotFoundException).asInstanceOf[PropertyContainer]
    Map(column + "." + propName -> node.getProperty(propName))
  }

  def nullablePropertyOutput(column: String, propName: String)(m: Map[String, Any]): Map[String, Any] = {
    val node = m.getOrElse(column, throw new NotFoundException).asInstanceOf[PropertyContainer]

    val property = try {
      node.getProperty(propName)
    } catch {
      case x: NotFoundException => null
    }

    Map(column + "." + propName -> property)
  }

  def foreach[U](f: (Map[String, Any]) => U) {
    source.foreach(row => {
      val projection = transformers.map(_(row)).reduceLeft(_ ++ _)
      f.apply(projection)
    })
  }
}

