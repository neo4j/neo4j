/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.fabric.planning

import org.neo4j.cypher.internal.ast

sealed trait StatementType {
  // Java access helpers
  def isQuery: Boolean = false
  def isReadQuery: Boolean = false
  def isSchemaCommand: Boolean = false
  def isAdminCommand: Boolean = false
}

object StatementType {

  case class Query(queryType: QueryType) extends StatementType {
    override def toString: String = queryType.toString
    override def isQuery: Boolean = true

    override def isReadQuery: Boolean = queryType match {
      case QueryType.Read => true
      case _              => false
    }
  }

  case object SchemaCommand extends StatementType {
    override def toString: String = "Schema modification"
    override def isSchemaCommand: Boolean = true
  }

  case object AdminCommand extends StatementType {
    override def toString: String = "Administration command"
    override def isAdminCommand: Boolean = true
  }

  def of(statement: ast.Statement): StatementType =
    statement match {
      case q: ast.Query                 => Query(QueryType.of(q))
      case _: ast.SchemaCommand         => SchemaCommand
      case _: ast.AdministrationCommand => AdminCommand
    }
}
