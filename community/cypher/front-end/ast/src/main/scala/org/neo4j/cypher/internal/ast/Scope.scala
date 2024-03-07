/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.cypher.internal.ast

import org.neo4j.cypher.internal.expressions.Expression
import org.neo4j.cypher.internal.util.InputPosition
import org.neo4j.cypher.internal.util.Rewritable

// Graph scopes

sealed trait GraphScope extends Rewritable {
  def simplify: Seq[GraphScope] = Seq(this)
  override def dup(children: Seq[AnyRef]): GraphScope.this.type = this
}

final case class NamedGraphsScope(graphs: Seq[DatabaseName])(val position: InputPosition)
    extends GraphScope {

  override def dup(children: Seq[AnyRef]): NamedGraphsScope.this.type =
    copy(children.head.asInstanceOf[Seq[DatabaseName]])(position).asInstanceOf[this.type]

  override def simplify: Seq[GraphScope] = graphs.map(graph => SingleNamedGraphScope(graph)(graph.position))
}

final case class SingleNamedGraphScope(graph: DatabaseName)(val position: InputPosition) extends GraphScope {

  override def dup(children: Seq[AnyRef]): SingleNamedGraphScope.this.type =
    this.copy(children.head.asInstanceOf[DatabaseName])(position).asInstanceOf[this.type]
}

final case class AllGraphsScope()(val position: InputPosition) extends GraphScope

final case class DefaultGraphScope()(val position: InputPosition) extends GraphScope

final case class HomeGraphScope()(val position: InputPosition) extends GraphScope

// Database scopes

sealed trait DatabaseScope extends Rewritable {
  val showCommandName: String

  override def dup(children: Seq[AnyRef]): DatabaseScope.this.type = this

  def simplify: Seq[DatabaseScope] = Seq(this)
}

final case class NamedDatabasesScope(databases: Seq[DatabaseName])(val position: InputPosition)
    extends DatabaseScope {

  override def dup(children: Seq[AnyRef]): NamedDatabasesScope.this.type =
    copy(children.head.asInstanceOf[Seq[DatabaseName]])(position).asInstanceOf[this.type]

  override def simplify: Seq[DatabaseScope] = databases.map(db => SingleNamedDatabaseScope(db)(db.position))

  override val showCommandName: String = "ShowDatabase"
}

final case class SingleNamedDatabaseScope(database: DatabaseName)(val position: InputPosition) extends DatabaseScope {

  override def dup(children: Seq[AnyRef]): SingleNamedDatabaseScope.this.type =
    this.copy(children.head.asInstanceOf[DatabaseName])(position).asInstanceOf[this.type]

  override val showCommandName: String = "ShowDatabase"
}

final case class AllDatabasesScope()(val position: InputPosition) extends DatabaseScope {
  override val showCommandName: String = "ShowDatabases"
}

final case class DefaultDatabaseScope()(val position: InputPosition) extends DatabaseScope {
  override val showCommandName: String = "ShowDefaultDatabase"
}

final case class HomeDatabaseScope()(val position: InputPosition) extends DatabaseScope {
  override val showCommandName: String = "ShowHomeDatabase"
}

// Dbms scopes

sealed trait ShowPrivilegeScope extends Rewritable {
  override def dup(children: Seq[AnyRef]): ShowPrivilegeScope.this.type = this
}

final case class ShowRolesPrivileges(roles: List[Expression])(val position: InputPosition)
    extends ShowPrivilegeScope {

  override def dup(children: Seq[AnyRef]): ShowRolesPrivileges.this.type =
    this.copy(children.head.asInstanceOf[List[Expression]])(position).asInstanceOf[this.type]
}

final case class ShowUserPrivileges(user: Option[Expression])(val position: InputPosition)
    extends ShowPrivilegeScope {

  override def dup(children: Seq[AnyRef]): ShowUserPrivileges.this.type =
    this.copy(children.head.asInstanceOf[Option[Expression]])(position).asInstanceOf[this.type]
}

final case class ShowUsersPrivileges(users: List[Expression])(val position: InputPosition)
    extends ShowPrivilegeScope {

  override def dup(children: Seq[AnyRef]): ShowUsersPrivileges.this.type =
    this.copy(children.head.asInstanceOf[List[Expression]])(position).asInstanceOf[this.type]
}

final case class ShowAllPrivileges()(val position: InputPosition) extends ShowPrivilegeScope
