/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.rewriting

import org.neo4j.cypher.internal.ast.CreateIndexNewSyntax
import org.neo4j.cypher.internal.ast.CreateNodeKeyConstraint
import org.neo4j.cypher.internal.ast.CreateNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateUniquePropertyConstraint
import org.neo4j.cypher.internal.ast.DefaultGraphScope
import org.neo4j.cypher.internal.ast.DropConstraintOnName
import org.neo4j.cypher.internal.ast.DropIndexOnName
import org.neo4j.cypher.internal.ast.IfExistsThrowError
import org.neo4j.cypher.internal.ast.ShowPrivileges
import org.neo4j.cypher.internal.ast.ShowRolesPrivileges
import org.neo4j.cypher.internal.ast.ShowUsersPrivileges
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.util.CypherExceptionFactory

object Additions {

  // This is functionality that has been added in 4.0 and 4.1 and should not work when using CYPHER 3.5
  case object addedFeaturesIn4_x extends Additions {

    override def check(statement: Statement, cypherExceptionFactory: CypherExceptionFactory): Unit = statement.treeExists {

      case u: UseGraph =>
        throw cypherExceptionFactory.syntaxException("The USE clause is not supported in this Cypher version.", u.position)

      // CREATE INDEX [name] FOR (n:Label) ON (n.prop)
      case c: CreateIndexNewSyntax =>
        throw cypherExceptionFactory.syntaxException("Creating index using this syntax is not supported in this Cypher version.", c.position)

      // DROP INDEX name
      case d: DropIndexOnName =>
        throw cypherExceptionFactory.syntaxException("Dropping index by name is not supported in this Cypher version.", d.position)

      // CREATE CONSTRAINT name ON ... IS NODE KEY
      case c@CreateNodeKeyConstraint(_, _, _, Some(_), _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating named node key constraint is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT name ON ... IS UNIQUE
      case c@CreateUniquePropertyConstraint(_, _, _, Some(_),_, _) =>
        throw cypherExceptionFactory.syntaxException("Creating named uniqueness constraint is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT name ON () ... EXISTS
      case c@CreateNodePropertyExistenceConstraint(_, _, _, Some(_),_, _) =>
        throw cypherExceptionFactory.syntaxException("Creating named node existence constraint is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT name ON ()-[]-() ... EXISTS
      case c@CreateRelationshipPropertyExistenceConstraint(_, _, _, Some(_), _,_) =>
        throw cypherExceptionFactory.syntaxException("Creating named relationship existence constraint is not supported in this Cypher version.", c.position)

      // DROP CONSTRAINT name
      case d: DropConstraintOnName =>
        throw cypherExceptionFactory.syntaxException("Dropping constraint by name is not supported in this Cypher version.", d.position)

      case e: ExistsSubClause =>
        throw cypherExceptionFactory.syntaxException("Existential subquery is not supported in this Cypher version.", e.position)

      // Administration commands against system database are checked in CompilerFactory to cover all of them at once
    }
  }

  // This is functionality that has been added in 4.2 and should not work when using CYPHER 3.5 and CYPHER 4.1
  case object addedFeaturesIn4_2 extends Additions {

    override def check(statement: Statement, cypherExceptionFactory: CypherExceptionFactory): Unit = statement.treeExists {
      // SHOW ROLE role1, role2 PRIVILEGES
      case s@ShowPrivileges(ShowRolesPrivileges(r), _, _, _) if r.size > 1 =>
        throw cypherExceptionFactory.syntaxException("Multiple roles in SHOW ROLE PRIVILEGE command is not supported in this Cypher version.", s.position)

      // SHOW USER user1, user2 PRIVILEGES
      case s@ShowPrivileges(ShowUsersPrivileges(u), _, _, _) if u.size > 1 =>
        throw cypherExceptionFactory.syntaxException("Multiple users in SHOW USER PRIVILEGE command is not supported in this Cypher version.", s.position)

      case d: DefaultGraphScope => throw cypherExceptionFactory.syntaxException("Default graph is not supported in this Cypher version.", d.position)

      // CREATE OR REPLACE INDEX name ...
      // CREATE INDEX [name] IF NOT EXISTS ...
      case c@CreateIndexNewSyntax(_, _, _, _, ifExistsDo, _) if ifExistsDo != IfExistsThrowError =>
        throw cypherExceptionFactory.syntaxException("Creating index using `OR REPLACE` or `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // DROP INDEX name IF EXISTS
      case d@DropIndexOnName(_, true, _) =>
        throw cypherExceptionFactory.syntaxException("Dropping index using `IF EXISTS` is not supported in this Cypher version.", d.position)

      // CREATE OR REPLACE CONSTRAINT name ...
      // CREATE CONSTRAINT [name] IF NOT EXISTS ...
      case c@CreateNodeKeyConstraint(_, _, _, _, ifExistsDo, _) if ifExistsDo != IfExistsThrowError =>
        throw cypherExceptionFactory.syntaxException("Creating node key constraint using `OR REPLACE` or `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // CREATE OR REPLACE CONSTRAINT name ...
      // CREATE CONSTRAINT [name] IF NOT EXISTS ...
      case c@CreateUniquePropertyConstraint(_, _, _, _, ifExistsDo, _) if ifExistsDo != IfExistsThrowError =>
        throw cypherExceptionFactory.syntaxException("Creating uniqueness constraint using `OR REPLACE` or `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // CREATE OR REPLACE CONSTRAINT name ...
      // CREATE CONSTRAINT [name] IF NOT EXISTS ...
      case c@CreateNodePropertyExistenceConstraint(_, _, _, _, ifExistsDo, _) if ifExistsDo != IfExistsThrowError =>
        throw cypherExceptionFactory.syntaxException("Creating node existence constraint using `OR REPLACE` or `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // CREATE OR REPLACE CONSTRAINT name ...
      // CREATE CONSTRAINT [name] IF NOT EXISTS ...
      case c@CreateRelationshipPropertyExistenceConstraint(_, _, _, _, ifExistsDo, _) if ifExistsDo != IfExistsThrowError =>
        throw cypherExceptionFactory.syntaxException("Creating relationship existence constraint using `OR REPLACE` or `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // DROP CONSTRAINT name IF EXISTS
      case d@DropConstraintOnName(_, true, _) =>
        throw cypherExceptionFactory.syntaxException("Dropping constraint using `IF EXISTS` is not supported in this Cypher version.", d.position)
    }
  }
}

trait Additions extends {
  def check(statement: Statement, cypherExceptionFactory: CypherExceptionFactory): Unit = {}
}
