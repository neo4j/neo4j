/*
 * Copyright (c) "Neo4j"
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

import org.neo4j.cypher.internal.ast.AlterDatabase
import org.neo4j.cypher.internal.ast.AlterDatabaseAction
import org.neo4j.cypher.internal.ast.ConstraintVersion1
import org.neo4j.cypher.internal.ast.ConstraintVersion2
import org.neo4j.cypher.internal.ast.CreateBtreeNodeIndex
import org.neo4j.cypher.internal.ast.CreateBtreeRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateFulltextNodeIndex
import org.neo4j.cypher.internal.ast.CreateFulltextRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateLookupIndex
import org.neo4j.cypher.internal.ast.CreateNodeKeyConstraint
import org.neo4j.cypher.internal.ast.CreateNodePropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreatePointNodeIndex
import org.neo4j.cypher.internal.ast.CreatePointRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateRangeNodeIndex
import org.neo4j.cypher.internal.ast.CreateRangeRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateRelationshipPropertyExistenceConstraint
import org.neo4j.cypher.internal.ast.CreateTextNodeIndex
import org.neo4j.cypher.internal.ast.CreateTextRelationshipIndex
import org.neo4j.cypher.internal.ast.CreateUniquePropertyConstraint
import org.neo4j.cypher.internal.ast.DbmsPrivilege
import org.neo4j.cypher.internal.ast.DenyPrivilege
import org.neo4j.cypher.internal.ast.DropConstraintOnName
import org.neo4j.cypher.internal.ast.DropIndexOnName
import org.neo4j.cypher.internal.ast.GrantPrivilege
import org.neo4j.cypher.internal.ast.IfExistsDoNothing
import org.neo4j.cypher.internal.ast.ImpersonateUserAction
import org.neo4j.cypher.internal.ast.NoOptions
import org.neo4j.cypher.internal.ast.Options
import org.neo4j.cypher.internal.ast.OptionsMap
import org.neo4j.cypher.internal.ast.PointIndexes
import org.neo4j.cypher.internal.ast.RangeIndexes
import org.neo4j.cypher.internal.ast.RevokePrivilege
import org.neo4j.cypher.internal.ast.SetDatabaseAccessAction
import org.neo4j.cypher.internal.ast.ShowConstraintsClause
import org.neo4j.cypher.internal.ast.ShowFunctionsClause
import org.neo4j.cypher.internal.ast.ShowIndexesClause
import org.neo4j.cypher.internal.ast.ShowProceduresClause
import org.neo4j.cypher.internal.ast.ShowTransactionsClause
import org.neo4j.cypher.internal.ast.Statement
import org.neo4j.cypher.internal.ast.TerminateTransactionsClause
import org.neo4j.cypher.internal.ast.TextIndexes
import org.neo4j.cypher.internal.ast.UniquePropertyConstraintCommand
import org.neo4j.cypher.internal.ast.UnresolvedCall
import org.neo4j.cypher.internal.ast.UseGraph
import org.neo4j.cypher.internal.expressions.ExistsSubClause
import org.neo4j.cypher.internal.expressions.StringLiteral
import org.neo4j.cypher.internal.util.CypherExceptionFactory

object Additions {

  // This is functionality that has been added earlier in 4.x and should not work when using CYPHER 3.5
  case object addedFeaturesIn4_x extends Additions {

    override def check(statement: Statement, cypherExceptionFactory: CypherExceptionFactory): Unit = statement.treeExists {

      case u: UseGraph =>
        throw cypherExceptionFactory.syntaxException("The USE clause is not supported in this Cypher version.", u.position)

      case c: UnresolvedCall if c.yieldAll =>
        throw cypherExceptionFactory.syntaxException("Procedure call using `YIELD *` is not supported in this Cypher version.", c.position)

      // CREATE [BTREE] INDEX [name] [IF NOT EXISTS] FOR (n:Label) ON (n.prop) [OPTIONS {...}]
      case c: CreateBtreeNodeIndex =>
        throw cypherExceptionFactory.syntaxException("Creating index using this syntax is not supported in this Cypher version.", c.position)
      case c: CreateRangeNodeIndex if c.fromDefault =>
        // Range index with `fromDefault` is a btree index, this was done to prepare for RANGE being the default index
        throw cypherExceptionFactory.syntaxException("Creating index using this syntax is not supported in this Cypher version.", c.position)

      // CREATE [BTREE] INDEX [name] [IF NOT EXISTS] FOR ()-[n:RelType]-() ON (n.prop) [OPTIONS {...}]
      case c: CreateBtreeRelationshipIndex =>
        throw cypherExceptionFactory.syntaxException("Relationship property indexes are not supported in this Cypher version.", c.position)
      case c: CreateRangeRelationshipIndex if c.fromDefault =>
        // Range index with `fromDefault` is a btree index, this was done to prepare for RANGE being the default index
        throw cypherExceptionFactory.syntaxException("Relationship property indexes are not supported in this Cypher version.", c.position)

      // CREATE LOOKUP INDEX ...
      case c: CreateLookupIndex =>
        throw cypherExceptionFactory.syntaxException("Lookup indexes are not supported in this Cypher version.", c.position)

      // CREATE FULLTEXT INDEX ...
      case c: CreateFulltextNodeIndex =>
        throw cypherExceptionFactory.syntaxException("Fulltext indexes can only be created using procedures in this Cypher version.", c.position)
      case c: CreateFulltextRelationshipIndex =>
        throw cypherExceptionFactory.syntaxException("Fulltext indexes can only be created using procedures in this Cypher version.", c.position)

      // DROP INDEX name
      case d: DropIndexOnName =>
        throw cypherExceptionFactory.syntaxException("Dropping index by name is not supported in this Cypher version.", d.position)

      // CREATE CONSTRAINT name ON ... IS NODE KEY
      case c@CreateNodeKeyConstraint(_, _, _, Some(_), _, _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating named node key constraint is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] IF NOT EXISTS ON ... IS NODE KEY
      case c@CreateNodeKeyConstraint(_, _, _, _, IfExistsDoNothing, _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating node key constraint using `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT ... IS NODE KEY OPTIONS {...}
      case c@CreateNodeKeyConstraint(_, _, _, _, _, options, _, _, _) if options != NoOptions =>
        throw cypherExceptionFactory.syntaxException("Creating node key constraint with options is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT name ON ... IS UNIQUE
      case c@CreateUniquePropertyConstraint(_, _, _, Some(_),_, _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating named uniqueness constraint is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] IF NOT EXISTS ON ... IS UNIQUE
      case c@CreateUniquePropertyConstraint(_, _, _, _, IfExistsDoNothing, _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating uniqueness constraint using `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT ... IS UNIQUE OPTIONS {...}
      case c@CreateUniquePropertyConstraint(_, _, _, _, _, options, _, _, _) if options != NoOptions =>
        throw cypherExceptionFactory.syntaxException("Creating uniqueness constraint with options is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT name ON () ... EXISTS
      case c@CreateNodePropertyExistenceConstraint(_, _, _, Some(_), _, _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating named node existence constraint is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] IF NOT EXISTS ON () ... EXISTS
      case c@CreateNodePropertyExistenceConstraint(_, _, _, _, IfExistsDoNothing, _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating node existence constraint using `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON (node:Label) ASSERT node.prop IS NOT NULL
      case c: CreateNodePropertyExistenceConstraint if c.constraintVersion == ConstraintVersion1 =>
        throw cypherExceptionFactory.syntaxException("Creating node existence constraint using `IS NOT NULL` is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] ON (node:Label) ASSERT EXISTS (node:Label) OPTIONS {...}
      case c@CreateNodePropertyExistenceConstraint(_, _, _, _, _, options, _, _, _) if options != NoOptions =>
        throw cypherExceptionFactory.syntaxException("Creating node existence constraint with options is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT name ON ()-[]-() ... EXISTS
      case c@CreateRelationshipPropertyExistenceConstraint(_, _, _, Some(_), _, _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating named relationship existence constraint is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] IF NOT EXISTS ON ()-[]-() ... EXISTS
      case c@CreateRelationshipPropertyExistenceConstraint(_, _, _, _, IfExistsDoNothing, _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("Creating relationship existence constraint using `IF NOT EXISTS` is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] ON ()-[r:R]-() ASSERT r.prop IS NOT NULL
      case c: CreateRelationshipPropertyExistenceConstraint if c.constraintVersion == ConstraintVersion1 =>
        throw cypherExceptionFactory.syntaxException("Creating relationship existence constraint using `IS NOT NULL` is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] ON ()-[r:R]-() ASSERT EXISTS (r.prop) OPTIONS {...}
      case c@CreateRelationshipPropertyExistenceConstraint(_, _, _, _, _, options, _, _, _) if options != NoOptions =>
        throw cypherExceptionFactory.syntaxException("Creating relationship existence constraint with options is not supported in this Cypher version.", c.position)

      // DROP CONSTRAINT name
      case d: DropConstraintOnName =>
        throw cypherExceptionFactory.syntaxException("Dropping constraint by name is not supported in this Cypher version.", d.position)

      case e: ExistsSubClause =>
        throw cypherExceptionFactory.syntaxException("Existential subquery is not supported in this Cypher version.", e.position)

      // SHOW [ALL|BTREE|FULLTEXT|LOOKUP|POINT|RANGE|TEXT] INDEX[ES] [WHERE clause|YIELD clause]
      case s: ShowIndexesClause =>
        throw cypherExceptionFactory.syntaxException("SHOW INDEXES is not supported in this Cypher version.", s.position)

      // SHOW [ALL|UNIQUE|NODE [PROPERTY] EXIST[ENCE]|RELATIONSHIP [PROPERTY] EXIST[ENCE]|EXIST[ENCE]|NODE KEY] CONSTRAINT[S] [WHERE clause|YIELD clause]
      case s: ShowConstraintsClause =>
        throw cypherExceptionFactory.syntaxException("SHOW CONSTRAINTS is not supported in this Cypher version.", s.position)

      // SHOW PROCEDURE[S] [EXECUTABLE [BY {CURRENT USER | username}]] [WHERE clause | YIELD clause]
      case c: ShowProceduresClause =>
        throw cypherExceptionFactory.syntaxException("`SHOW PROCEDURES` is not supported in this Cypher version.", c.position)

      // SHOW [ALL | BUILT IN | USER DEFINED] FUNCTION[S] [EXECUTABLE [BY {CURRENT USER | username}]] [WHERE clause | YIELD clause]
      case c: ShowFunctionsClause =>
        throw cypherExceptionFactory.syntaxException("`SHOW FUNCTIONS` is not supported in this Cypher version.", c.position)

      // Administration commands against system database are not supported at all in CYPHER 3.5.
      // This is checked in CompilerFactory, so separate checks for such commands are not needed here.
    }
  }

  // This is functionality that has been added in 4.4 and should not work when using CYPHER 3.5 and CYPHER 4.3
  case object addedFeaturesIn4_4 extends Additions {

    override def check(statement: Statement, cypherExceptionFactory: CypherExceptionFactory): Unit = statement.treeExists {

      case c: UniquePropertyConstraintCommand if c.properties.size > 1 =>
        throw cypherExceptionFactory.syntaxException("Multi-property uniqueness constraints are not supported in this Cypher version.", c.position)

      // CREATE RANGE INDEX ...
      case c: CreateRangeNodeIndex if !c.fromDefault =>
        throw cypherExceptionFactory.syntaxException("Range indexes are not supported in this Cypher version.", c.position)
      case c: CreateRangeRelationshipIndex if !c.fromDefault =>
        throw cypherExceptionFactory.syntaxException("Range indexes are not supported in this Cypher version.", c.position)

      // SHOW RANGE INDEXES
      case s: ShowIndexesClause if s.indexType == RangeIndexes =>
        throw cypherExceptionFactory.syntaxException("Filtering on range indexes in SHOW INDEXES is not supported in this Cypher version.", s.position)

      // CREATE TEXT INDEX ...
      case c: CreateTextNodeIndex =>
        throw cypherExceptionFactory.syntaxException("Text indexes are not supported in this Cypher version.", c.position)
      case c: CreateTextRelationshipIndex =>
        throw cypherExceptionFactory.syntaxException("Text indexes are not supported in this Cypher version.", c.position)

      // SHOW TEXT INDEXES
      case s: ShowIndexesClause if s.indexType == TextIndexes =>
        throw cypherExceptionFactory.syntaxException("Filtering on text indexes in SHOW INDEXES is not supported in this Cypher version.", s.position)

      // CREATE POINT INDEX ...
      case c: CreatePointNodeIndex =>
        throw cypherExceptionFactory.syntaxException("Point indexes are not supported in this Cypher version.", c.position)
      case c: CreatePointRelationshipIndex =>
        throw cypherExceptionFactory.syntaxException("Point indexes are not supported in this Cypher version.", c.position)

      // SHOW POINT INDEXES
      case s: ShowIndexesClause if s.indexType == PointIndexes =>
        throw cypherExceptionFactory.syntaxException("Filtering on point indexes in SHOW INDEXES is not supported in this Cypher version.", s.position)

      // CREATE CONSTRAINT ... OPTIONS {indexProvider:  'range-1.0'}
      case c: CreateNodeKeyConstraint if hasRangeOptions(c.options) =>
        throw cypherExceptionFactory.syntaxException("Creating node key constraint backed by range index is not supported in this Cypher version.", c.position)
      case c: CreateUniquePropertyConstraint if hasRangeOptions(c.options) =>
        throw cypherExceptionFactory.syntaxException("Creating uniqueness constraint backed by range index is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE node.prop IS NOT NULL
      case c: CreateNodePropertyExistenceConstraint if c.constraintVersion == ConstraintVersion2 =>
        throw cypherExceptionFactory.syntaxException("Creating node existence constraint using `FOR ... REQUIRE` is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR ()-[r:R]-() REQUIRE r.prop IS NOT NULL
      case c: CreateRelationshipPropertyExistenceConstraint if c.constraintVersion == ConstraintVersion2 =>
        throw cypherExceptionFactory.syntaxException("Creating relationship existence constraint using `FOR ... REQUIRE` is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE node.prop IS NODE KEY
      case c: CreateNodeKeyConstraint if c.constraintVersion == ConstraintVersion2 =>
        throw cypherExceptionFactory.syntaxException("Creating node key constraint using `FOR ... REQUIRE` is not supported in this Cypher version.", c.position)

      // CREATE CONSTRAINT [name] [IF NOT EXISTS] FOR (node:Label) REQUIRE node.prop IS UNIQUE
      case c: CreateUniquePropertyConstraint if c.constraintVersion == ConstraintVersion2 =>
        throw cypherExceptionFactory.syntaxException("Creating uniqueness constraint using `FOR ... REQUIRE` is not supported in this Cypher version.", c.position)

      // GRANT IMPERSONATE (name) ON DBMS TO role
      case p@GrantPrivilege(DbmsPrivilege(ImpersonateUserAction), _, _, _) =>
        throw cypherExceptionFactory.syntaxException("IMPERSONATE privilege is not supported in this Cypher version.", p.position)

      // DENY IMPERSONATE (name) ON DBMS TO role
      case p@DenyPrivilege(DbmsPrivilege(ImpersonateUserAction), _, _, _) =>
        throw cypherExceptionFactory.syntaxException("IMPERSONATE privilege is not supported in this Cypher version.", p.position)

      // REVOKE IMPERSONATE (name) ON DBMS TO role
      case p@RevokePrivilege(DbmsPrivilege(ImpersonateUserAction), _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("IMPERSONATE privilege is not supported in this Cypher version.", p.position)

      // ALTER DATABASE
      case a: AlterDatabase =>
        throw cypherExceptionFactory.syntaxException("The ALTER DATABASE command is not supported in this Cypher version.", a.position)

      // GRANT/DENY/REVOKE ALTER DATABASE ...
      case p@GrantPrivilege(DbmsPrivilege(AlterDatabaseAction), _, _, _)    =>
        throw cypherExceptionFactory.syntaxException("ALTER DATABASE privilege is not supported in this Cypher version.", p.position)
      case p@DenyPrivilege(DbmsPrivilege(AlterDatabaseAction), _, _, _) =>
        throw cypherExceptionFactory.syntaxException("ALTER DATABASE privilege is not supported in this Cypher version.", p.position)
      case p@RevokePrivilege(DbmsPrivilege(AlterDatabaseAction), _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("ALTER DATABASE privilege is not supported in this Cypher version.", p.position)

      // GRANT/DENY/REVOKE SET DATABASE ACCESS ...
      case p@GrantPrivilege(DbmsPrivilege(SetDatabaseAccessAction), _, _, _)    =>
        throw cypherExceptionFactory.syntaxException("SET DATABASE ACCESS privilege is not supported in this Cypher version.", p.position)
      case p@DenyPrivilege(DbmsPrivilege(SetDatabaseAccessAction), _, _, _) =>
        throw cypherExceptionFactory.syntaxException("SET DATABASE ACCESS privilege is not supported in this Cypher version.", p.position)
      case p@RevokePrivilege(DbmsPrivilege(SetDatabaseAccessAction), _, _, _, _) =>
        throw cypherExceptionFactory.syntaxException("SET DATABASE ACCESS privilege is not supported in this Cypher version.", p.position)

      // SHOW TRANSACTION[S] [id[, ...]] [WHERE clause | YIELD clause]
      case c: ShowTransactionsClause =>
        throw cypherExceptionFactory.syntaxException("`SHOW TRANSACTIONS` is not supported in this Cypher version.", c.position)

      // TERMINATE TRANSACTION[S] id[, ...]
      case c: TerminateTransactionsClause =>
        throw cypherExceptionFactory.syntaxException("`TERMINATE TRANSACTIONS` is not supported in this Cypher version.", c.position)

    }

    private def hasRangeOptions(options: Options): Boolean = options match {
      case OptionsMap(opt) => opt.exists {
        case (key, value: StringLiteral) if key.equalsIgnoreCase("indexProvider") =>
          // Can't reach the org.neo4j.kernel.impl.index.schema.RangeIndexProvider
          // so have to hardcode the range provider instead
          value.value.equalsIgnoreCase("range-1.0")
        case _ => false
      }
      case _ => false
    }
  }

}

trait Additions extends {
  def check(statement: Statement, cypherExceptionFactory: CypherExceptionFactory): Unit = {}
}
