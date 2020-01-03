/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.cypher.internal.plandescription

import org.neo4j.cypher.internal.ir.ProvidedOrder
import org.neo4j.cypher.internal.logical.plans.{QualifiedName, SeekableArgs}
import org.neo4j.cypher.internal.v4_0.expressions.SemanticDirection
import org.neo4j.cypher.internal.v4_0.util.symbols.CypherType
import org.neo4j.cypher.internal.v4_0.{expressions => ast}

sealed abstract class Argument extends Product {

  def name: String = productPrefix
}

object Arguments {

  case class Time(value: Long) extends Argument

  case class Rows(value: Long) extends Argument

  case class DbHits(value: Long) extends Argument

  case class Order(order: ProvidedOrder) extends Argument

  case class PageCacheHits(value: Long) extends Argument

  case class PageCacheMisses(value: Long) extends Argument

  case class PageCacheHitRatio(value: Double) extends Argument

  case class Expression(value: ast.Expression) extends Argument

  case class Expressions(expressions: Map[String, ast.Expression]) extends Argument

  case class UpdateActionName(value: String) extends Argument

  case class MergePattern(startPoint: String) extends Argument

  case class Index(label: String, propertyKeys: Seq[String], caches: Seq[ast.Expression]) extends Argument

  case class PrefixIndex(label: String, propertyKey: String, prefix: ast.Expression, caches: Seq[ast.Expression]) extends Argument

  case class InequalityIndex(label: String, propertyKey: String, bounds: Seq[String], caches: Seq[ast.Expression]) extends Argument

  case class PointDistanceIndex(label: String, propertyKey: String, point: String, distance: String, inclusive: Boolean, caches: Seq[ast.Expression]) extends Argument

  case class IndexName(index: String) extends Argument

  case class ConstraintName(constraint: String) extends Argument

  case class LabelName(label: String) extends Argument

  case class KeyNames(keys: Seq[String]) extends Argument

  case class KeyExpressions(expressions: Seq[Expression]) extends Argument

  case class EntityByIdRhs(value: SeekableArgs) extends Argument

  case class EstimatedRows(value: Double) extends Argument

  case class Signature(procedureName: QualifiedName,
                       args: Seq[ast.Expression],
                       results: Seq[(String, CypherType)]) extends Argument

  // This is the version of cypher
  case class Version(value: String) extends Argument {

    override def name = "version"
  }

  case class RuntimeVersion(value: String) extends Argument {

    override def name = "runtime-version"
  }

  case class Planner(value: String) extends Argument {

    override def name = "planner"
  }

  case class PlannerImpl(value: String) extends Argument {

    override def name = "planner-impl"
  }

  case class PlannerVersion(value: String) extends Argument {

    override def name = "planner-version"
  }

  case class Runtime(value: String) extends Argument {

    override def name = "runtime"
  }

  case class RuntimeImpl(value: String) extends Argument {

    override def name = "runtime-impl"
  }

  case class DbmsAction(value: String) extends Argument {

    override def name = "dbms-action"
  }

  case class DatabaseAction(value: String) extends Argument {

    override def name = "database-action"
  }

  case class Database(value: String) extends Argument {

    override def name = "database"
  }

  case class Role(value: String) extends Argument {

    override def name = "role"
  }

  case class User(value: String) extends Argument {

    override def name = "user"
  }

  case class Qualifier(value: String) extends Argument {

    override def name = "qualifier"
  }

  case class Scope(value: String) extends Argument {

    override def name = "scope"
  }

  case class ExpandExpression(from: String, relName: String, relTypes: Seq[String], to: String,
                              direction: SemanticDirection, minLength: Int, maxLength: Option[Int]) extends Argument

  case class CountNodesExpression(ident: String, labels: List[Option[String]]) extends Argument

  case class CountRelationshipsExpression(ident: String, startLabel: Option[String],
                                          typeNames: Seq[String], endLabel: Option[String]) extends Argument

  case class SourceCode(className: String, sourceCode: String) extends Argument {

    override def name: String = "source:" + className
  }

  case class ByteCode(className: String, disassembly: String) extends Argument {

    override def name: String = "bytecode:" + className
  }
}
