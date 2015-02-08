/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.commands.expressions

import org.neo4j.cypher.internal.compiler.v2_1.ExecutionContext
import org.neo4j.cypher.internal.compiler.v2_1.symbols._
import org.neo4j.cypher.internal.compiler.v2_1.symbols.SymbolTable
import org.neo4j.cypher.internal.compiler.v2_1.pipes.QueryState
import org.neo4j.graphdb.{Relationship, Node}

object ProjectedPath {

  type Projector = (ExecutionContext, PathValueBuilder) => PathValueBuilder

  object nilProjector extends Projector {
    def apply(ctx: ExecutionContext, builder: PathValueBuilder) = builder
  }

  case class singleNodeProjector(node: String, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, builder: PathValueBuilder) =
      tailProjector(ctx, builder.addNode(ctx(node).asInstanceOf[Node]))
  }

  case class singleIncomingRelationshipProjector(rel: String, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, builder: PathValueBuilder) =
      tailProjector(ctx, builder.addIncomingRelationship(ctx(rel).asInstanceOf[Relationship]))
  }

  case class singleOutgoingRelationshipProjector(rel: String, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, builder: PathValueBuilder) =
      tailProjector(ctx, builder.addOutgoingRelationship(ctx(rel).asInstanceOf[Relationship]))
  }

  case class multiIncomingRelationshipProjector(rel: String, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, builder: PathValueBuilder) =
      tailProjector(ctx, builder.addIncomingRelationships(ctx(rel).asInstanceOf[Iterable[Relationship]]))
  }

  case class multiOutgoingRelationshipProjector(rel: String, tailProjector: Projector) extends Projector {
    def apply(ctx: ExecutionContext, builder: PathValueBuilder) =
      tailProjector(ctx, builder.addOutgoingRelationships(ctx(rel).asInstanceOf[Iterable[Relationship]]))
  }
}

/*
 Expressions for materializing new paths (used by ronja)

 These expressions cannot be generated by the user directly
 */
case class ProjectedPath(symbolTableDependencies: Set[String], projector: ProjectedPath.Projector) extends Expression {
  private val builder = new PathValueBuilder

  def apply(ctx: ExecutionContext)(implicit state: QueryState) = {
    builder.clear()
    projector(ctx, builder).result()
  }

  def arguments = Seq.empty

  def rewrite(f: (Expression) => Expression): Expression = f(this)

  def calculateType(symbols: SymbolTable): CypherType = CTPath
}


