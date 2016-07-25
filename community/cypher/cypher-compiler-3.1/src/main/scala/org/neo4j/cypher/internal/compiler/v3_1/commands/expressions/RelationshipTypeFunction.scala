/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v3_1.commands.expressions

import org.neo4j.cypher.internal.compiler.v3_1._
import org.neo4j.cypher.internal.compiler.v3_1.executionplan.{Effects, ReadsAllRelationships}
import org.neo4j.cypher.internal.compiler.v3_1.pipes.QueryState
import org.neo4j.cypher.internal.compiler.v3_1.symbols.SymbolTable
import org.neo4j.cypher.internal.frontend.v3_1.{EntityNotFoundException, ParameterWrongTypeException}
import org.neo4j.cypher.internal.frontend.v3_1.symbols._
import org.neo4j.graphdb.Relationship

case class RelationshipTypeFunction(relationship: Expression) extends NullInNullOutExpression(relationship) {

  override def compute(value: Any, m: ExecutionContext)(implicit state: QueryState): String = value match {
    case r: Relationship =>
      if (state.query.relationshipOps.isDeletedInThisTx(r)) {
        throw new EntityNotFoundException(s"Relationship with id ${r.getId} has been deleted in this transaction")
      } else {
        r.getType.name()
      }
    case x => throw new ParameterWrongTypeException("Expected a Relationship, got: " + x)
  }

  override def rewrite(f: (Expression) => Expression) = f(RelationshipTypeFunction(relationship.rewrite(f)))

  override def arguments = Seq(relationship)

  override def calculateType(symbols: SymbolTable) = {
    relationship.evaluateType(CTRelationship, symbols)
    CTString
  }

  override def symbolTableDependencies = relationship.symbolTableDependencies

  override def localEffects(symbols: SymbolTable) = Effects(ReadsAllRelationships)
}
