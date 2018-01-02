/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_3.planner

import org.neo4j.cypher.internal.frontend.v2_3.ast._
import org.neo4j.cypher.internal.compiler.v2_3.spi.TokenContext
import org.neo4j.cypher.internal.frontend.v2_3.ast.Query
import org.neo4j.cypher.internal.frontend.v2_3.{SemanticTable, PropertyKeyId, RelTypeId, LabelId}

class SimpleTokenResolver {
  def resolve(ast: Query)(implicit semanticTable: SemanticTable, tokenContext: TokenContext) {
    ast.fold(()) {
      case token: PropertyKeyName =>
        _ => resolvePropertyKeyName(token.name)
      case token: LabelName =>
        _ => resolveLabelName(token.name)
      case token: RelTypeName =>
        _ => resolveRelTypeName(token.name)
    }
  }

  private def resolvePropertyKeyName(name: String)(implicit semanticTable: SemanticTable, tokenContext: TokenContext) {
    tokenContext.getOptPropertyKeyId(name).map(PropertyKeyId) match {
      case Some(id) =>
        semanticTable.resolvedPropertyKeyNames += name -> id
      case None =>
    }
  }

  private def resolveLabelName(name: String)(implicit semanticTable: SemanticTable, tokenContext: TokenContext) {
    tokenContext.getOptLabelId(name).map(LabelId) match {
      case Some(id) =>
        semanticTable.resolvedLabelIds += name -> id
      case None =>
    }
  }

  private def resolveRelTypeName(name: String)(implicit semanticTable: SemanticTable, tokenContext: TokenContext) {
    tokenContext.getOptRelTypeId(name).map(RelTypeId) match {
      case Some(id) =>
        semanticTable.resolvedRelTypeNames += name -> id
      case None =>
    }
  }
}
