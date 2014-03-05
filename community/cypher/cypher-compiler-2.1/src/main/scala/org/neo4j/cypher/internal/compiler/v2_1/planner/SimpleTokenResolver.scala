/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.cypher.internal.compiler.v2_1.planner

import org.neo4j.cypher.internal.compiler.v2_1.ast._
import org.neo4j.cypher.internal.compiler.v2_1.spi.TokenContext
import org.neo4j.cypher.internal.compiler.v2_1._
import org.neo4j.cypher.internal.compiler.v2_1.LabelId
import org.neo4j.cypher.internal.compiler.v2_1.PropertyKeyId
import org.neo4j.cypher.internal.compiler.v2_1.bottomUp
import org.neo4j.cypher.internal.compiler.v2_1.ast.Query
import org.neo4j.cypher.internal.compiler.v2_1.RelTypeId

class SimpleTokenResolver {
  def resolve(ast: Query)(implicit tokenContext: TokenContext): Query = ast.rewrite(bottomUp( Rewriter.lift {
    case token @ PropertyKeyName(name, None) => PropertyKeyName(name, propertyKeyId(name))(token.position)
    case token @ LabelName(name, None)       => LabelName(name, labelId(name))(token.position)
    case token @ RelTypeName(name, None)     => RelTypeName(name, relTypeId(name))(token.position)
  })).asInstanceOf[Query]

  def propertyKeyId(name: String)(implicit tokenContext: TokenContext): Option[PropertyKeyId] = tokenContext.getOptPropertyKeyId(name).map(PropertyKeyId)
  def labelId(name: String)(implicit tokenContext: TokenContext): Option[LabelId] = tokenContext.getOptLabelId(name).map(LabelId)
  def relTypeId(name: String)(implicit tokenContext: TokenContext): Option[RelTypeId] = tokenContext.getOptRelTypeId(name).map(RelTypeId)
}
