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
package org.neo4j.cypher.internal.compiler.v3_0.planner.logical.eagerness

import org.neo4j.cypher.internal.compiler.v3_0.planner.QueryGraph
import org.neo4j.cypher.internal.compiler.v3_0.planner.logical.plans.IdName
import org.neo4j.cypher.internal.frontend.v3_0.ast.{LabelName, PropertyKeyName, RelTypeName}

case class ReadView(qg: QueryGraph) extends Read {
  override def readsNodes = qg.patternNodes.nonEmpty
  override def readsRelationships = qg.patternRelationships.nonEmpty

  override def nodeIds = qg.patternNodes
  override def labelsOn(x: IdName): Set[LabelName] = qg.selections
    .labelPredicates.getOrElse(x, Set.empty)
    .flatMap(_.labels)

  override def relationships = qg.patternRelationships

  override def typesOn(x: IdName): Set[RelTypeName] = qg.patternRelationships.collect {
    case rel if rel.name == x => rel.types
  }.flatten

  override def readsProperties: Set[PropertyKeyName] =
    qg.allKnownNodeProperties.map(_.propertyKey)

  override def propertiesOn(x: IdName): Set[PropertyKeyName] =
    qg.knownProperties(x).map(_.propertyKey)

  override def graphEntities: Set[IdName] = qg.patternRelationships.flatMap(p => Seq(p.left, p.right, p.name))
}
