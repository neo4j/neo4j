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

import org.neo4j.cypher.internal.compiler.v2_3.planner.logical.{Cardinality, Selectivity}
import org.neo4j.cypher.internal.compiler.v2_3.spi.GraphStatistics
import org.neo4j.cypher.internal.frontend.v2_3.{LabelId, NameId, PropertyKeyId, RelTypeId}
import org.neo4j.kernel.impl.util.dbstructure.DbStructureLookup

class DbStructureGraphStatistics(lookup: DbStructureLookup) extends GraphStatistics {

  import NameId._

  override def nodesWithLabelCardinality( label: Option[LabelId] ): Cardinality =
    Cardinality(lookup.nodesWithLabelCardinality(label))

  override def cardinalityByLabelsAndRelationshipType( fromLabel: Option[LabelId], relTypeId: Option[RelTypeId], toLabel: Option[LabelId] ): Cardinality =
    Cardinality(lookup.cardinalityByLabelsAndRelationshipType(fromLabel, relTypeId, toLabel))

  /*
      Probability of any node with the given label, to have a given property with a particular value

      indexSelectivity(:X, prop) = s => |MATCH (a:X)| * s = |MATCH (a:X) WHERE x.prop = '*'|
   */
  override def indexSelectivity( label: LabelId, property: PropertyKeyId ): Option[Selectivity] = {
    val result = lookup.indexSelectivity( label.id, property.id )
    Selectivity.of(result)
  }

  /*
      Probability of any node with the given label, to have a particular property

      indexPropertyExistsSelectivity(:X, prop) = s => |MATCH (a:X)| * s = |MATCH (a:X) WHERE has(x.prop)|
   */
  override def indexPropertyExistsSelectivity( label: LabelId, property: PropertyKeyId ): Option[Selectivity] = {
    val result = lookup.indexPropertyExistsSelectivity( label.id, property.id )
    if (result.isNaN) None else Some(Selectivity.of(result).get)
  }
}
