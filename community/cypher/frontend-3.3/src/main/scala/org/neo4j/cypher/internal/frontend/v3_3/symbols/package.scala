/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.cypher.internal.frontend.v3_3

package object symbols {
  val CTAny: AnyType = AnyType.instance
  val CTBoolean: BooleanType = BooleanType.instance
  val CTString: StringType = StringType.instance
  val CTNumber: NumberType = NumberType.instance
  val CTFloat: FloatType = FloatType.instance
  val CTInteger: IntegerType = IntegerType.instance
  val CTMap: MapType = MapType.instance
  val CTNode: NodeType = NodeType.instance
  val CTRelationship: RelationshipType = RelationshipType.instance
  val CTPoint: PointType = PointType.instance
  val CTGeometry: GeometryType = GeometryType.instance
  val CTPath: PathType = PathType.instance
  def CTList(inner: CypherType): ListType = ListType(inner)

  implicit def invariantTypeSpec(that: CypherType): TypeSpec = that.invariant
}
