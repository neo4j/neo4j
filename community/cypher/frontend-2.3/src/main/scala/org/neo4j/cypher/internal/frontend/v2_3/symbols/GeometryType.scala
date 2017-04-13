package org.neo4j.cypher.internal.frontend.v2_3.symbols

object GeometryType {
  val instance = new GeometryType() {
    val parentType = CTAny
    override val toString = "Geometry"
  }
}

sealed abstract class GeometryType extends CypherType
