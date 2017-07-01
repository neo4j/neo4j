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
package org.neo4j.cypher.internal.compatibility.v3_3.runtime.helpers

import org.neo4j.cypher.internal.frontend.v3_3.symbols
import org.neo4j.cypher.internal.frontend.v3_3.symbols.CypherType
import org.neo4j.graphdb.{Node, Path, Relationship}
import org.neo4j.values.storable.Values
import org.neo4j.values.{AnyValue, AnyValues}

object ValueConversion {
  def getValueConverter(cType: CypherType): Any => AnyValue = cType match {
    case symbols.CTNode => n => AnyValues.asNodeValue(n.asInstanceOf[Node])
    case symbols.CTRelationship => r => AnyValues.asEdgeValue(r.asInstanceOf[Relationship])
    case symbols.CTBoolean => b => Values.booleanValue(b.asInstanceOf[Boolean])
    case symbols.CTFloat => d => Values.doubleValue(d.asInstanceOf[Double])
    case symbols.CTInteger => l => Values.longValue(l.asInstanceOf[Long])
    case symbols.CTPath => p => AnyValues.asPathValue(p.asInstanceOf[Path])
    case symbols.CTMap => m => AnyValues.asMapValue(m.asInstanceOf[java.util.Map[String, AnyRef]])
    case symbols.ListType(_)  => l => AnyValues.asListValue(l.asInstanceOf[java.util.Collection[_]])
    case symbols.CTAny => o => AnyValues.of(o)
  }
}
