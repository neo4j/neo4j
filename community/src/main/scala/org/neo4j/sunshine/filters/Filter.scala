/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.sunshine.filters

import org.neo4j.graphdb.PropertyContainer

abstract class Filter {
  def isMatch(result: Map[String, Any]):Boolean
}

class AndFilter(a:Filter, b:Filter) extends Filter {
  def isMatch(result: Map[String, Any]): Boolean = a.isMatch(result) && b.isMatch(result)
}

class OrFilter(a:Filter, b:Filter) extends Filter {
  def isMatch(result: Map[String, Any]): Boolean = a.isMatch(result) || b.isMatch(result)
}

class TrueFilter extends Filter {
  def isMatch(result: Map[String, Any]): Boolean = true
}

class EqualsFilter(variable: String, propName: String, value: Any) extends Filter {
  def isMatch(result: Map[String, Any]): Boolean = result(variable).asInstanceOf[PropertyContainer].getProperty(propName) == value
}

class ComparisonFilter(varA:String, propA:String, varB:String, propB:String) extends Filter {
  def isMatch(result: Map[String, Any]): Boolean = {
    val valA = result(varA).asInstanceOf[PropertyContainer].getProperty(propA)
    val valB = result(varB).asInstanceOf[PropertyContainer].getProperty(propB)
    valA == valB
  }
}