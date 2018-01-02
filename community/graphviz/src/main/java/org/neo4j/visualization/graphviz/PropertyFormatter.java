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
package org.neo4j.visualization.graphviz;

import org.neo4j.visualization.PropertyType;

interface PropertyFormatter
{
	/**
	 * Format a property as a string.
	 * @param key
	 *            the key of the property to format.
	 * @param type
	 *            an object representing the type of the property.
	 * @param value
	 *            the value or the property to format.
	 * @return the property formatted as a string.
	 */
	String format( String key, PropertyType type, Object value );
}
