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

import java.io.IOException;

interface PropertyContainerStyle
{
	/**
	 * Emit the end of a node or relationship.
	 * @param stream
	 *            the stream to emit the end on.
	 * @throws IOException
	 *             if there is an error on the stream.
	 */
	void emitEnd( Appendable stream ) throws IOException;

	/**
	 * Emit a property of a node of relationship.
	 * @param stream
	 *            the stream to emit the property on.
	 * @param key
	 *            the key of the property.
	 * @param value
	 *            the value of the property.
	 * @throws IOException
	 *             if there is an error on the stream.
	 */
	void emitProperty( Appendable stream, String key, Object value )
	    throws IOException;
}
