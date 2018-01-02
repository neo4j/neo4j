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
package org.neo4j.visualization;

/**
 * The {@link PropertyRenderer} is responsible for rendering the properties of a
 * node or relationship.
 * @param <E>
 *            A base exception type that can be thrown by the methods of this
 *            renderer.
 */
public interface PropertyRenderer<E extends Throwable>
{
	/**
	 * Renders a property.
	 * @param propertyKey
	 *            the key of the property.
	 * @param propertyValue
	 *            the value of the property.
	 * @throws E
	 *             if an error occurs when rendering the property.
	 */
	void renderProperty( String propertyKey, Object propertyValue ) throws E;

	/**
	 * Invoked when all properties have been rendered.
	 * @throws E
	 *             if an error occurs.
	 */
	void done() throws E;
}
