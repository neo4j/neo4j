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
package org.neo4j.kernel.impl.core;

import java.util.Iterator;

import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.api.properties.Property;
import org.neo4j.kernel.impl.api.store.CacheLoader;
import org.neo4j.kernel.impl.api.store.CacheUpdateListener;

public interface GraphProperties extends PropertyContainer
{

	void commitPropertyMaps( PrimitiveIntObjectMap < DefinedProperty > translateAddedAndChangedProperties,
			Iterator < Integer > removed );

	Iterator < DefinedProperty > getProperties( CacheLoader< Iterator < DefinedProperty > > cacheLoader,
			CacheUpdateListener noUpdates );

	PrimitiveLongIterator getPropertyKeys( CacheLoader < Iterator < DefinedProperty > > cacheLoader,
			CacheUpdateListener noUpdates );

	Property getProperty( CacheLoader < Iterator < DefinedProperty > > cacheLoader,
			CacheUpdateListener noUpdates, int propertyKeyId );
}
