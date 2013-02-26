/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import org.neo4j.helpers.Service;

/**
 * Provides implementations for some given indexing scheme.
 */
public abstract class SchemaIndexProvider extends Service
{
    /**
     * Create a new instance of a service implementation identified with the
     * specified key(s).
     *
     * @param key     the main key for identifying this service implementation
     */
    protected SchemaIndexProvider( String key )
    {
        super( key );
    }

    abstract IndexWriter getPopulator( long indexId );
    
    abstract IndexWriter getWriter( long indexId );
    
    // Design idea: we add methods here like:
    //    getReader( IndexDefinition index )
    //    populationCompleted()
    //    getState() -> POPULATING, ONLINE
}
