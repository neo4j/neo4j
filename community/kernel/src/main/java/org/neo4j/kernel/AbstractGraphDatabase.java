/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.kernel;

import org.neo4j.graphdb.GraphDatabaseService;

/**
 * Exposes the methods getConfig() and getManagementBean() a.s.o.
 */
public abstract class AbstractGraphDatabase implements GraphDatabaseService
{
    public abstract String getStoreDir();
    
    public abstract Config getConfig();
    
    public abstract <T> T getManagementBean( Class<T> type );
    
    public abstract boolean isReadOnly();
    
    public String toString()
    {
        return getClass().getSimpleName() + " [" + getStoreDir() + "]";
    }
}
