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
package org.neo4j.kernel.impl.core;

import static java.lang.System.arraycopy;

import java.util.Map;

import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;

public class PropertyKeyTokenHolder extends TokenHolder<PropertyKeyToken>
{
    private static final PropertyKeyToken[] EMPTY_PROPERTY_INDEXES = new PropertyKeyToken[0];
    
    private final Map<String, PropertyKeyToken[]> indexMap = new CopyOnWriteHashMap<String, PropertyKeyToken[]>();

    public PropertyKeyTokenHolder( AbstractTransactionManager txManager, PersistenceManager persistenceManager,
                                   EntityIdGenerator idGenerator, TokenCreator tokenCreator )
    {
        super( txManager, persistenceManager, idGenerator, tokenCreator );
    }

    @Override
    public void stop()
    {
        super.stop();
        indexMap.clear();
    }

    /*
     * TODO Since legacy databases can have multiple ids for any given property key
     * this legacy method is left behind and used specifically for property indexes
     * until migration has been added to dedup them.
     */
    public PropertyKeyToken[] index( String key )
    {
        PropertyKeyToken[] existing = null;
        if ( key != null )
        {
            existing = indexMap.get( key );
        }
        if ( existing == null )
        {
            existing = EMPTY_PROPERTY_INDEXES;
        }
        return existing;
    }

    /*
     * TODO Since legacy databases can have multiple ids for any given property key
     * this legacy method is left behind and used specifically for property indexes
     * until migration has been added to dedup them.
     */
    @Override
    protected void notifyMeOfTokensAdded( String name, int id )
    {
        PropertyKeyToken[] list = indexMap.get( name );
        PropertyKeyToken key = newToken( name, id );
        if ( list == null )
        {
            list = new PropertyKeyToken[] { key };
        }
        else
        {
            PropertyKeyToken[] extendedList = new PropertyKeyToken[list.length+1];
            arraycopy( list, 0, extendedList, 0, list.length );
            extendedList[list.length] = key;
            list = extendedList;
        }
        indexMap.put( name, list );
    }

    @Override
    protected PropertyKeyToken newToken( String name, int id )
    {
        return new PropertyKeyToken( name, id );
    }
    
    @Override
    protected String nameOf( PropertyKeyToken token )
    {
        return token.getKey();
    }
}
