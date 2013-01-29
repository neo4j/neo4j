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

import org.neo4j.graphdb.NotFoundException;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.transaction.AbstractTransactionManager;
import org.neo4j.kernel.impl.util.CopyOnWriteHashMap;

public class PropertyIndexManager extends KeyHolder<PropertyIndex>
{
    private static final PropertyIndex[] EMPTY_PROPERTY_INDEXES = new PropertyIndex[0];
    
    private final Map<String, PropertyIndex[]> indexMap = new CopyOnWriteHashMap<String, PropertyIndex[]>();

    public PropertyIndexManager( AbstractTransactionManager txManager, PersistenceManager persistenceManager,
            EntityIdGenerator idGenerator, KeyCreator keyCreator )
    {
        super( txManager, persistenceManager, idGenerator, keyCreator );
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
    public PropertyIndex[] index( String key )
    {
        PropertyIndex[] existing = null;
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
    
    @Override
    public PropertyIndex getKeyByIdOrNull( int id )
    {
        PropertyIndex index = super.getKeyByIdOrNull( id );
        if ( index != null )
            return index;
        
        // Try load it
        String keyName = persistenceManager.loadIndex( id );
        if ( keyName == null )
            throw new NotFoundException( "Index not found [" + id + "]" );
        
        index = new PropertyIndex( keyName, id );
        addKeyEntry( keyName, id );
        return index;
    }
    
    @Override
    protected PropertyIndex newKey( String key, int id )
    {
        return new PropertyIndex( key, id );
    }

    /*
     * Need synchronization here so we don't create multiple lists.
     * 
     * TODO Since legacy databases can have multiple ids for any given property key
     * this legacy method is left behind and used specifically for property indexes
     * until migration has been added to dedup them.
     */
    @Override
    protected void addKeyEntry( String name, int id )
    {
        super.addKeyEntry( name, id );
        PropertyIndex[] list = indexMap.get( name );
        PropertyIndex key = newKey( name, id );
        if ( list == null )
        {
            list = new PropertyIndex[] { key };
        }
        else
        {
            PropertyIndex[] extendedList = new PropertyIndex[list.length+1];
            arraycopy( list, 0, extendedList, 0, list.length );
            extendedList[list.length] = key;
            list = extendedList;
        }
        indexMap.put( name, list );
    }

    @Override
    protected String nameOf( PropertyIndex key )
    {
        return key.getKey();
    }
}
