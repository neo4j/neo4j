/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.impl.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.util.ArrayMap;

public class RelationshipTypeHolder
{
    private ArrayMap<String,Integer> relTypes = 
        new ArrayMap<String,Integer>( 5, true, true );
    private Map<Integer,String> relTranslation = 
        new ConcurrentHashMap<Integer,String>();

    private final TransactionManager transactionManager;
    private final PersistenceManager persistenceManager;
    private final EntityIdGenerator idGenerator;
    private final RelationshipTypeCreator relTypeCreator;

    RelationshipTypeHolder( TransactionManager transactionManager,
        PersistenceManager persistenceManager, EntityIdGenerator idGenerator,
        RelationshipTypeCreator relTypeCreator )
    {
        this.transactionManager = transactionManager;
        this.persistenceManager = persistenceManager;
        this.idGenerator = idGenerator;
        this.relTypeCreator = relTypeCreator;
    }

    void addRawRelationshipTypes( RelationshipTypeData[] types )
    {
        for ( int i = 0; i < types.length; i++ )
        {
            addRawRelationshipType( types[i] );
        }
    }
    
    void addRawRelationshipType( RelationshipTypeData type )
    {
        relTypes.put( type.getName(), type.getId() );
        relTranslation.put( type.getId(), type.getName() );
        printIt( type.getId(), type.getName() );
    }

    private void printIt( int id, String name )
    {
        new Exception( "relTranslation.put( " + id + ", " + name + " )" ).printStackTrace();
    }

    public RelationshipType addValidRelationshipType( String name,
        boolean create )
    {
        if ( relTypes.get( name ) == null )
        {
            if ( !create )
            {
                return null;
            }
            int id = createRelationshipType( name );
            relTranslation.put( id, name );
            printIt( id, name );
        }
        else
        {
            relTranslation.put( relTypes.get( name ), name );
            printIt( relTypes.get( name ), name );
        }
        return new RelationshipTypeImpl( name );
    }

    boolean isValidRelationshipType( RelationshipType type )
    {
        return relTypes.get( type.name() ) != null;
    }

    private static class RelationshipTypeImpl implements RelationshipType
    {
        private String name;

        RelationshipTypeImpl( String name )
        {
            assert name != null;
            this.name = name;
        }

        public String name()
        {
            return name;
        }

        public String toString()
        {
            return name;
        }

        public boolean equals( Object o )
        {
            if ( !(o instanceof RelationshipType) )
            {
                return false;
            }
            return name.equals( ((RelationshipType) o).name() );
        }

        public int hashCode()
        {
            return name.hashCode();
        }
    }

    private synchronized int createRelationshipType( String name )
    {
        Integer id = relTypes.get( name );
        if ( id != null )
        {
            return id;
        }
        id = relTypeCreator.getOrCreate( transactionManager, idGenerator,
                persistenceManager, this, name );
        addRelType( name, id );
        return id;
    }

    void addRelType( String name, Integer id )
    {
        relTypes.put( name, id );
    }

    void removeRelType( String name )
    {
        relTypes.remove( name );
    }

    void removeRelType( int id )
    {
        String name = relTranslation.remove( id );
        if ( name != null )
        {
            relTypes.remove( name );
        }
    }

    int getIdFor( RelationshipType type )
    {
        return getIdFor( type.name() );
    }
    
    public Integer getIdFor( String name )
    {
        return relTypes.get( name );
    }

    RelationshipType getRelationshipType( int id )
    {
        String name = relTranslation.get( id );
        if ( name != null )
        {
            return new RelationshipTypeImpl( name );
        }
        return null;
    }

    public Iterable<RelationshipType> getRelationshipTypes()
    {
        List<RelationshipType> relTypeList = new ArrayList<RelationshipType>();
        for ( String name : relTypes.keySet() )
        {
            relTypeList.add( new RelationshipTypeImpl( name ) );
        }
        return relTypeList;
    }

    void clear()
    {
        relTypes = new ArrayMap<String,Integer>();
        relTranslation = new HashMap<Integer,String>();
    }
}