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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.kernel.impl.persistence.IdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;
import org.neo4j.kernel.impl.util.ArrayMap;

class RelationshipTypeHolder
{
    private ArrayMap<String,Integer> relTypes = 
        new ArrayMap<String,Integer>( 5, true, true );
    private Map<Integer,RelationshipTypeImpl> relTranslation = 
        new ConcurrentHashMap<Integer,RelationshipTypeImpl>();

    private final TransactionManager transactionManager;
    private final PersistenceManager persistenceManager;
    private final IdGenerator idGenerator;

    RelationshipTypeHolder( TransactionManager transactionManager,
        PersistenceManager persistenceManager, IdGenerator idGenerator )
    {
        this.transactionManager = transactionManager;
        this.persistenceManager = persistenceManager;
        this.idGenerator = idGenerator;
    }

    void addRawRelationshipTypes( RelationshipTypeData[] types )
    {
        for ( int i = 0; i < types.length; i++ )
        {
            RelationshipTypeImpl relType = new RelationshipTypeImpl( types[i].getName() );
            relTypes.put( types[i].getName(), types[i].getId() );
            relTranslation.put( types[i].getId(), relType );
        }
    }
    
    void addRawRelationshipType( RelationshipTypeData type )
    {
        RelationshipTypeImpl relType = new RelationshipTypeImpl( type.getName() );
        relTypes.put( type.getName(), type.getId() );
        relTranslation.put( type.getId(), relType );
    }

    public RelationshipType addValidRelationshipType( String name,
        boolean create )
    {
        Integer id = relTypes.get( name );
        if ( id == null )
        {
            if ( !create )
            {
                return null;
            }
            id = createRelationshipType( name );
            RelationshipTypeImpl type = new RelationshipTypeImpl( name );
            relTranslation.put( id, type );
            return type;
        }
        RelationshipTypeImpl relType = relTranslation.get( id );
        if ( relType == null )
        {
            relType = new RelationshipTypeImpl( name );
            relTranslation.put( id, relType );
        }
        return relType;
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

    // TODO: this should be fixed to run in same thread
    private class RelTypeCreater extends Thread
    {
        private boolean success = false;
        private String name;
        private int id = -1;

        RelTypeCreater( String name )
        {
            super();
            this.name = name;
        }

        synchronized boolean succeded()
        {
            return success;
        }

        synchronized int getRelTypeId()
        {
            return id;
        }

        public synchronized void run()
        {
            try
            {
                transactionManager.begin();
                id = idGenerator.nextId( RelationshipType.class );
                persistenceManager.createRelationshipType( id, name );
                transactionManager.commit();
                success = true;
            }
            catch ( Throwable t )
            {
                t.printStackTrace();
                try
                {
                    transactionManager.rollback();
                }
                catch ( Throwable tt )
                {
                    tt.printStackTrace();
                }
            }
            finally
            {
                this.notify();
            }
        }
    }

    private synchronized int createRelationshipType( String name )
    {
        Integer id = relTypes.get( name );
        if ( id != null )
        {
            return id;
        }
        RelTypeCreater createrThread = new RelTypeCreater( name );
        synchronized ( createrThread )
        {
            createrThread.start();
            while ( createrThread.isAlive() )
            {
                try
                {
                    createrThread.wait( 50 );
                }
                catch ( InterruptedException e )
                { 
                    Thread.interrupted();
                }
            }
        }
        if ( createrThread.succeded() )
        {
            addRelType( name, createrThread.getRelTypeId() );
            return createrThread.getRelTypeId();
        }
        throw new TransactionFailureException( 
            "Unable to create relationship type " + name );
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
        RelationshipTypeImpl relType = relTranslation.remove( id );
        if ( relType != null )
        {
            relTypes.remove( relType.name );
        }
    }

    int getIdFor( RelationshipType type )
    {
        return relTypes.get( type.name() );
    }

    RelationshipType getRelationshipType( int id )
    {
        return relTranslation.get( id );
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
        relTranslation = new ConcurrentHashMap<Integer,RelationshipTypeImpl>();
    }
}