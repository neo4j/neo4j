/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.impl.nioneo.store.RelationshipTypeData;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

public class DefaultRelationshipTypeCreator implements RelationshipTypeCreator
{
    public int getOrCreate( TransactionManager txManager, EntityIdGenerator idGenerator,
            PersistenceManager persistence, RelationshipTypeHolder relTypeHolder, String name )
    {
        RelTypeCreater createrThread = new RelTypeCreater( name, txManager, idGenerator,
                persistence );
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
            int id = createrThread.getRelTypeId();
            relTypeHolder.addRawRelationshipType( new RelationshipTypeData( id, name ) );
            return id;
        }
        throw new TransactionFailureException(
                "Unable to create relationship type " + name );
    }

    // TODO: this should be fixed to run in same thread
    private static class RelTypeCreater extends Thread
    {
        private boolean success = false;
        private String name;
        private int id = -1;
        private final TransactionManager txManager;
        private final PersistenceManager persistence;
        private final EntityIdGenerator idGenerator;

        RelTypeCreater( String name, TransactionManager txManager, EntityIdGenerator idGenerator,
                PersistenceManager persistence )
        {
            super();
            this.name = name;
            this.txManager = txManager;
            this.idGenerator = idGenerator;
            this.persistence = persistence;
        }

        synchronized boolean succeded()
        {
            return success;
        }

        synchronized int getRelTypeId()
        {
            return id;
        }

        @Override
        public synchronized void run()
        {
            try
            {
                txManager.begin();
                id = (int) idGenerator.nextId( RelationshipType.class );
                persistence.createRelationshipType( id, name );
                txManager.commit();
                success = true;
            }
            catch ( Throwable t )
            {
                t.printStackTrace();
                try
                {
                    txManager.rollback();
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
}
