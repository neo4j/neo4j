package org.neo4j.kernel.impl.core;

import javax.transaction.TransactionManager;

import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.kernel.impl.persistence.EntityIdGenerator;
import org.neo4j.kernel.impl.persistence.PersistenceManager;

public class DefaultRelationshipTypeCreator implements RelationshipTypeCreator
{
    public static final RelationshipTypeCreator INSTANCE = new DefaultRelationshipTypeCreator();
    
    private DefaultRelationshipTypeCreator()
    {
    }
    
    public int create( TransactionManager txManager, EntityIdGenerator idGenerator,
            PersistenceManager persistence, String name )
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
            return createrThread.getRelTypeId();
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

        public synchronized void run()
        {
            try
            {
                txManager.begin();
                id = idGenerator.nextId( RelationshipType.class );
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
