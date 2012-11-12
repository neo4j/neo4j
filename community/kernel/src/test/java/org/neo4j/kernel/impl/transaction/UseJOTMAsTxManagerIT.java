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
package org.neo4j.kernel.impl.transaction;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.transaction.HeuristicMixedException;
import javax.transaction.HeuristicRollbackException;
import javax.transaction.NotSupportedException;
import javax.transaction.RollbackException;
import javax.transaction.SystemException;
import javax.transaction.UserTransaction;
import javax.transaction.xa.Xid;

import org.junit.Test;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventHandler;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.ImpermanentGraphDatabase;
import org.objectweb.jotm.Jotm;

public class UseJOTMAsTxManagerIT
{

    private TransactionEventHandler<Object> failsBeforeCommitTransactionHandler = new TransactionEventHandler<Object>(){

                @Override
                public Object beforeCommit( TransactionData data ) throws Exception
                {
                    throw new RuntimeException("LURING!");
                }

                @Override
                public void afterCommit( TransactionData data, Object state )
                {
                    // TODO Auto-generated method stub
                    
                }

                @Override
                public void afterRollback( TransactionData data, Object state )
                {
                    // TODO Auto-generated method stub
                    
                }
                
            };

    // TODO: This is meant to be a documented test case.
    /**
     * To use JOTM, you need to add both the jotm-core as a dependency, but also
     * the j2ee spec:
     * 
     * <dependency> <groupId>geronimo-spec</groupId>
     * <artifactId>geronimo-spec-j2ee</artifactId> <version>1.4-rc4</version>
     * </dependency>
     * 
     * Then create a transaction manager service that Neo4j can consume, see
     * {@link JOTMTransactionManager}, and a provider of that service that Neo4j
     * can use to create the service provider,
     * {@link JOTMTransactionManager$Provider}.
     * 
     * The provider should be listed in META-INF/services/
     * 
     * With that in place, tell neo4j to use JOTM by passing in the appropriate
     * config setting, see below.
     */
    @Test
    public void shouldStartWithJOTM()
    {
        Map<String, String> config = new HashMap<String, String>();
        config.put( GraphDatabaseSettings.tx_manager_impl.name(), JOTMTransactionManager.NAME );
        GraphDatabaseAPI db = null;

        try
        {
            db = new ImpermanentGraphDatabase( config );
            assertThat( db.getTxManager(), is( JOTMTransactionManager.class ) );
            
            Transaction tx = db.beginTx();
            Node node = null;
            try {
                node = db.createNode();
                tx.success();
            } finally {
                tx.finish();
            }
            
            assertThat( db.getNodeById( node.getId() ), is( node ) );
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
    }

    @Test
    public void shouldHandleFailingCommitWithExternalDataSourceGracefully() throws IllegalStateException, RollbackException, SystemException
    {
        Map<String, String> config = new HashMap<String, String>();
        config.put( GraphDatabaseSettings.tx_manager_impl.name(), JOTMTransactionManager.NAME );
        GraphDatabaseAPI db = null;

        final AtomicBoolean externalResourceWasRolledBack = new AtomicBoolean(false);
        
        try
        {
            db = new ImpermanentGraphDatabase( config );
            
            // Fail onBeforeCommit
            db.registerTransactionEventHandler( failsBeforeCommitTransactionHandler );
            
            Transaction outerTx = db.beginTx();
            
            // Add an external data source
            FakeXAResource externalResource = new FakeXAResource("BananaStorageFacility"){
                @Override
                public void rollback( Xid xid )
                {
                    super.rollback(xid);
                    externalResourceWasRolledBack.set( true );
                }
            };
            db.getTxManager().getTransaction().enlistResource( externalResource );
            
            try
            {
                db.createNode();
                Transaction innerTx = db.beginTx();
                try
                {
                    db.createNode();
                    innerTx.success();
                }
                finally
                {
                    innerTx.finish();
                }
                
                outerTx.success();

            }
            finally
            {
                outerTx.finish();
            }
            
            fail("Transaction should have failed.");

        } catch(TransactionFailureException e) {
            // ok!
        }
        finally
        {
            if ( db != null )
            {
                db.shutdown();
            }
        }
        
        // Phew..
        assertThat( externalResourceWasRolledBack.get(), is( true ) );
    }
    
    @Test
    public void shouldSupportRollbacks() throws IllegalStateException, RollbackException, SystemException, NotSupportedException, SecurityException, HeuristicMixedException, HeuristicRollbackException
    {
        Map<String, String> config = new HashMap<String, String>();
        config.put( GraphDatabaseSettings.tx_manager_impl.name(), JOTMTransactionManager.NAME );
        GraphDatabaseAPI neo4j = null;

        try
        {
            neo4j = new ImpermanentGraphDatabase( config );
            
            Jotm jotm = ((JOTMTransactionManager)neo4j.getTxManager()).getJotmTxManager();
            
            UserTransaction userTx = jotm.getUserTransaction();
            userTx.begin();
            
            neo4j.createNode();
            
            userTx.rollback();
        }
        finally
        {
            if ( neo4j != null )
            {
                neo4j.shutdown();
            }
        }
    }

}
