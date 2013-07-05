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
package org.neo4j.kernel.impl.api;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.LifecycleOperations;
import org.neo4j.kernel.api.StatementOperations;
import org.neo4j.kernel.api.operations.RefCounting;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.impl.api.state.TxState;

/**
 * Captures functionality for implicit {@link StatementOperations} creation and retention for read-only operations
 * through the core {@link GraphDatabaseService} API, where the concept of statements is missing.
 * The idea is to implicitly create a read-only (as light-weight as possible) {@link StatementOperations}
 * if there are none open. Tying it into one level above the most granular operations (such as
 * get single property from a node, get next relationship from iterator or similar)
 * 
 * given scenario:
 *
 *  relationships = node.getRelationships();
 *  for ( Relationship rel : relationships )
 *  {
 *      rel.getProperty( "name" );
 *  }
 *
 *
 *                      KernelTransaction
 *                           |
 *                           v
 *                      ReferencedStatementState (actual StatementState + ref counter)
 *                      /            \
 *                     /              \------------
 *                    v                            \
 *   StatementState (+boolean open)   StatementState (+boolean open)
 *           ^                                     
 *           |                                    
 *           |                                    
 *      action: close()                      
 */
public abstract class StatementStateOwner
{
    private ReferencedStatementState reference;
    private final LifecycleOperations operations;
    
    StatementStateOwner( LifecycleOperations operations )
    {
        this.operations = operations;
    }

    public StatementState getStatementState()
    {
        if ( reference == null )
        {
            reference = new ReferencedStatementState( createStatementState() );
        }
        return reference.newReference();
    }
    
    protected abstract StatementState createStatementState();

    public void closeAllStatements()
    {
        if ( reference != null )
        {
            reference.close();
            reference = null;
        }
    }
    
    private class ReferencedStatementState
    {
        private int count;
        private final StatementState parentState;

        ReferencedStatementState( StatementState statement )
        {
            this.parentState = statement;
        }

        public StatementState newReference()
        {
            count++;
            final AtomicReference<RefCounting> refCounting = new AtomicReference<>();
            final StatementState referencedState = new StatementState()
            {
                @Override
                public TxState txState()
                {
                    return parentState.txState();
                }
                
                @Override
                public RefCounting refCounting()
                {
                    return refCounting.get();
                }
                
                @Override
                public LockHolder locks()
                {
                    return parentState.locks();
                }
                
                @Override
                public IndexReaderFactory indexReaderFactory()
                {
                    return parentState.indexReaderFactory();
                }
                
                @Override
                public Closeable closeable( StatementOperations logic )
                {
                    return parentState.closeable( logic );
                }
            };
            refCounting.set( new RefCounting()
            {
                private boolean open = true;
                
                @Override
                public boolean isOpen()
                {
                    return open && count > 0;
                }
                
                @Override
                public void close()
                {
                    if ( !isOpen() )
                    {
                        throw new IllegalStateException( "This " + StatementState.class.getSimpleName() +
                                " has been closed. No more interaction allowed" );
                    }

                    open = false;
                    if ( --count == 0 )
                    {
                        operations.close( parentState );
                        reference = null;
                    }
                }
            } );
            return referencedState;
        }

        void close()
        {
            if ( count > 0 )
            {
                operations.close( parentState );
                reference = null;
            }
            count = 0;
        }
    }
}
