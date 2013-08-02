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

import java.io.IOException;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.StatementOperations;
import org.neo4j.kernel.api.operations.StatementState;
import org.neo4j.kernel.impl.api.state.TxState;

/**
 * Note: This is in a state of flux, as we are removing support for read operations without having a
 * transaction around it. This should be refactored away.
 *
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

    public StatementState getStatementState()
    {
        if ( reference == null )
        {
            reference = new ReferencedStatementState( createStatementState() );
        }
        return reference.newReference();
    }
    
    protected abstract StatementState createStatementState();

    public void closeAllStatements() throws IOException
    {
        if ( reference != null )
        {
            reference.forceClose();
            reference = null;
        }
    }
    
    private class ReferencedStatementState implements StatementState
    {
        private final StatementState actual;
        private int refCount;

        ReferencedStatementState( StatementState actual )
        {
            this.actual = actual;
        }

        public void forceClose() throws IOException
        {
            if ( refCount > 0 )
            {
                refCount = 0;
                actual.close();
                reference = null;
            }
        }

        public StatementState newReference()
        {
            refCount++;
            return this;
        }

        @Override
        public void close()
        {
            if ( --refCount == 0 )
            {
                actual.close();
                reference = null;
            }
        }

        @Override
        public LockHolder locks()
        {
            return actual.locks();
        }

        @Override
        public TxState txState()
        {
            return actual.txState();
        }

        @Override
        public boolean hasTxState()
        {
            return actual.hasTxState();
        }

        @Override
        public boolean hasTxStateWithChanges()
        {
            return actual.hasTxStateWithChanges();
        }

        @Override
        public IndexReaderFactory indexReaderFactory()
        {
            return actual.indexReaderFactory();
        }
    }
}
