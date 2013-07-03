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

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.kernel.api.LifecycleOperations;
import org.neo4j.kernel.api.StatementContext;
import org.neo4j.kernel.api.StatementContextParts;

/**
 * Captures functionality for implicit {@link StatementContext} creation and retention for read-only operations
 * through the core {@link GraphDatabaseService} API, where the concept of statements is missing.
 * The idea is to implicitly create a read-only (as light-weight as possible) {@link StatementContext}
 * if there are none open. Tying it into one level above the most granular operations (such as
 * get single property from a node, get next relationship from iterator or similar)
 * @author Mattias
 *
 */
public abstract class StatementContextOwner
{
    private ReferencedStatementContext reference;

    public StatementContextParts getStatementContext()
    {
        if ( reference == null )
        {
            reference = new ReferencedStatementContext( createStatementContext() );
        }
        return reference.newReference();
    }

    protected abstract StatementContextParts createStatementContext();

    public void closeAllStatements()
    {
        if ( reference != null )
        {
            reference.close();
            reference = null;
        }
    }

    private class ReferencedStatementContext
    {
        private int count;
        private final StatementContextParts originalParts;

        ReferencedStatementContext( StatementContextParts statementContextParts )
        {
            this.originalParts = statementContextParts;
        }

        @SuppressWarnings( "resource" )
        public StatementContextParts newReference()
        {
            count++;
            return originalParts.override( null, null, null, null, null, null, null,
                    new CountingLifecycleOperations() );
        }

        void close()
        {
            if ( count > 0 )
            {
                originalParts.close();
                reference = null;
            }
            count = 0;
        }
        
        class CountingLifecycleOperations implements LifecycleOperations
        {
            private boolean open = true;
            
            @Override
            public void close()
            {
                if ( !isOpen() )
                {
                    throw new IllegalStateException(
                            "This StatementContext has been closed. No more interaction allowed" );
                }
                
                open = false;
                count--;
                if ( count == 0 )
                {
                    originalParts.close();
                    reference = null;
                }
            }

            @Override
            public boolean isOpen()
            {
                return open && count > 0;
            }
        }
    }
}
