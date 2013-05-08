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

import org.neo4j.kernel.api.StatementContext;

public abstract class StatementContextOwner
{
    private ReferencedStatementContext reference;

    public StatementContext getStatementContext()
    {
        if ( reference == null )
        {
            reference = new ReferencedStatementContext( createStatementContext() );
        }
        return reference.new StatementContextReference();
    }

    protected abstract StatementContext createStatementContext();

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
        private final StatementContext statementContext;

        ReferencedStatementContext( StatementContext statementContext )
        {
            this.statementContext = statementContext;
        }

        void close()
        {
            if ( count > 0 )
            {
                statementContext.close();
                reference = null;
            }
            count = 0;
        }

        class StatementContextReference extends InteractionStoppingStatementContext
        {
            private boolean open = true;

            StatementContextReference()
            {
                super( statementContext );
                count++;
            }

            @Override
            protected void markAsClosed()
            {
                open = false;
                count--;
                if ( count == 0 )
                {
                    statementContext.close();
                    reference = null;
                }
            }

            @Override
            public boolean isOpen()
            {
                return open && count > 0;
            }

            @Override
            protected void doClose()
            {
                // do nothing - we close when the count reaches 0
            }
        }
    }
}
