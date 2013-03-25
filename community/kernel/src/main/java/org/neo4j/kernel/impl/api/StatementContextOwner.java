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
    private StatementContext statementContext;
    public int count;

    public StatementContext getStatementContext()
    {
        if ( statementContext == null )
        {
            statementContext = createStatementContext();
        }
        count++;
        return new StatementContextReference( statementContext );
    }

    protected abstract StatementContext createStatementContext();

    private class StatementContextReference extends InteractionStoppingStatementContext
    {
        public StatementContextReference( StatementContext delegate )
        {
            super( delegate );
        }

        @Override
        public void close()
        {
            markAsClosed();
            if ( --count == 0 )
            {
                statementContext.close();
                statementContext = null;
            }
        }
    }

    public void assertAllClosed()
    {
        if ( count > 0 )
        {
            throw new IllegalStateException( "Tried to close transaction context but had statement context's open" );
        }
    }
}
