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

import org.junit.Test;

import org.neo4j.kernel.api.StatementContext;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class InteractionStoppingStatementContextTest
{
    @Test( expected = IllegalStateException.class )
    public void shouldDisallowInteractionAfterBeingClosed() throws Exception
    {
        // GIVEN
        StatementContext statement = new SimpleInteractionStoppingStatementContext( mock( StatementContext.class ) );
        statement.close();

        // WHEN
        statement.getLabelId( "my label" );
    }
    
    @Test
    public void shouldAllowInteractionIfNotClosed() throws Exception
    {
        // GIVEN
        int labelId = 10;
        int nodeId = 20;
        StatementContext actual = mock( StatementContext.class );
        StatementContext statement = new SimpleInteractionStoppingStatementContext( actual );

        // WHEN
        statement.addLabelToNode( labelId, nodeId );

        // THEN
        verify( actual ).addLabelToNode( labelId, nodeId );
    }

    private static class SimpleInteractionStoppingStatementContext extends InteractionStoppingStatementContext
    {
        private boolean open = true;

        SimpleInteractionStoppingStatementContext( StatementContext delegate )
        {
            super( delegate );
        }

        @Override
        protected void markAsClosed()
        {
            open = false;
        }

        @Override
        public boolean isOpen()
        {
            return open;
        }
    }
}
