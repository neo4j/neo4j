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

import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.kernel.api.LabelNotFoundKernelException;
import org.neo4j.kernel.api.StatementContext;

public class ReadOnlyStatementContext implements StatementContext
{
    private final StatementContext actual;

    public ReadOnlyStatementContext( StatementContext actual )
    {
        this.actual = actual;
    }

    @Override
    public long getOrCreateLabelId( String label )
    {
        throw readOnlyException();
    }

    @Override
    public void addLabelToNode( long labelId, long nodeId )
    {
        throw readOnlyException();
    }

    @Override
    public long getLabelId( String label ) throws LabelNotFoundKernelException
    {
        return actual.getLabelId( label );
    }

    @Override
    public boolean isLabelSetOnNode( long labelId, long nodeId )
    {
        return actual.isLabelSetOnNode( labelId, nodeId );
    }
    
    @Override
    public Iterable<Long> getLabelsForNode( long nodeId )
    {
        return actual.getLabelsForNode( nodeId );
    }
    
    @Override
    public String getLabelName( long labelId ) throws LabelNotFoundKernelException
    {
        return actual.getLabelName( labelId );
    }

    @Override
    public void removeLabelFromNode( long labelId, long nodeId )
    {
        throw readOnlyException();
    }

    @Override
    public void close( boolean successful )
    {
        actual.close( successful );
    }

    private NotInTransactionException readOnlyException()
    {
        return new NotInTransactionException( "You have to be in a transaction context to perform write operations." );
    }
}
