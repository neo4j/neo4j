/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.recordstorage;

import java.io.IOException;

import org.neo4j.storageengine.api.StorageCommand;

/**
 * Wraps several {@link TransactionApplier}s. In this case, each individual visit-call will delegate to {@link
 * #visit(StorageCommand)} instead, which will call each wrapped {@link TransactionApplier} in turn. In
 * {@link #close()},
 * the appliers are closed in reversed order.
 */
public class TransactionApplierFacade implements TransactionApplier
{
    final TransactionApplier[] appliers;

    TransactionApplierFacade( TransactionApplier... appliers )
    {
        this.appliers = appliers;
    }

    @Override
    public void close() throws Exception
    {
        // Need to close in reverse order or LuceneRecoveryIT can hang on database shutdown, when
        // errors are thrown
        for ( int i = appliers.length - 1; i >= 0; i-- )
        {
            appliers[i].close();
        }
    }

    @Override
    public boolean visit( StorageCommand element ) throws IOException
    {
        for ( TransactionApplier applier : appliers )
        {
            if ( ((Command)element).handle( applier ) )
            {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean visitNodeCommand( Command.NodeCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitRelationshipCommand( Command.RelationshipCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitPropertyCommand( Command.PropertyCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitRelationshipGroupCommand( Command.RelationshipGroupCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitRelationshipTypeTokenCommand( Command.RelationshipTypeTokenCommand command )
            throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitLabelTokenCommand( Command.LabelTokenCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitPropertyKeyTokenCommand( Command.PropertyKeyTokenCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitSchemaRuleCommand( Command.SchemaRuleCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitNodeCountsCommand( Command.NodeCountsCommand command ) throws IOException
    {
        return visit( command );
    }

    @Override
    public boolean visitRelationshipCountsCommand( Command.RelationshipCountsCommand command ) throws IOException
    {
        return visit( command );
    }
}
