/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.collection.primitive.PrimitiveLongVisitor;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.TransactionApplicationMode;
import org.neo4j.kernel.impl.store.NeoStore;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.command.Command;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.command.NeoCommandHandler;
import org.neo4j.kernel.impl.transaction.state.LazyIndexUpdates;
import org.neo4j.kernel.impl.transaction.state.PropertyLoader;

/**
 * Performs validation of index updates for transactions based on
 * {@link org.neo4j.kernel.impl.transaction.command.Command}s in transaction state.
 * It is done by inferring {@link org.neo4j.kernel.api.index.NodePropertyUpdate}s from commands and asking
 * {@link org.neo4j.kernel.impl.api.index.IndexingService} to check those via
 * {@link org.neo4j.kernel.impl.api.index.IndexingService#validate(Iterable)}.
 */
public class IndexUpdatesValidator
{
    private final NodeStore nodeStore;
    private final PropertyStore propertyStore;
    private final PropertyLoader propertyLoader;
    private final IndexingService indexing;

    public IndexUpdatesValidator( NeoStore neoStore, PropertyLoader propertyLoader, IndexingService indexing )
    {
        this.nodeStore = neoStore.getNodeStore();
        this.propertyStore = neoStore.getPropertyStore();
        this.propertyLoader = propertyLoader;
        this.indexing = indexing;
    }

    public ValidatedIndexUpdates validate( TransactionRepresentation transaction, TransactionApplicationMode mode )
            throws IOException
    {
        NodePropertyCommandsExtractor extractor = new NodePropertyCommandsExtractor();
        transaction.accept( extractor );

        if ( extractor.noCommandsExtracted() )
        {
            return ValidatedIndexUpdates.NONE;
        }

        if ( mode == TransactionApplicationMode.RECOVERY )
        {
            return newValidatedRecoveredUpdates( extractor.changedNodeIds(), indexing );
        }

        Iterable<NodePropertyUpdate> updates = new LazyIndexUpdates( nodeStore, propertyStore, propertyLoader,
                extractor.propertyCommandsByNodeIds, extractor.nodeCommandsById );

        return indexing.validate( updates );
    }

    private static ValidatedIndexUpdates newValidatedRecoveredUpdates( final PrimitiveLongSet nodeIds,
            final IndexingService indexing )
    {
        return new ValidatedIndexUpdates()
        {
            @Override
            public void flush()
            {
                indexing.addRecoveredNodeIds( nodeIds );
            }

            @Override
            public void close()
            {
            }

            @Override
            public boolean hasChanges()
            {
                return !nodeIds.isEmpty();
            }
        };
    }

    private static class NodePropertyCommandsExtractor
            extends NeoCommandHandler.Adapter implements Visitor<Command,IOException>
    {
        final PrimitiveLongObjectMap<NodeCommand> nodeCommandsById = Primitive.longObjectMap( 16 );
        final PrimitiveLongObjectMap<List<PropertyCommand>> propertyCommandsByNodeIds = Primitive.longObjectMap( 16 );

        @Override
        public boolean visit( Command element ) throws IOException
        {
            element.handle( this );
            return false;
        }

        @Override
        public boolean visitNodeCommand( NodeCommand command ) throws IOException
        {
            nodeCommandsById.put( command.getKey(), command );
            return false;
        }

        @Override
        public boolean visitPropertyCommand( PropertyCommand command ) throws IOException
        {
            PropertyRecord record = command.getAfter();
            if ( record.isNodeSet() )
            {
                long nodeId = command.getAfter().getNodeId();
                List<PropertyCommand> group = propertyCommandsByNodeIds.get( nodeId );
                if ( group == null )
                {
                    propertyCommandsByNodeIds.put( nodeId, group = new ArrayList<>() );
                }
                group.add( command );
            }
            return false;
        }

        boolean noCommandsExtracted()
        {
            return nodeCommandsById.isEmpty() && propertyCommandsByNodeIds.isEmpty();
        }

        PrimitiveLongSet changedNodeIds()
        {
            PrimitiveLongSet nodeIds = Primitive.longSet( nodeCommandsById.size() + propertyCommandsByNodeIds.size() );
            nodeCommandsById.visitKeys( copyTo( nodeIds ) );
            propertyCommandsByNodeIds.visitKeys( copyTo( nodeIds ) );
            return nodeIds;
        }

        PrimitiveLongVisitor<RuntimeException> copyTo( final PrimitiveLongSet set )
        {
            return new PrimitiveLongVisitor<RuntimeException>()
            {
                @Override
                public boolean visited( long value )
                {
                    set.add( value );
                    return false;
                }
            };
        }
    }
}
