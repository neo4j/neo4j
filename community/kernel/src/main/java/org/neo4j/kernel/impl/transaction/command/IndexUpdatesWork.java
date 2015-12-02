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
package org.neo4j.kernel.impl.transaction.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.concurrent.Work;
import org.neo4j.helpers.collection.NestingIterator;
import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.state.IndexUpdates;

/**
 * Combines {@link IndexUpdates} from multiple transactions into one bigger job.
 */
public class IndexUpdatesWork implements Work<IndexingService,IndexUpdatesWork>
{
    private final List<IndexUpdates> updates = new ArrayList<>();

    public IndexUpdatesWork( IndexUpdates updates )
    {
        this.updates.add( updates );
    }

    @Override
    public IndexUpdatesWork combine( IndexUpdatesWork work )
    {
        updates.addAll( work.updates );
        return this;
    }

    @Override
    public void apply( IndexingService material )
    {
        try
        {
            material.apply( combinedUpdates() );
        }
        catch ( IOException | IndexEntryConflictException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private IndexUpdates combinedUpdates()
    {
        return new IndexUpdates()
        {
            @Override
            public Iterator<NodePropertyUpdate> iterator()
            {
                return new NestingIterator<NodePropertyUpdate,IndexUpdates>( updates.iterator() )
                {
                    @Override
                    protected Iterator<NodePropertyUpdate> createNestedIterator( IndexUpdates item )
                    {
                        return item.iterator();
                    }
                };
            }

            @Override
            public void collectUpdatedNodeIds( PrimitiveLongSet target )
            {
                for ( IndexUpdates indexUpdates : updates )
                {
                    indexUpdates.collectUpdatedNodeIds( target );
                }
            }

            @Override
            public void feed( PrimitiveLongObjectMap<List<PropertyCommand>> propCommands,
                    PrimitiveLongObjectMap<NodeCommand> nodeCommands )
            {
                throw new UnsupportedOperationException();
            }
        };
    }
}
