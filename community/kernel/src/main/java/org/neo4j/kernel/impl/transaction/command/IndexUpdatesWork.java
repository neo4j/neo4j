/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.transaction.command;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.concurrent.Work;
import org.neo4j.helpers.collection.NestingIterator;
import org.neo4j.internal.kernel.api.schema.SchemaDescriptor;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.impl.api.index.IndexingUpdateService;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.transaction.command.Command.NodeCommand;
import org.neo4j.kernel.impl.transaction.command.Command.PropertyCommand;
import org.neo4j.kernel.impl.transaction.state.IndexUpdates;

/**
 * Combines {@link IndexUpdates} from multiple transactions into one bigger job.
 */
public class IndexUpdatesWork implements Work<IndexingUpdateService,IndexUpdatesWork>
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
    public void apply( IndexingUpdateService material )
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
            public Iterator<IndexEntryUpdate<SchemaDescriptor>> iterator()
            {
                return new NestingIterator<IndexEntryUpdate<SchemaDescriptor>,IndexUpdates>( updates.iterator() )
                {
                    @Override
                    protected Iterator<IndexEntryUpdate<SchemaDescriptor>> createNestedIterator( IndexUpdates item )
                    {
                        return item.iterator();
                    }
                };
            }

            @Override
            public void feed( PrimitiveLongObjectMap<List<PropertyCommand>> propCommands,
                    PrimitiveLongObjectMap<NodeCommand> nodeCommands )
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean hasUpdates()
            {
                return true;
            }
        };
    }
}
