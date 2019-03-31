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
package org.neo4j.internal.recordstorage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.helpers.collection.NestingIterator;
import org.neo4j.internal.schema.SchemaDescriptor;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.util.concurrent.Work;

/**
 * Combines {@link IndexUpdates} from multiple transactions into one bigger job.
 */
public class IndexUpdatesWork implements Work<IndexUpdateListener,IndexUpdatesWork>
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
    public void apply( IndexUpdateListener material )
    {
        try
        {
            material.applyUpdates( combinedUpdates() );
        }
        catch ( IOException | KernelException e )
        {
            throw new UnderlyingStorageException( e );
        }
    }

    private Iterable<IndexEntryUpdate<SchemaDescriptor>> combinedUpdates()
    {
        return () -> new NestingIterator<IndexEntryUpdate<SchemaDescriptor>,IndexUpdates>( updates.iterator() )
        {
            @Override
            protected Iterator<IndexEntryUpdate<SchemaDescriptor>> createNestedIterator( IndexUpdates item )
            {
                return item.iterator();
            }
        };
    }
}
