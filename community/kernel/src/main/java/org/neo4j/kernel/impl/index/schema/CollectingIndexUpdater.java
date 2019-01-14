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
package org.neo4j.kernel.impl.index.schema;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexEntryUpdate;
import org.neo4j.kernel.api.index.IndexUpdater;

/**
 * Collects updates from {@link #process(IndexEntryUpdate)} and passes them to the {@link Applier} on {@link #close()}.
 */
public class CollectingIndexUpdater implements IndexUpdater
{
    private final Applier applier;

    private boolean closed;
    private final Collection<IndexEntryUpdate<?>> updates = new ArrayList<>();

    public CollectingIndexUpdater( Applier applier )
    {
        this.applier = applier;
    }

    @Override
    public void process( IndexEntryUpdate<?> update )
    {
        assertOpen();
        updates.add( update );
    }

    @Override
    public void close() throws IndexEntryConflictException
    {
        assertOpen();
        try
        {
            applier.accept( updates );
        }
        finally
        {
            closed = true;
        }
    }

    private void assertOpen()
    {
        if ( closed )
        {
            throw new IllegalStateException( "Updater has been closed" );
        }
    }

    public interface Applier
    {
        void accept( Collection<IndexEntryUpdate<?>> updates ) throws IndexEntryConflictException;
    }
}
