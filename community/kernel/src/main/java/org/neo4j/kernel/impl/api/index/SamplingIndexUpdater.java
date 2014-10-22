/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import org.neo4j.kernel.api.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.NodePropertyUpdate;
import org.neo4j.kernel.api.index.PopulatingValueSampler;

public class SamplingIndexUpdater implements IndexUpdater
{
    private final IndexUpdater delegate;
    private final PopulatingValueSampler sampler;

    public SamplingIndexUpdater( IndexUpdater delegate, PopulatingValueSampler sampler )
    {
        this.delegate = delegate;
        this.sampler = sampler;
    }

    @Override
    public void process( NodePropertyUpdate update ) throws IOException, IndexEntryConflictException
    {
        switch ( update.getUpdateMode() )
        {
            case ADDED:
                sampler.include( update.getValueAfter() );
                break;

            case REMOVED:
                sampler.exclude( update.getValueBefore() );
                break;

            default:
                break;
        }
        delegate.process( update );
    }

    @Override
    public void close() throws IOException, IndexEntryConflictException
    {
        delegate.close();
    }

    @Override
    public void remove( Iterable<Long> nodeIds ) throws IOException
    {
        throw new UnsupportedOperationException();
    }
}
