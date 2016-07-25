/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport;

import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.unsafe.impl.batchimport.staging.BatchSender;
import org.neo4j.unsafe.impl.batchimport.staging.Configuration;
import org.neo4j.unsafe.impl.batchimport.staging.ProcessorStep;
import org.neo4j.unsafe.impl.batchimport.staging.StageControl;

/**
 * After this step is {@link #done()} all BOTH ID fields in the rel group cache will contain,
 * for each node, the absolute number of groups from node 0 up to this point there is.
 */
public class CountGroupsStep extends ProcessorStep<RelationshipGroupRecord[]>
{
    private final RelationshipGroupCache cache;

    public CountGroupsStep( StageControl control, Configuration config, RelationshipGroupCache groupCache )
    {
        super( control, "COUNT", config, 1 );
        this.cache = groupCache;
    }

    @Override
    protected void process( RelationshipGroupRecord[] batch, BatchSender sender ) throws Throwable
    {
        for ( RelationshipGroupRecord group : batch )
        {
            cache.incrementGroupCount( group.getOwningNode() );
        }
    }
}
