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

import java.util.List;
import java.util.function.Supplier;

import org.neo4j.concurrent.Work;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;
import org.neo4j.kernel.impl.store.UnderlyingStorageException;

import static org.neo4j.kernel.api.labelscan.NodeLabelUpdate.SORT_BY_NODE_ID;

public class LabelUpdateWork implements Work<Supplier<LabelScanWriter>,LabelUpdateWork>
{
    private final List<NodeLabelUpdate> labelUpdates;

    public LabelUpdateWork( List<NodeLabelUpdate> labelUpdates )
    {
        this.labelUpdates = labelUpdates;
    }

    @Override
    public LabelUpdateWork combine( LabelUpdateWork work )
    {
        labelUpdates.addAll( work.labelUpdates );
        return this;
    }

    @Override
    public void apply( Supplier<LabelScanWriter> labelScanStore )
    {
        labelUpdates.sort( SORT_BY_NODE_ID );
        try ( LabelScanWriter writer = labelScanStore.get() )
        {
            for ( NodeLabelUpdate update : labelUpdates )
            {
                writer.write( update );
            }
        }
        catch ( Exception e )
        {
            throw new UnderlyingStorageException( e );
        }
    }
}
