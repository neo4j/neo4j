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
package org.neo4j.kernel.impl.api.scan;

import java.io.IOException;
import java.util.List;

import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.labelscan.LabelScanWriter;
import org.neo4j.kernel.api.labelscan.NodeLabelUpdate;

/**
 * Stream of changes used to rebuild a {@link LabelScanStore} from scratch.
 */
public interface FullStoreChangeStream
{
    FullStoreChangeStream EMPTY = writer -> 0;

    long applyTo( LabelScanWriter writer ) throws IOException;

    static FullStoreChangeStream asStream( final List<NodeLabelUpdate> existingData )
    {
        return writer ->
        {
            long count = 0;
            for ( NodeLabelUpdate update : existingData )
            {
                writer.write( update );
                count++;
            }
            return count;
        };
    }
}
