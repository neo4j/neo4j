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
package org.neo4j.kernel.impl.store.counts;

import org.neo4j.kernel.impl.api.CountsAccessor;
import org.neo4j.kernel.impl.store.counts.keys.CountsKeyFactory;

class IndexStatsUpdaterFactory
{
    protected CountsAccessor.IndexStatsUpdater indexStatsUpdater(CountsStore countsStore)
    {
        return new CountsAccessor.IndexStatsUpdater()
        {
            @Override
            public void replaceIndexUpdateAndSize( int labelId, int propertyKeyId, long updates, long size )
            {
                countsStore.replace( CountsKeyFactory.indexStatisticsKey( labelId, propertyKeyId ),
                        new long[]{updates, size} );
            }

            @Override
            public void replaceIndexSample( int labelId, int propertyKeyId, long unique, long size )
            {
                countsStore
                        .replace( CountsKeyFactory.indexSampleKey( labelId, propertyKeyId ), new long[]{unique, size} );
            }

            @Override
            public void incrementIndexUpdates( int labelId, int propertyKeyId, long delta )
            {
                countsStore.update( CountsKeyFactory.indexSampleKey( labelId, propertyKeyId ), new long[]{delta} );
            }

            @Override
            public void close()
            {
                // nothing to do
            }
        };
    }
}
