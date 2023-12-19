/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus.log.cache;

public interface InFlightCacheMonitor
{
    InFlightCacheMonitor VOID = new InFlightCacheMonitor()
    {
        @Override
        public void miss()
        {
        }

        @Override
        public void hit()
        {

        }

        @Override
        public void setMaxBytes( long maxBytes )
        {

        }

        @Override
        public void setTotalBytes( long totalBytes )
        {

        }

        @Override
        public void setMaxElements( int maxElements )
        {

        }

        @Override
        public void setElementCount( int elementCount )
        {

        }
    };

    void miss();

    void hit();

    void setMaxBytes( long maxBytes );

    void setTotalBytes( long totalBytes );

    void setMaxElements( int maxElements );

    void setElementCount( int elementCount );
}
