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
package org.neo4j.ext.udc.impl;

import org.junit.Test;

import org.neo4j.ext.udc.UdcConstants;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.KernelData;
import org.neo4j.kernel.Version;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.udc.impl.ComponentVersion;

import static org.junit.Assert.assertNotNull;

public class DefaultUdcInformationCollectorTest
{
    private final DefaultUdcInformationCollector collector = new DefaultUdcInformationCollector( new Config(), null,
            new StubKernelData() );

    @Test
    public void shouldIncludeTheMacAddress()
    {
        assertNotNull( collector.getUdcParams().get( UdcConstants.MAC ) );
    }

    @Test
    public void shouldIncludeTheNumberOfProcessors()
    {
        assertNotNull( collector.getUdcParams().get( UdcConstants.NUM_PROCESSORS ) );
    }

    @Test
    public void shouldIncludeTotalMemorySize()
    {
        assertNotNull( collector.getUdcParams().get( UdcConstants.TOTAL_MEMORY ) );
    }

    @Test
    public void shouldIncludeHeapSize()
    {
        assertNotNull( collector.getUdcParams().get( UdcConstants.HEAP_SIZE ) );
    }

    private static class StubKernelData extends KernelData
    {
        public StubKernelData()
        {
            super( new Config() );
        }

        @Override
        public Version version()
        {
            return new ComponentVersion();
        }

        @Override
        public GraphDatabaseAPI graphDatabase()
        {
            return null;
        }
    }
}
