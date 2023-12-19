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
package org.neo4j.kernel.monitoring.tracing;

import org.junit.Test;

import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.BufferingLog;
import org.neo4j.test.OnDemandJobScheduler;
import org.neo4j.time.Clocks;

import static org.junit.Assert.assertEquals;

public class VerboseTracerFactoryTest
{

    @Test
    public void verboseTracerFactoryRegisterTracerWithCodeNameVerbose()
    {
        assertEquals( "verbose", tracerFactory().getImplementationName() );
    }

    @Test
    public void verboseFactoryCreateVerboseTracer()
    {
        BufferingLog msgLog = new BufferingLog();
        PageCacheTracer pageCacheTracer = tracerFactory().createPageCacheTracer( new Monitors(),
                new OnDemandJobScheduler(), Clocks.nanoClock(), msgLog );
        pageCacheTracer.beginCacheFlush();
        assertEquals( "Start whole page cache flush.", msgLog.toString().trim() );
    }

    private VerboseTracerFactory tracerFactory()
    {
        return new VerboseTracerFactory();
    }
}
