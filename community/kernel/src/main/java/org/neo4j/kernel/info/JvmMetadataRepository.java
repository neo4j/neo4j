/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.info;

import jdk.jfr.Configuration;
import jdk.jfr.FlightRecorder;
import jdk.jfr.consumer.RecordingStream;

import java.io.IOException;
import java.lang.Runtime.Version;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.text.ParseException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class JvmMetadataRepository
{
    public String getJavaVmName()
    {
        return System.getProperty( "java.vm.name" );
    }

    public Version getJavaVersion()
    {
        return Runtime.version();
    }

    public List<String> getJvmInputArguments()
    {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        return runtimeMXBean.getInputArguments();
    }

    public MemoryUsage getHeapMemoryUsage()
    {
        return ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
    }

    public long getReservedCodeCacheSize()
    {
        AtomicLong reservedSize = new AtomicLong( -1 );
        try ( var rs = new RecordingStream( Configuration.getConfiguration( "default" ) ) )
        {

            var latch = new CountDownLatch( 1 );
            rs.onEvent( "jdk.CodeCacheConfiguration", event ->
            {
                try
                {
                     reservedSize.set( event.getLong( "reservedSize" ) );
                }
                finally
                {
                    latch.countDown();
                }
            } );
            rs.startAsync();
            latch.await( 10, TimeUnit.SECONDS );
        }
        catch ( IOException | ParseException | InterruptedException e )
        {
            // ignore
        }
        return reservedSize.get();
    }
}
