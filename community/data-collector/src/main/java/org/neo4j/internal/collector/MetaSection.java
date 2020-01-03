/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.collector;

import java.lang.management.CompilationMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.RuntimeMXBean;
import java.nio.ByteOrder;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Stream;

import org.neo4j.internal.kernel.api.Kernel;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.os.OsBeanUtil;

/**
 * Data collector section that contains meta data about the System,
 * Neo4j deployment, graph token counts, and retrieval.
 */
final class MetaSection
{
    private MetaSection()
    { // only static methods
    }

    static Stream<RetrieveResult> retrieve( String graphToken,
                                            Kernel kernel,
                                            long numSilentQueryDrops ) throws TransactionFailureException
    {
        Map<String, Object> systemData = new HashMap<>();
        systemData.put( "jvmMemoryFree", Runtime.getRuntime().freeMemory() );
        systemData.put( "jvmMemoryTotal", Runtime.getRuntime().totalMemory() );
        systemData.put( "jvmMemoryMax", Runtime.getRuntime().maxMemory() );
        systemData.put( "systemTotalPhysicalMemory", OsBeanUtil.getTotalPhysicalMemory() );
        systemData.put( "systemFreePhysicalMemory", OsBeanUtil.getFreePhysicalMemory() );
        systemData.put( "systemCommittedVirtualMemory", OsBeanUtil.getCommittedVirtualMemory() );
        systemData.put( "systemTotalSwapSpace", OsBeanUtil.getTotalSwapSpace() );
        systemData.put( "systemFreeSwapSpace", OsBeanUtil.getFreeSwapSpace() );

        OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();
        systemData.put( "osArch", os.getArch() );
        systemData.put( "osName", os.getName() );
        systemData.put( "osVersion", os.getVersion() );
        systemData.put( "availableProcessors", os.getAvailableProcessors() );
        systemData.put( "byteOrder", ByteOrder.nativeOrder().toString() );

        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        systemData.put( "jvmName", runtime.getVmName() );
        systemData.put( "jvmVendor", runtime.getVmVendor() );
        systemData.put( "jvmVersion", runtime.getVmVersion() );
        CompilationMXBean compiler = ManagementFactory.getCompilationMXBean();
        systemData.put( "jvmJITCompiler", compiler == null ? "unknown" : compiler.getName() );

        systemData.put( "userLanguage", Locale.getDefault().getLanguage() );
        systemData.put( "userCountry", Locale.getDefault().getCountry() );
        systemData.put( "userTimezone", TimeZone.getDefault().getID() );
        systemData.put( "fileEncoding",  System.getProperty( "file.encoding" ) );

        Map<String, Object> internalData = new HashMap<>();
        internalData.put( "numSilentQueryCollectionMisses", numSilentQueryDrops );

        Map<String, Object> metaData = new HashMap<>();
        metaData.put( "graphToken", graphToken );
        metaData.put( "retrieveTime", ZonedDateTime.now() );
        metaData.put( "system", systemData );
        metaData.put( "internal", internalData );

        TokensSection.putTokenCounts( metaData, kernel );

        return Stream.of( new RetrieveResult( Sections.META, metaData ) );
    }
}
