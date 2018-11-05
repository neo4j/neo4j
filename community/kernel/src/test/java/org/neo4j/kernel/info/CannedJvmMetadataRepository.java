/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.lang.management.MemoryUsage;
import java.util.List;

import static java.util.Collections.emptyList;

public class CannedJvmMetadataRepository extends JvmMetadataRepository
{
    private final String javaVmName;
    private final String javaVersion;
    private final List<String> inputArguments;
    private final long initialHeapSize;
    private final long maxHeapSize;

    CannedJvmMetadataRepository( String javaVmName, String javaVersion )
    {
        this( javaVmName, javaVersion, emptyList(), 1, 2 );
    }

    CannedJvmMetadataRepository( String javaVmName, String javaVersion, List<String> inputArguments, long initialHeapSize, long maxHeapSize )
    {
        this.javaVmName = javaVmName;
        this.javaVersion = javaVersion;
        this.inputArguments = inputArguments;
        this.initialHeapSize = initialHeapSize;
        this.maxHeapSize = maxHeapSize;
    }

    @Override
    public String getJavaVmName()
    {
        return javaVmName;
    }

    @Override
    public String getJavaVersion()
    {
        return javaVersion;
    }

    @Override
    public List<String> getJvmInputArguments()
    {
        return inputArguments;
    }

    @Override
    public MemoryUsage getHeapMemoryUsage()
    {
        return new MemoryUsage( initialHeapSize, 0, 0, maxHeapSize );
    }
}
