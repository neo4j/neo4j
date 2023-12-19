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
package org.neo4j.causalclustering.core.replication;

@SuppressWarnings( "WeakerAccess" )
public class BenchmarkResult
{
    public Long totalRequests;
    public Long totalBytes;
    public Long timeMillis;
    public Double opsPerMilli;
    public Double mbPerSecond;

    BenchmarkResult( long totalRequests, long totalBytes, long totalTimeMillis )
    {
        this.totalRequests = totalRequests;
        this.totalBytes = totalBytes;
        this.timeMillis = totalTimeMillis;
        this.opsPerMilli = totalRequests / (double) totalTimeMillis;
        this.mbPerSecond = totalBytes / (double) totalTimeMillis / 1048576 * 1000;
    }
}
