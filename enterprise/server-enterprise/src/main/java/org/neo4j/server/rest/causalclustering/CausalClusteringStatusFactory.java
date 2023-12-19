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
package org.neo4j.server.rest.causalclustering;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.server.rest.repr.OutputFormat;

public class CausalClusteringStatusFactory
{
    public static CausalClusteringStatus build( OutputFormat output, GraphDatabaseService db )
    {
        if ( db instanceof CoreGraphDatabase )
        {
            return new CoreStatus( output, (CoreGraphDatabase) db );
        }
        else if ( db instanceof ReadReplicaGraphDatabase )
        {
            return new ReadReplicaStatus( output );
        }
        else
        {
            return new NotCausalClustering( output );
        }
    }
}
