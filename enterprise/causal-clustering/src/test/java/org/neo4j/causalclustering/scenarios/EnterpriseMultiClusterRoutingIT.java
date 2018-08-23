/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.scenarios;

import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

import static org.neo4j.causalclustering.scenarios.EnterpriseDiscoveryServiceType.HAZELCAST;
import static org.neo4j.causalclustering.scenarios.EnterpriseDiscoveryServiceType.SHARED;

public class EnterpriseMultiClusterRoutingIT extends BaseMultiClusterRoutingIT
{
    public EnterpriseMultiClusterRoutingIT( String ignoredName, int numCores, int numReplicas, Set<String> dbNames,
            DiscoveryServiceType discoveryType )
    {
        super( ignoredName, numCores, numReplicas, dbNames, discoveryType );
    }

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> data()
    {
        return Arrays.asList( new Object[][]
                {
                        { "[shared discovery, 6 core hosts, 2 databases]", 6, 0, DB_NAMES_1, SHARED },
                        { "[hazelcast discovery, 6 core hosts, 2 databases]", 6, 0, DB_NAMES_1, HAZELCAST },
                        { "[shared discovery, 5 core hosts, 1 database]", 5, 0, DB_NAMES_2, SHARED },
                        { "[hazelcast discovery, 5 core hosts, 1 database]", 5, 0, DB_NAMES_2, HAZELCAST },
                        { "[hazelcast discovery, 6 core hosts, 3 read replicas, 3 databases]", 9, 3, DB_NAMES_3, HAZELCAST },
                        { "[shared discovery, 6 core hosts, 3 read replicas, 3 databases]", 8, 2, DB_NAMES_3, SHARED }
                }
        );
    }

}
