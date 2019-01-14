/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.causalclustering.core.consensus;

import java.util.Collection;

public class MajorityIncludingSelfQuorum
{
    private static final int MIN_QUORUM = 2;

    private MajorityIncludingSelfQuorum()
    {
    }

    public static boolean isQuorum( Collection<?> cluster, Collection<?> countNotIncludingMyself )
    {
        return isQuorum( cluster.size(), countNotIncludingMyself.size() );
    }

    public static boolean isQuorum( int clusterSize, int countNotIncludingSelf )
    {
        return isQuorum( MIN_QUORUM, clusterSize, countNotIncludingSelf );
    }

    public static boolean isQuorum( int minQuorum, int clusterSize, int countNotIncludingSelf )
    {
        return (countNotIncludingSelf + 1) >= minQuorum &&
                countNotIncludingSelf >= clusterSize / 2;
    }
}
