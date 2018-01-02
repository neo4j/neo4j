/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
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
