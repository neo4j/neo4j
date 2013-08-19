/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

package org.neo4j.cluster.protocol.atomicbroadcast;

import java.util.HashMap;
import java.util.Map;

public class VersionMapper
{
    private static final Map<Long, Long> versionMapping = new HashMap<Long, Long>();

    static
    {
        long v1_9_1MemberIsUnavailable = -2826926850135965485l;
        long v1_9_3MemberIsUnavailable = 977671511575679253l;

        versionMapping.put( v1_9_1MemberIsUnavailable, v1_9_3MemberIsUnavailable );
        versionMapping.put( v1_9_3MemberIsUnavailable, v1_9_1MemberIsUnavailable );
    }

    public boolean hasMappingFor( long oldSUID )
    {
        return versionMapping.containsKey( oldSUID );
    }

    public long map( long oldSUID )
    {
        return versionMapping.get( oldSUID );
    }
}
