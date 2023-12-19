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
package org.neo4j.kernel.ha;

import org.neo4j.com.ObjectSerializer;
import org.neo4j.com.RequestType;
import org.neo4j.com.TargetCaller;

abstract class AbstractHaRequestTypes implements HaRequestTypes
{
    private final HaRequestType[] types = new HaRequestType[HaRequestTypes.Type.values().length];

    protected <A,B,C> void register( Type type, TargetCaller<A,B> targetCaller, ObjectSerializer<C> objectSerializer )
    {
        register( type, targetCaller, objectSerializer, true );
    }

    protected <A,B,C> void register( Type type, TargetCaller<A,B> targetCaller, ObjectSerializer<C> objectSerializer, boolean unpack )
    {
        assert types[type.ordinal()] == null;
        types[type.ordinal()] = new HaRequestType( targetCaller, objectSerializer, (byte)type.ordinal(), unpack );
    }

    @Override
    public RequestType type( Type type )
    {
        return type( (byte) type.ordinal() );
    }

    @Override
    public RequestType type( byte id )
    {
        HaRequestType requestType = types[id];
        if ( requestType == null )
        {
            throw new UnsupportedOperationException(
                    "Not used anymore, merely here to keep the ordinal ids of the others" );
        }
        return requestType;
    }
}
