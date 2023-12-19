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
package org.neo4j.causalclustering.catchup;

import static java.lang.String.format;

public enum ResponseMessageType
{
    TX( (byte) 1 ),
    STORE_ID( (byte) 2 ),
    FILE( (byte) 3 ),
    STORE_COPY_FINISHED( (byte) 4 ),
    CORE_SNAPSHOT( (byte) 5 ),
    TX_STREAM_FINISHED( (byte) 6 ),
    PREPARE_STORE_COPY_RESPONSE( (byte) 7 ),
    INDEX_SNAPSHOT_RESPONSE( (byte) 8 ),
    UNKNOWN( (byte) 200 );

    private byte messageType;

    ResponseMessageType( byte messageType )
    {
        this.messageType = messageType;
    }

    public static ResponseMessageType from( byte b )
    {
        for ( ResponseMessageType responseMessageType : values() )
        {
            if ( responseMessageType.messageType == b )
            {
                return responseMessageType;
            }
        }
        return UNKNOWN;
    }

    public byte messageType()
    {
        return messageType;
    }

    @Override
    public String toString()
    {
        return format( "ResponseMessageType{messageType=%s}", messageType );
    }
}
