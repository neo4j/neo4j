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
package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;

public class FailingByteChannel extends KnownDataByteChannel
{
    private final String failWithMessage;
    private final int sizeToFailAt;

    public FailingByteChannel( int sizeToFailAt, String failWithMessage )
    {
        super( sizeToFailAt * 2 );
        this.sizeToFailAt = sizeToFailAt;
        this.failWithMessage = failWithMessage;
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        if ( position > sizeToFailAt )
        {
            throw new MadeUpException( failWithMessage );
        }
        return super.read( dst );
    }
}
