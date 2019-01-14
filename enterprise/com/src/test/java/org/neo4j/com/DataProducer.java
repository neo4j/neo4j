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
package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import static java.lang.Math.min;

public class DataProducer implements ReadableByteChannel
{
    private int bytesLeftToProduce;
    private boolean closed;

    public DataProducer( int size )
    {
        this.bytesLeftToProduce = size;
    }

    @Override
    public boolean isOpen()
    {
        return !closed;
    }

    @Override
    public void close()
    {
        if ( closed )
        {
            throw new IllegalStateException( "Already closed" );
        }
        closed = true;
    }

    @Override
    public int read( ByteBuffer dst )
    {
        int toFill = min( dst.remaining(), bytesLeftToProduce );
        int leftToFill = toFill;
        if ( toFill <= 0 )
        {
            return -1;
        }

        while ( leftToFill-- > 0 )
        {
            dst.put( (byte) 5 );
        }
        bytesLeftToProduce -= toFill;
        return toFill;
    }
}
