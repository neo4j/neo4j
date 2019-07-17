/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v3.messaging.request;

import java.io.IOException;

import org.neo4j.bolt.messaging.Neo4jPack.Unpacker;
import org.neo4j.bolt.v3.messaging.decoder.BeginMessageDecoder;
import org.neo4j.values.virtual.MapValue;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.testing.NullResponseHandler.nullResponseHandler;

class BeginMessageTest extends AbstractTransactionInitiatingMessage
{
    @Override
    protected TransactionInitiatingMessage createMessage()
    {
        return new BeginMessage();
    }

    @Override
    protected TransactionInitiatingMessage createMessage( MapValue meta ) throws IOException
    {
        var unpacker = mock( Unpacker.class );
        when( unpacker.unpackMap() ).thenReturn( meta );
        var decoder = new BeginMessageDecoder( nullResponseHandler() );
        return (TransactionInitiatingMessage) decoder.decode( unpacker );
    }
}
