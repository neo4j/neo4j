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
package org.neo4j.bolt.v1.messaging;

import org.junit.Test;
import org.mockito.InOrder;

import java.io.IOException;

import org.neo4j.bolt.logging.NullBoltMessageLogger;
import org.neo4j.bolt.v1.packstream.PackOutput;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.v1.messaging.BoltResponseMessage.IGNORED;
import static org.neo4j.values.storable.DateValue.date;
import static org.neo4j.values.storable.Values.intValue;
import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.map;

public class BoltResponseMessageWriterTest
{
    @Test
    public void shouldWriteRecordMessage() throws Exception
    {
        PackOutput output = mock( PackOutput.class );
        Neo4jPack.Packer packer = mock( Neo4jPack.Packer.class );

        BoltResponseMessageWriter writer = newWriter( output, packer );

        writer.onRecord( () -> new AnyValue[]{longValue( 42 ), stringValue( "42" )} );

        InOrder inOrder = inOrder( output, packer );
        inOrder.verify( output ).beginMessage();
        inOrder.verify( packer ).pack( longValue( 42 ) );
        inOrder.verify( packer ).pack( stringValue( "42" ) );
        inOrder.verify( output ).messageSucceeded();
    }

    @Test
    public void shouldWriteSuccessMessage() throws Exception
    {
        PackOutput output = mock( PackOutput.class );
        Neo4jPack.Packer packer = mock( Neo4jPack.Packer.class );

        BoltResponseMessageWriter writer = newWriter( output, packer );

        MapValue metadata = map( new String[]{"a", "b", "c"}, new AnyValue[]{intValue( 1 ), stringValue( "2" ), date( 2010, 02, 02 )} );
        writer.onSuccess( metadata );

        InOrder inOrder = inOrder( output, packer );
        inOrder.verify( output ).beginMessage();
        inOrder.verify( packer ).pack( metadata );
        inOrder.verify( output ).messageSucceeded();
    }

    @Test
    public void shouldWriteFailureMessage() throws Exception
    {
        PackOutput output = mock( PackOutput.class );
        Neo4jPack.Packer packer = mock( Neo4jPack.Packer.class );

        BoltResponseMessageWriter writer = newWriter( output, packer );

        Status.Transaction errorStatus = Status.Transaction.DeadlockDetected;
        String errorMessage = "Hi Deadlock!";
        writer.onFailure( errorStatus, errorMessage );

        InOrder inOrder = inOrder( output, packer );
        inOrder.verify( output ).beginMessage();
        inOrder.verify( packer ).pack( errorStatus.code().serialize() );
        inOrder.verify( packer ).pack( errorMessage );
        inOrder.verify( output ).messageSucceeded();
    }

    @Test
    public void shouldWriteIgnoredMessage() throws Exception
    {
        PackOutput output = mock( PackOutput.class );
        Neo4jPack.Packer packer = mock( Neo4jPack.Packer.class );

        BoltResponseMessageWriter writer = newWriter( output, packer );

        writer.onIgnored();

        InOrder inOrder = inOrder( output, packer );
        inOrder.verify( output ).beginMessage();
        inOrder.verify( packer ).packStructHeader( 0, IGNORED.signature() );
        inOrder.verify( output ).messageSucceeded();
    }

    @Test
    public void shouldFlush() throws Exception
    {
        PackOutput output = mock( PackOutput.class );
        Neo4jPack.Packer packer = mock( Neo4jPack.Packer.class );

        BoltResponseMessageWriter writer = newWriter( output, packer );

        writer.flush();

        verify( packer ).flush();
    }

    @Test
    public void shouldNotifyOutputAboutFailedRecordMessage() throws Exception
    {
        PackOutput output = mock( PackOutput.class );
        Neo4jPack.Packer packer = mock( Neo4jPack.Packer.class );
        IOException error = new IOException( "Unable to pack 42" );
        doThrow( error ).when( packer ).pack( longValue( 42 ) );

        BoltResponseMessageWriter writer = newWriter( output, packer );

        try
        {
            writer.onRecord( () -> new AnyValue[]{stringValue( "42" ), longValue( 42 )} );
            fail( "Exception expected" );
        }
        catch ( IOException e )
        {
            assertEquals( error, e );
        }

        InOrder inOrder = inOrder( output, packer );
        inOrder.verify( output ).beginMessage();
        inOrder.verify( packer ).pack( stringValue( "42" ) );
        inOrder.verify( packer ).pack( longValue( 42 ) );
        inOrder.verify( output ).messageFailed();
    }

    @Test
    public void shouldNotNotifyOutputWhenOutputItselfFails() throws Exception
    {
        PackOutput output = mock( PackOutput.class );
        Neo4jPack.Packer packer = mock( Neo4jPack.Packer.class );
        IOException error = new IOException( "Unable to flush" );
        doThrow( error ).when( output ).messageSucceeded();

        BoltResponseMessageWriter writer = newWriter( output, packer );

        try
        {
            writer.onRecord( () -> new AnyValue[]{longValue( 1 ), longValue( 2 )} );
            fail( "Exception expected" );
        }
        catch ( IOException e )
        {
            assertEquals( error, e );
        }

        InOrder inOrder = inOrder( output, packer );
        inOrder.verify( output ).beginMessage();
        inOrder.verify( packer ).pack( longValue( 1 ) );
        inOrder.verify( packer ).pack( longValue( 2 ) );
        inOrder.verify( output ).messageSucceeded();

        verify( output, never() ).messageFailed();
    }

    private static BoltResponseMessageWriter newWriter( PackOutput output, Neo4jPack.Packer packer )
    {
        Neo4jPack neo4jPack = newNeo4jPackMock( output, packer );
        return new BoltResponseMessageWriter( neo4jPack, output, NullLogService.getInstance(), NullBoltMessageLogger.getInstance() );
    }

    private static Neo4jPack newNeo4jPackMock( PackOutput output, Neo4jPack.Packer packer )
    {
        Neo4jPack neo4jPack = mock( Neo4jPack.class );
        when( neo4jPack.newPacker( output ) ).thenReturn( packer );
        return neo4jPack;
    }
}
