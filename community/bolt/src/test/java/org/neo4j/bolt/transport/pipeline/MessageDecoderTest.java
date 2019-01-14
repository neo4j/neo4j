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
package org.neo4j.bolt.transport.pipeline;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.Map;

import org.neo4j.bolt.v1.messaging.BoltIOException;
import org.neo4j.bolt.v1.messaging.BoltRequestMessageHandler;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.packstream.PackedOutputArray;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.NullLogService;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.logging.Log;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualValues;

import static io.netty.buffer.ByteBufUtil.hexDump;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.v1.messaging.BoltRequestMessage.INIT;
import static org.neo4j.bolt.v1.messaging.BoltRequestMessage.RUN;
import static org.neo4j.bolt.v1.messaging.example.Edges.ALICE_KNOWS_BOB;
import static org.neo4j.bolt.v1.messaging.example.Nodes.ALICE;
import static org.neo4j.bolt.v1.messaging.example.Paths.ALL_PATHS;
import static org.neo4j.bolt.v1.messaging.message.AckFailureMessage.ackFailure;
import static org.neo4j.bolt.v1.messaging.message.DiscardAllMessage.discardAll;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.ResetMessage.reset;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.serialize;
import static org.neo4j.values.storable.Values.durationValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

@RunWith( Parameterized.class )
public class MessageDecoderTest
{
    private EmbeddedChannel channel;

    @Parameterized.Parameter( 0 )
    public Neo4jPack packerUnderTest;

    @Parameterized.Parameter( 1 )
    public String name;

    @Parameterized.Parameters( name = "{1}" )
    public static Object[][] testParameters()
    {
        return new Object[][]{new Object[]{new Neo4jPackV1(), "V1"}, new Object[]{new Neo4jPackV2(), "V2"}};
    }

    @After
    public void cleanup()
    {
        if ( channel != null )
        {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    public void shouldDispatchInit() throws Exception
    {
        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );
        channel = new EmbeddedChannel( newDecoder( handler ) );

        String userAgent = "Test/User Agent 1.0";
        Map<String, Object> authToken = MapUtil.map( "scheme", "basic", "principal", "user", "credentials", "password" );

        channel.writeInbound( Unpooled.wrappedBuffer( serialize( packerUnderTest, init( userAgent, authToken ) ) ) );
        channel.finishAndReleaseAll();

        verify( handler ).onInit( userAgent, authToken );
        verifyNoMoreInteractions( handler );
    }

    @Test
    public void shouldDispatchAckFailure() throws Exception
    {
        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );
        channel = new EmbeddedChannel( newDecoder( handler ) );

        channel.writeInbound( Unpooled.wrappedBuffer( serialize( packerUnderTest, ackFailure() ) ) );
        channel.finishAndReleaseAll();

        verify( handler ).onAckFailure();
        verifyNoMoreInteractions( handler );
    }

    @Test
    public void shouldDispatchReset() throws Exception
    {
        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );
        channel = new EmbeddedChannel( newDecoder( handler ) );

        channel.writeInbound( Unpooled.wrappedBuffer( serialize( packerUnderTest, reset() ) ) );
        channel.finishAndReleaseAll();

        verify( handler ).onReset();
        verifyNoMoreInteractions( handler );
    }

    @Test
    public void shouldDispatchRun() throws Exception
    {
        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );
        channel = new EmbeddedChannel( newDecoder( handler ) );

        String statement = "RETURN 1";
        MapValue parameters = ValueUtils.asMapValue( MapUtil.map( "param1", 1, "param2", "2", "param3", true, "param4", 5.0 ) );

        channel.writeInbound( Unpooled.wrappedBuffer( serialize( packerUnderTest, run( statement, parameters ) ) ) );
        channel.finishAndReleaseAll();

        verify( handler ).onRun( statement, parameters );
        verifyNoMoreInteractions( handler );
    }

    @Test
    public void shouldDispatchDiscardAll() throws Exception
    {
        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );
        channel = new EmbeddedChannel( newDecoder( handler ) );

        channel.writeInbound( Unpooled.wrappedBuffer( serialize( packerUnderTest, discardAll() ) ) );
        channel.finishAndReleaseAll();

        verify( handler ).onDiscardAll();
        verifyNoMoreInteractions( handler );
    }

    @Test
    public void shouldDispatchPullAll() throws Exception
    {
        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );
        channel = new EmbeddedChannel( newDecoder( handler ) );

        channel.writeInbound( Unpooled.wrappedBuffer( serialize( packerUnderTest, pullAll() ) ) );
        channel.finishAndReleaseAll();

        verify( handler ).onPullAll();
        verifyNoMoreInteractions( handler );
    }

    @Test
    public void shouldCallExternalErrorOnInitWithNullKeys() throws Exception
    {
        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );
        channel = new EmbeddedChannel( newDecoder( handler ) );

        String userAgent = "Test/User Agent 1.0";
        Map<String,Object> authToken = MapUtil.map( "scheme", "basic", null, "user", "credentials", "password" );

        channel.writeInbound( Unpooled.wrappedBuffer( serialize( packerUnderTest, init( userAgent, authToken ) ) ) );
        channel.finishAndReleaseAll();

        verify( handler ).onExternalError(
                Neo4jError.from( Status.Request.Invalid, "Value `null` is not supported as key in maps, must be a non-nullable string." ) );
        verifyNoMoreInteractions( handler );
    }

    @Test
    public void shouldCallExternalErrorOnInitWithDuplicateKeys() throws Exception
    {
        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );
        channel = new EmbeddedChannel( newDecoder( handler ) );

        // Generate INIT message with duplicate keys
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = packerUnderTest.newPacker( out );
        packer.packStructHeader( 2, INIT.signature() );
        packer.pack( "Test/User Agent 1.0" );
        packer.packMapHeader( 3 );
        packer.pack( "scheme" );
        packer.pack( "basic" );
        packer.pack( "principal" );
        packer.pack( "user" );
        packer.pack( "scheme" );
        packer.pack( "password" );

        channel.writeInbound( Unpooled.wrappedBuffer( out.bytes() ) );
        channel.finishAndReleaseAll();

        verify( handler ).onExternalError( Neo4jError.from( Status.Request.Invalid, "Duplicate map key `scheme`." ) );
        verifyNoMoreInteractions( handler );
    }

    @Test
    public void shouldCallExternalErrorOnNodeParameter() throws Exception
    {
        testUnpackableStructParametersWithKnownType( ALICE, "Node values cannot be unpacked with this version of bolt." );
    }

    @Test
    public void shouldCallExternalErrorOnRelationshipParameter() throws Exception
    {
        testUnpackableStructParametersWithKnownType( ALICE_KNOWS_BOB, "Relationship values cannot be unpacked with this version of bolt." );
    }

    @Test
    public void shouldCallExternalErrorOnPathParameter() throws Exception
    {
        for ( PathValue path : ALL_PATHS )
        {
            testUnpackableStructParametersWithKnownType( path, "Path values cannot be unpacked with this version of bolt." );
        }
    }

    @Test
    public void shouldCallExternalErrorOnDuration() throws Exception
    {
        assumeThat( packerUnderTest.version(), equalTo( 1L ) );

        testUnpackableStructParametersWithKnownType( new Neo4jPackV2(), durationValue( Duration.ofDays( 10 ) ),
                "Duration values cannot be unpacked with this version of bolt." );
    }

    @Test
    public void shouldCallExternalErrorOnDate() throws Exception
    {
        assumeThat( packerUnderTest.version(), equalTo( 1L ) );

        testUnpackableStructParametersWithKnownType( new Neo4jPackV2(), ValueUtils.of( LocalDate.now() ),
                "LocalDate values cannot be unpacked with this version of bolt." );
    }

    @Test
    public void shouldCallExternalErrorOnLocalTime() throws Exception
    {
        assumeThat( packerUnderTest.version(), equalTo( 1L ) );

        testUnpackableStructParametersWithKnownType( new Neo4jPackV2(), ValueUtils.of( LocalTime.now() ),
                "LocalTime values cannot be unpacked with this version of bolt." );
    }

    @Test
    public void shouldCallExternalErrorOnTime() throws Exception
    {
        assumeThat( packerUnderTest.version(), equalTo( 1L ) );

        testUnpackableStructParametersWithKnownType( new Neo4jPackV2(), ValueUtils.of( OffsetTime.now() ),
                "OffsetTime values cannot be unpacked with this version of bolt." );
    }

    @Test
    public void shouldCallExternalErrorOnLocalDateTime() throws Exception
    {
        assumeThat( packerUnderTest.version(), equalTo( 1L ) );

        testUnpackableStructParametersWithKnownType( new Neo4jPackV2(), ValueUtils.of( LocalDateTime.now() ),
                "LocalDateTime values cannot be unpacked with this version of bolt." );
    }

    @Test
    public void shouldCallExternalErrorOnDateTimeWithOffset() throws Exception
    {
        assumeThat( packerUnderTest.version(), equalTo( 1L ) );

        testUnpackableStructParametersWithKnownType( new Neo4jPackV2(), ValueUtils.of( OffsetDateTime.now() ),
                "OffsetDateTime values cannot be unpacked with this version of bolt." );
    }

    @Test
    public void shouldCallExternalErrorOnDateTimeWithZoneName() throws Exception
    {
        assumeThat( packerUnderTest.version(), equalTo( 1L ) );

        testUnpackableStructParametersWithKnownType( new Neo4jPackV2(), ValueUtils.of( ZonedDateTime.now() ),
                "ZonedDateTime values cannot be unpacked with this version of bolt." );
    }

    @Test
    public void shouldThrowOnUnknownStructType() throws Exception
    {
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = packerUnderTest.newPacker( out );
        packer.packStructHeader( 2, RUN.signature() );
        packer.pack( "RETURN $x" );
        packer.packMapHeader( 1 );
        packer.pack( "x" );
        packer.packStructHeader( 0, (byte) 'A' );

        try
        {
            unpack( out.bytes() );
        }
        catch ( BoltIOException ex )
        {
            assertThat( ex.getMessage(), equalTo( "Struct types of 0x41 are not recognized." ) );
        }
    }

    @Test
    public void shouldLogContentOfTheMessageOnIOError() throws Exception
    {
        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );

        LogService logService = mock( LogService.class );
        Log log = mock( Log.class );
        when( logService.getInternalLog( MessageDecoder.class ) ).thenReturn( log );

        channel = new EmbeddedChannel( new MessageDecoder( packerUnderTest, handler, logService ) );

        byte invalidMessageSignature = Byte.MAX_VALUE;
        byte[] messageBytes = packMessageWithSignature( invalidMessageSignature );

        try
        {
            channel.writeInbound( Unpooled.wrappedBuffer( messageBytes ) );
            fail( "Exception expected" );
        }
        catch ( Exception ignore )
        {
        }

        assertMessageHexDumpLogged( log, messageBytes );
    }

    @Test
    public void shouldLogContentOfTheMessageOnError() throws Exception
    {
        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );
        RuntimeException error = new RuntimeException( "Hello!" );
        doThrow( error ).when( handler ).onRun( any(), any() );

        LogService logService = mock( LogService.class );
        Log log = mock( Log.class );
        when( logService.getInternalLog( MessageDecoder.class ) ).thenReturn( log );

        channel = new EmbeddedChannel( new MessageDecoder( packerUnderTest, handler, logService ) );

        byte[] messageBytes = packMessageWithSignature( RUN.signature() );

        try
        {
            channel.writeInbound( Unpooled.wrappedBuffer( messageBytes ) );
            fail( "Exception expected" );
        }
        catch ( RuntimeException e )
        {
            assertEquals( error, e );
        }

        assertMessageHexDumpLogged( log, messageBytes );
    }

    private void testUnpackableStructParametersWithKnownType( AnyValue parameterValue, String expectedMessage ) throws IOException
    {
        testUnpackableStructParametersWithKnownType( packerUnderTest, parameterValue, expectedMessage );
    }

    private void testUnpackableStructParametersWithKnownType( Neo4jPack packerForSerialization, AnyValue parameterValue, String expectedMessage )
            throws IOException
    {
        String statement = "RETURN $x";
        MapValue parameters = VirtualValues.map( Collections.singletonMap( "x", parameterValue ) );

        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );
        channel = new EmbeddedChannel( newDecoder( handler ) );

        channel.writeInbound( Unpooled.wrappedBuffer( serialize( packerForSerialization, run( statement, parameters ) ) ) );
        channel.finishAndReleaseAll();

        verify( handler ).onExternalError( Neo4jError.from( Status.Statement.TypeError, expectedMessage ) );
        verifyNoMoreInteractions( handler );
    }

    private void unpack( byte[] input ) throws IOException
    {
        BoltRequestMessageHandler handler = mock( BoltRequestMessageHandler.class );
        channel = new EmbeddedChannel( newDecoder( handler ) );

        channel.writeInbound( Unpooled.wrappedBuffer( input ) );
        channel.finishAndReleaseAll();
    }

    private byte[] packMessageWithSignature( byte signature ) throws IOException
    {
        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = packerUnderTest.newPacker( out );
        packer.packStructHeader( 2, signature );
        packer.pack( "RETURN 'Hello World!'" );
        packer.pack( EMPTY_MAP );
        return out.bytes();
    }

    private MessageDecoder newDecoder( BoltRequestMessageHandler handler )
    {
        return new MessageDecoder( packerUnderTest, handler, NullLogService.getInstance() );
    }

    private static void assertMessageHexDumpLogged( Log logMock, byte[] messageBytes )
    {
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass( String.class );
        verify( logMock ).error( captor.capture() );
        assertThat( captor.getValue(), containsString( hexDump( messageBytes ) ) );
    }
}
