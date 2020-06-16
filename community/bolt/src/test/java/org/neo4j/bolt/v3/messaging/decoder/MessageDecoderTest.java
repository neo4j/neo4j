/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.v3.messaging.decoder;

import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.ZonedDateTime;
import java.util.stream.Stream;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.Neo4jPackV1;
import org.neo4j.bolt.packstream.Neo4jPackV2;
import org.neo4j.bolt.packstream.PackedOutputArray;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.runtime.Neo4jError;
import org.neo4j.bolt.runtime.SynchronousBoltConnection;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.transport.pipeline.MessageDecoder;
import org.neo4j.bolt.v3.messaging.BoltRequestMessageReaderV3;
import org.neo4j.bolt.v3.messaging.request.ResetMessage;
import org.neo4j.bolt.v3.messaging.request.RunMessage;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.PathValue;
import org.neo4j.values.virtual.VirtualValues;

import static io.netty.buffer.ByteBufUtil.hexDump;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.bolt.packstream.example.Edges.ALICE_KNOWS_BOB;
import static org.neo4j.bolt.packstream.example.Nodes.ALICE;
import static org.neo4j.bolt.packstream.example.Paths.ALL_PATHS;
import static org.neo4j.bolt.testing.MessageConditions.serialize;
import static org.neo4j.values.storable.Values.durationValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

public class MessageDecoderTest
{
    private EmbeddedChannel channel;

    public Neo4jPack packerUnderTest;

    protected static Stream<Arguments> argumentsProvider()
    {
        return Stream.of( Arguments.of( new Neo4jPackV1() ), Arguments.of( new Neo4jPackV2() ) );
    }

    @AfterEach
    public void cleanup()
    {
        if ( channel != null )
        {
            channel.finishAndReleaseAll();
        }
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldDispatchRequestMessage( Neo4jPack packerUnderTest ) throws Exception
    {
        this.packerUnderTest = packerUnderTest;

        BoltStateMachine stateMachine = mock( BoltStateMachine.class );
        SynchronousBoltConnection connection = new SynchronousBoltConnection( stateMachine );
        channel = new EmbeddedChannel( newDecoder( connection ) );

        channel.writeInbound( Unpooled.wrappedBuffer( serialize( packerUnderTest, ResetMessage.INSTANCE ) ) );
        channel.finishAndReleaseAll();

        verify( stateMachine ).process( eq( ResetMessage.INSTANCE ), any() );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldCallExternalErrorOnNodeParameter( Neo4jPack packerUnderTest ) throws Exception
    {
        this.packerUnderTest = packerUnderTest;

        testUnpackableStructParametersWithKnownType( ALICE, "Node values cannot be unpacked with this version of bolt." );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldCallExternalErrorOnRelationshipParameter( Neo4jPack packerUnderTest ) throws Exception
    {
        this.packerUnderTest = packerUnderTest;

        testUnpackableStructParametersWithKnownType( ALICE_KNOWS_BOB, "Relationship values cannot be unpacked with this version of bolt." );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldCallExternalErrorOnPathParameter( Neo4jPack packerUnderTest ) throws Exception
    {
        this.packerUnderTest = packerUnderTest;

        for ( PathValue path : ALL_PATHS )
        {
            testUnpackableStructParametersWithKnownType( path, "Path values cannot be unpacked with this version of bolt." );
        }
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldCallExternalErrorOnDuration( Neo4jPack packerUnderTest ) throws Exception
    {
        this.packerUnderTest = packerUnderTest;

        assumeThat( packerUnderTest.version() ).isEqualTo( 1 );

        testUnpackableStructParametersWithKnownType( new Neo4jPackV2(), durationValue( Duration.ofDays( 10 ) ),
                "Duration values cannot be unpacked with this version of bolt." );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldCallExternalErrorOnDate( Neo4jPack packerUnderTest ) throws Exception
    {
        this.packerUnderTest = packerUnderTest;

        assumeThat( packerUnderTest.version() ).isEqualTo( 1 );

        testUnpackableStructParametersWithKnownType( new Neo4jPackV2(), ValueUtils.of( LocalDate.now() ),
                "LocalDate values cannot be unpacked with this version of bolt." );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldCallExternalErrorOnLocalTime( Neo4jPack packerUnderTest ) throws Exception
    {
        this.packerUnderTest = packerUnderTest;

        assumeThat( packerUnderTest.version() ).isEqualTo( 1 );

        testUnpackableStructParametersWithKnownType( new Neo4jPackV2(), ValueUtils.of( LocalTime.now() ),
                "LocalTime values cannot be unpacked with this version of bolt." );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldCallExternalErrorOnTime( Neo4jPack packerUnderTest ) throws Exception
    {
        this.packerUnderTest = packerUnderTest;

        assumeThat( packerUnderTest.version() ).isEqualTo( 1 );

        testUnpackableStructParametersWithKnownType( new Neo4jPackV2(), ValueUtils.of( OffsetTime.now() ),
                "OffsetTime values cannot be unpacked with this version of bolt." );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldCallExternalErrorOnLocalDateTime( Neo4jPack packerUnderTest ) throws Exception
    {
        this.packerUnderTest = packerUnderTest;

        assumeThat( packerUnderTest.version() ).isEqualTo( 1 );

        testUnpackableStructParametersWithKnownType( new Neo4jPackV2(), ValueUtils.of( LocalDateTime.now() ),
                "LocalDateTime values cannot be unpacked with this version of bolt." );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldCallExternalErrorOnDateTimeWithOffset( Neo4jPack packerUnderTest ) throws Exception
    {
        this.packerUnderTest = packerUnderTest;

        assumeThat( packerUnderTest.version() ).isEqualTo( 1 );

        testUnpackableStructParametersWithKnownType( new Neo4jPackV2(), ValueUtils.of( OffsetDateTime.now() ),
                "OffsetDateTime values cannot be unpacked with this version of bolt." );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldCallExternalErrorOnDateTimeWithZoneName( Neo4jPack packerUnderTest ) throws Exception
    {
        this.packerUnderTest = packerUnderTest;

        assumeThat( packerUnderTest.version() ).isEqualTo( 1 );

        testUnpackableStructParametersWithKnownType( new Neo4jPackV2(), ValueUtils.of( ZonedDateTime.now() ),
                "ZonedDateTime values cannot be unpacked with this version of bolt." );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldThrowOnUnknownStructType( Neo4jPack packerUnderTest ) throws Exception
    {
        this.packerUnderTest = packerUnderTest;

        PackedOutputArray out = new PackedOutputArray();
        Neo4jPack.Packer packer = packerUnderTest.newPacker( out );
        packer.packStructHeader( 2, RunMessage.SIGNATURE );
        packer.pack( "RETURN $x" );
        packer.packMapHeader( 1 );
        packer.pack( "x" );
        packer.packStructHeader( 0, (byte) 'A' );

        var ex = assertThrows( BoltIOException.class, () -> unpack( out.bytes() ));
        assertEquals( "Struct types of 0x41 are not recognized.", ex.getMessage() );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldLogContentOfTheMessageOnIOError( Neo4jPack packerUnderTest ) throws Exception
    {
        this.packerUnderTest = packerUnderTest;

        BoltConnection connection = mock( BoltConnection.class );
        BoltResponseMessageWriter responseMessageHandler = mock( BoltResponseMessageWriter.class );

        BoltRequestMessageReader requestMessageReader = new BoltRequestMessageReaderV3( connection, responseMessageHandler, NullLogService.getInstance() );

        LogService logService = mock( LogService.class );
        Log log = mock( Log.class );
        when( logService.getInternalLog( MessageDecoder.class ) ).thenReturn( log );

        channel = new EmbeddedChannel( new MessageDecoder( packerUnderTest::newUnpacker, requestMessageReader, logService ) );

        byte invalidMessageSignature = Byte.MAX_VALUE;
        byte[] messageBytes = packMessageWithSignature( invalidMessageSignature );

        assertThrows( BoltIOException.class, () -> channel.writeInbound( Unpooled.wrappedBuffer( messageBytes ) ) );

        assertMessageHexDumpLogged( log, messageBytes );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldLogContentOfTheMessageOnError( Neo4jPack packerUnderTest ) throws Exception
    {
        this.packerUnderTest = packerUnderTest;

        BoltRequestMessageReader requestMessageReader = mock( BoltRequestMessageReader.class );
        RuntimeException error = new RuntimeException( "Hello!" );
        doThrow( error ).when( requestMessageReader ).read( any() );

        LogService logService = mock( LogService.class );
        Log log = mock( Log.class );
        when( logService.getInternalLog( MessageDecoder.class ) ).thenReturn( log );

        channel = new EmbeddedChannel( new MessageDecoder( packerUnderTest::newUnpacker, requestMessageReader, logService ) );

        byte[] messageBytes = packMessageWithSignature( RunMessage.SIGNATURE );

        var e = assertThrows( RuntimeException.class, () -> channel.writeInbound( Unpooled.wrappedBuffer( messageBytes ) ) );
        assertEquals( error, e );

        assertMessageHexDumpLogged( log, messageBytes );
    }

    private void testUnpackableStructParametersWithKnownType( AnyValue parameterValue, String expectedMessage ) throws Exception
    {
        testUnpackableStructParametersWithKnownType( packerUnderTest, parameterValue, expectedMessage );
    }

    private void testUnpackableStructParametersWithKnownType( Neo4jPack packerForSerialization, AnyValue parameterValue, String expectedMessage )
            throws Exception
    {
        String statement = "RETURN $x";
        MapValue parameters = VirtualValues.map( new String[]{"x"}, new AnyValue[]{parameterValue } );

        BoltStateMachine stateMachine = mock( BoltStateMachine.class );
        SynchronousBoltConnection connection = new SynchronousBoltConnection( stateMachine );
        channel = new EmbeddedChannel( newDecoder( connection ) );

        channel.writeInbound( Unpooled.wrappedBuffer( serialize( packerForSerialization, new RunMessage( statement, parameters ) ) ) );
        channel.finishAndReleaseAll();

        verify( stateMachine ).handleExternalFailure( eq( Neo4jError.from( Status.Statement.TypeError, expectedMessage ) ), any() );
    }

    private void unpack( byte[] input )
    {
        BoltStateMachine stateMachine = mock( BoltStateMachine.class );
        SynchronousBoltConnection connection = new SynchronousBoltConnection( stateMachine );
        channel = new EmbeddedChannel( newDecoder( connection ) );

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

    private MessageDecoder newDecoder( BoltConnection connection )
    {
        BoltRequestMessageReader reader = new BoltRequestMessageReaderV3( connection, mock( BoltResponseMessageWriter.class ), NullLogService.getInstance() );
        return new MessageDecoder( packerUnderTest::newUnpacker, reader, NullLogService.getInstance() );
    }

    private static void assertMessageHexDumpLogged( Log logMock, byte[] messageBytes )
    {
        ArgumentCaptor<String> captor = ArgumentCaptor.forClass( String.class );
        verify( logMock ).error( captor.capture() );
        assertThat( captor.getValue() ).contains( hexDump( messageBytes ) );
    }
}
