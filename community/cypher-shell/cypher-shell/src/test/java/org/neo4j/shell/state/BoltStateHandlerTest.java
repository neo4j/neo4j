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
package org.neo4j.shell.state;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthToken;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.summary.ServerInfo;
import org.neo4j.shell.ConnectionConfig;
import org.neo4j.shell.TriFunction;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.test.bolt.FakeDriver;
import org.neo4j.shell.test.bolt.FakeSession;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BoltStateHandlerTest
{
    @Rule
    public final ExpectedException thrown = ExpectedException.none();
    private final Driver mockDriver = mock( Driver.class );
    private Logger logger = mock( Logger.class );
    private OfflineBoltStateHandler boltStateHandler = new OfflineBoltStateHandler( mockDriver );

    @Before
    public void setup()
    {
        when( mockDriver.session( any(), nullable(String.class) ) ).thenReturn( new FakeSession() );
        doReturn( System.out ).when( logger ).getOutputStream();
    }

    @Test
    public void versionIsEmptyBeforeConnect() throws CommandException
    {
        assertFalse( boltStateHandler.isConnected() );
        assertEquals( "", boltStateHandler.getServerVersion() );
    }

    @Test
    public void versionIsEmptyIfDriverReturnsNull() throws CommandException
    {
        RecordingDriverProvider provider = new RecordingDriverProvider()
        {
            @Override
            public Driver apply( String uri, AuthToken authToken, Config config )
            {
                super.apply( uri, authToken, config );
                return new FakeDriver()
                {
                    @Override
                    public Session session( AccessMode accessMode, String bookmark )
                    {
                        return new FakeSession();
                    }
                };
            }
        };
        BoltStateHandler handler = new BoltStateHandler( provider );
        ConnectionConfig config = new ConnectionConfig( "bolt://", "", -1, "", "", false );
        handler.connect( config );

        assertEquals( "", handler.getServerVersion() );
    }

    @Test
    public void versionIsNotEmptyAfterConnect() throws CommandException
    {
        Driver driverMock = stubVersionInAnOpenSession( mock( StatementResult.class ), mock( Session.class ), "Neo4j/9.4.1-ALPHA" );

        BoltStateHandler handler = new BoltStateHandler( ( s, authToken, config ) -> driverMock );
        ConnectionConfig config = new ConnectionConfig( "bolt://", "", -1, "", "", false );
        handler.connect( config );

        assertEquals( "9.4.1-ALPHA", handler.getServerVersion() );
    }

    @Test
    public void closeTransactionAfterRollback() throws CommandException
    {
        boltStateHandler.connect();
        boltStateHandler.beginTransaction();

        assertNotNull( boltStateHandler.getTransactionStatements() );

        boltStateHandler.rollbackTransaction();

        assertNull( boltStateHandler.getTransactionStatements() );
    }

    @Test
    public void exceptionsFromSilentDisconnectAreSuppressedToReportOriginalErrors() throws CommandException
    {
        Session session = mock( Session.class );
        StatementResult resultMock = mock( StatementResult.class );

        RuntimeException originalException = new RuntimeException( "original exception" );
        RuntimeException thrownFromSilentDisconnect = new RuntimeException( "exception from silent disconnect" );

        Driver mockedDriver = stubVersionInAnOpenSession( resultMock, session, "neo4j-version" );
        OfflineBoltStateHandler boltStateHandler = new OfflineBoltStateHandler( mockedDriver );

        when( resultMock.consume() ).thenThrow( originalException );
        doThrow( thrownFromSilentDisconnect ).when( session ).close();

        try
        {
            boltStateHandler.connect();
            fail( "should fail on silent disconnect" );
        }
        catch ( Exception e )
        {
            assertThat( e.getSuppressed()[0], is( thrownFromSilentDisconnect ) );
            assertThat( e, is( originalException ) );
        }
    }

    @Test
    public void closeTransactionAfterCommit() throws CommandException
    {
        boltStateHandler.connect();
        boltStateHandler.beginTransaction();
        assertNotNull( boltStateHandler.getTransactionStatements() );

        boltStateHandler.commitTransaction();

        assertNull( boltStateHandler.getTransactionStatements() );
    }

    @Test
    public void beginNeedsToBeConnected() throws CommandException
    {
        thrown.expect( CommandException.class );
        thrown.expectMessage( "Not connected to Neo4j" );

        assertFalse( boltStateHandler.isConnected() );

        boltStateHandler.beginTransaction();
    }

    @Test
    public void commitNeedsToBeConnected() throws CommandException
    {
        thrown.expect( CommandException.class );
        thrown.expectMessage( "Not connected to Neo4j" );

        assertFalse( boltStateHandler.isConnected() );

        boltStateHandler.commitTransaction();
    }

    @Test
    public void beginNeedsToInitialiseTransactionStatements() throws CommandException
    {
        boltStateHandler.connect();

        boltStateHandler.beginTransaction();
        assertNotNull( boltStateHandler.getTransactionStatements() );
    }

    @Test
    public void commitPurgesTheTransactionStatementsAndCollectsResults() throws CommandException
    {
        Session sessionMock = mock( Session.class );
        Driver driverMock = stubVersionInAnOpenSession( mock( StatementResult.class ), sessionMock, "neo4j-version" );

        Record record1 = mock( Record.class );
        Record record2 = mock( Record.class );
        Record record3 = mock( Record.class );
        Value val1 = mock( Value.class );
        Value val2 = mock( Value.class );
        Value str1Val = mock( Value.class );
        Value str2Val = mock( Value.class );

        BoltResult boltResult1 = new ListBoltResult( asList( record1, record2 ), mock( ResultSummary.class ) );
        BoltResult boltResult2 = new ListBoltResult( asList( record3 ), mock( ResultSummary.class ) );

        when( str1Val.toString() ).thenReturn( "str1" );
        when( str2Val.toString() ).thenReturn( "str2" );
        when( val1.toString() ).thenReturn( "1" );
        when( val2.toString() ).thenReturn( "2" );

        when( record1.get( 0 ) ).thenReturn( val1 );
        when( record2.get( 0 ) ).thenReturn( val2 );

        when( record3.get( 0 ) ).thenReturn( str1Val );
        when( record3.get( 1 ) ).thenReturn( str2Val );
        when( sessionMock.writeTransaction( anyObject() ) ).thenReturn( asList( boltResult1, boltResult2 ) );

        OfflineBoltStateHandler boltStateHandler = new OfflineBoltStateHandler( driverMock );
        boltStateHandler.connect();
        boltStateHandler.beginTransaction();
        boltStateHandler.runCypher( "UNWIND [1,2] as num RETURN *", Collections.emptyMap() );
        boltStateHandler.runCypher( "RETURN \"str1\", \"str2\"", Collections.emptyMap() );

        Optional<List<BoltResult>> boltResultOptional = boltStateHandler.commitTransaction();
        List<BoltResult> boltResults = boltResultOptional.get();
        Record actualRecord1 = boltResults.get( 0 ).getRecords().get( 0 );
        Record actualRecord2 = boltResults.get( 0 ).getRecords().get( 1 );

        Record actualRecord3 = boltResults.get( 1 ).getRecords().get( 0 );

        assertNull( boltStateHandler.getTransactionStatements() );

        assertEquals( "1", actualRecord1.get( 0 ).toString() );
        assertEquals( "2", actualRecord2.get( 0 ).toString() );

        assertEquals( "str1", actualRecord3.get( 0 ).toString() );
        assertEquals( "str2", actualRecord3.get( 1 ).toString() );
    }

    @Test
    public void rollbackNeedsToBeConnected() throws CommandException
    {
        thrown.expect( CommandException.class );
        thrown.expectMessage( "Not connected to Neo4j" );

        assertFalse( boltStateHandler.isConnected() );

        boltStateHandler.rollbackTransaction();
    }

    @Test
    public void executeNeedsToBeConnected() throws CommandException
    {
        thrown.expect( CommandException.class );
        thrown.expectMessage( "Not connected to Neo4j" );

        boltStateHandler.runCypher( "", Collections.emptyMap() );
    }

    @Test
    public void shouldExecuteInTransactionIfOpen() throws CommandException
    {
        boltStateHandler.connect();
        boltStateHandler.beginTransaction();

        assertNotNull( "Expected a transaction", boltStateHandler.getTransactionStatements() );
    }

    @Test
    public void shouldRunCypherQuery() throws CommandException
    {
        Session sessionMock = mock( Session.class );
        StatementResult versionMock = mock( StatementResult.class );
        StatementResult resultMock = mock( StatementResult.class );
        Record recordMock = mock( Record.class );
        Value valueMock = mock( Value.class );

        Driver driverMock = stubVersionInAnOpenSession( versionMock, sessionMock, "neo4j-version" );

        when( resultMock.list() ).thenReturn( asList( recordMock ) );

        when( valueMock.toString() ).thenReturn( "999" );
        when( recordMock.get( 0 ) ).thenReturn( valueMock );
        when( sessionMock.run( any( Statement.class ) ) ).thenReturn( resultMock );

        OfflineBoltStateHandler boltStateHandler = new OfflineBoltStateHandler( driverMock );

        boltStateHandler.connect();

        BoltResult boltResult = boltStateHandler.runCypher( "RETURN 999",
                                                            new HashMap<>() ).get();
        verify( sessionMock ).run( any( Statement.class ) );

        assertEquals( "999", boltResult.getRecords().get( 0 ).get( 0 ).toString() );
    }

    @Test
    public void triesAgainOnSessionExpired() throws Exception
    {
        Session sessionMock = mock( Session.class );
        StatementResult versionMock = mock( StatementResult.class );
        StatementResult resultMock = mock( StatementResult.class );
        Record recordMock = mock( Record.class );
        Value valueMock = mock( Value.class );

        Driver driverMock = stubVersionInAnOpenSession( versionMock, sessionMock, "neo4j-version" );

        when( resultMock.list() ).thenReturn( asList( recordMock ) );

        when( valueMock.toString() ).thenReturn( "999" );
        when( recordMock.get( 0 ) ).thenReturn( valueMock );
        when( sessionMock.run( any( Statement.class ) ) )
                .thenThrow( new SessionExpiredException( "leaderswitch" ) )
                .thenReturn( resultMock );

        OfflineBoltStateHandler boltStateHandler = new OfflineBoltStateHandler( driverMock );

        boltStateHandler.connect();
        BoltResult boltResult = boltStateHandler.runCypher( "RETURN 999",
                                                            new HashMap<>() ).get();

        verify( driverMock, times( 2 ) ).session( any(), nullable( String.class ) );
        verify( sessionMock, times( 2 ) ).run( any( Statement.class ) );

        assertEquals( "999", boltResult.getRecords().get( 0 ).get( 0 ).toString() );
    }

    @Test
    public void shouldExecuteInSessionByDefault() throws CommandException
    {
        boltStateHandler.connect();

        assertNull( "Did not expect a transaction", boltStateHandler.getTransactionStatements() );
    }

    @Test
    public void canOnlyConnectOnce() throws CommandException
    {
        thrown.expect( CommandException.class );
        thrown.expectMessage( "Already connected" );

        try
        {
            boltStateHandler.connect();
        }
        catch ( Throwable e )
        {
            System.out.println( ExceptionUtils.getStackTrace( e ) );
            fail( "Should not throw here: " + e );
        }

        boltStateHandler.connect();
    }

    @Test
    public void resetSessionOnReset() throws Exception
    {
        // given
        Session sessionMock = mock( Session.class );
        Driver driverMock = stubVersionInAnOpenSession( mock( StatementResult.class ), sessionMock, "neo4j-version" );

        OfflineBoltStateHandler boltStateHandler = new OfflineBoltStateHandler( driverMock );

        boltStateHandler.connect();
        boltStateHandler.beginTransaction();

        // when
        boltStateHandler.reset();

        // then
        verify( sessionMock ).reset();
        assertNull( boltStateHandler.getTransactionStatements() );
    }

    @Test
    public void silentDisconnectCleansUp() throws Exception
    {
        // given
        boltStateHandler.connect();

        Session session = boltStateHandler.session;
        assertNotNull( session );
        assertNotNull( boltStateHandler.driver );

        assertTrue( boltStateHandler.session.isOpen() );

        // when
        boltStateHandler.silentDisconnect();

        // then
        assertFalse( session.isOpen() );
    }

    @Test
    public void turnOffEncryptionIfRequested() throws CommandException
    {
        RecordingDriverProvider provider = new RecordingDriverProvider();
        BoltStateHandler handler = new BoltStateHandler( provider );
        ConnectionConfig config = new ConnectionConfig( "bolt://", "", -1, "", "", false );
        handler.connect( config );
        assertEquals( Config.EncryptionLevel.NONE, provider.config.encryptionLevel() );
    }

    @Test
    public void turnOnEncryptionIfRequested() throws CommandException
    {
        RecordingDriverProvider provider = new RecordingDriverProvider();
        BoltStateHandler handler = new BoltStateHandler( provider );
        ConnectionConfig config = new ConnectionConfig( "bolt://", "", -1, "", "", true );
        handler.connect( config );
        assertEquals( Config.EncryptionLevel.REQUIRED, provider.config.encryptionLevel() );
    }

    private Driver stubVersionInAnOpenSession( StatementResult versionMock, Session sessionMock, String value )
    {
        Driver driverMock = mock( Driver.class );
        ResultSummary resultSummary = mock( ResultSummary.class );
        ServerInfo serverInfo = mock( ServerInfo.class );

        when( resultSummary.server() ).thenReturn( serverInfo );
        when( serverInfo.version() ).thenReturn( value );
        when( versionMock.summary() ).thenReturn( resultSummary );

        when( sessionMock.isOpen() ).thenReturn( true );
        when( sessionMock.run( "RETURN 1" ) ).thenReturn( versionMock );
        when( driverMock.session( any(), nullable(String.class) ) ).thenReturn( sessionMock );

        return driverMock;
    }

    /**
     * Bolt state with faked bolt interactions
     */
    private static class OfflineBoltStateHandler extends BoltStateHandler
    {

        OfflineBoltStateHandler( Driver driver )
        {
            super( ( uri, authToken, config ) -> driver );
        }

        public void connect() throws CommandException
        {
            connect( new ConnectionConfig( "bolt://", "", 1, "", "", false ) );
        }
    }

    private class RecordingDriverProvider implements TriFunction<String, AuthToken, Config, Driver>
    {
        public Config config;

        @Override
        public Driver apply( String uri, AuthToken authToken, Config config )
        {
            this.config = config;
            return new FakeDriver();
        }
    }
}
