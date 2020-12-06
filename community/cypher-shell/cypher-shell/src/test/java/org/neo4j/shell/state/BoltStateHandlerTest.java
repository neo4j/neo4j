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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

import org.neo4j.driver.AccessMode;
import org.neo4j.driver.AuthToken;
import org.neo4j.driver.Bookmark;
import org.neo4j.driver.Config;
import org.neo4j.driver.Driver;
import org.neo4j.driver.Query;
import org.neo4j.driver.Record;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.summary.DatabaseInfo;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.ServerInfo;
import org.neo4j.shell.ConnectionConfig;
import org.neo4j.shell.TriFunction;
import org.neo4j.shell.cli.Encryption;
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
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.anyObject;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;
import static org.neo4j.shell.DatabaseManager.DEFAULT_DEFAULT_DB_NAME;
import static org.neo4j.shell.DatabaseManager.SYSTEM_DB_NAME;

public class BoltStateHandlerTest
{
    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    private final Logger logger = mock( Logger.class );
    private final Driver mockDriver = mock( Driver.class );
    private final OfflineBoltStateHandler boltStateHandler = new OfflineBoltStateHandler( mockDriver );
    private final ConnectionConfig config = new ConnectionConfig( "bolt", "", -1, "", "", Encryption.DEFAULT, ABSENT_DB_NAME );

    @Before
    public void setup()
    {
        when( mockDriver.session( any() ) ).thenReturn( new FakeSession() );
        doReturn( System.out ).when( logger ).getOutputStream();
    }

    @Test
    public void versionIsEmptyBeforeConnect()
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
                return new FakeDriver();
            }
        };
        BoltStateHandler handler = new BoltStateHandler( provider, false );
        handler.connect( config );

        assertEquals( "", handler.getServerVersion() );
    }

    @Test
    public void versionIsNotEmptyAfterConnect() throws CommandException
    {
        Driver driverMock = stubResultSummaryInAnOpenSession( mock( Result.class ), mock( Session.class ), "Neo4j/9.4.1-ALPHA" );

        BoltStateHandler handler = new BoltStateHandler( ( s, authToken, config ) -> driverMock, false );
        handler.connect( config );

        assertEquals( "9.4.1-ALPHA", handler.getServerVersion() );
    }

    @Test
    public void actualDatabaseNameIsNotEmptyAfterConnect() throws CommandException
    {
        Driver driverMock =
                stubResultSummaryInAnOpenSession( mock( Result.class ), mock( Session.class ), "Neo4j/9.4.1-ALPHA", "my_default_db" );

        BoltStateHandler handler = new BoltStateHandler( ( s, authToken, config ) -> driverMock, false );
        handler.connect( config );

        assertEquals( "my_default_db", handler.getActualDatabaseAsReportedByServer() );
    }

    @Test
    public void exceptionFromRunQueryDoesNotResetActualDatabaseNameToUnresolved() throws CommandException
    {
        Session sessionMock = mock( Session.class );
        Result resultMock = mock( Result.class );
        Driver driverMock =
                stubResultSummaryInAnOpenSession( resultMock, sessionMock, "Neo4j/9.4.1-ALPHA", "my_default_db" );

        ClientException databaseNotFound = new ClientException( "Neo.ClientError.Database.DatabaseNotFound", "blah" );

        when( sessionMock.run( any( Query.class ) ) )
                .thenThrow( databaseNotFound )
                .thenReturn( resultMock );

        BoltStateHandler handler = new BoltStateHandler( ( s, authToken, config ) -> driverMock, false );
        handler.connect( config );

        try
        {
            handler.runCypher( "RETURN \"hello\"", Collections.emptyMap() );
            fail( "should fail on runCypher" );
        }
        catch ( Exception e )
        {
            assertThat( e, is( databaseNotFound ) );
            assertEquals( "my_default_db", handler.getActualDatabaseAsReportedByServer() );
        }
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
        Result resultMock = mock( Result.class );

        RuntimeException originalException = new RuntimeException( "original exception" );
        RuntimeException thrownFromSilentDisconnect = new RuntimeException( "exception from silent disconnect" );

        Driver mockedDriver = stubResultSummaryInAnOpenSession( resultMock, session, "neo4j-version" );
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
        Driver driverMock = stubResultSummaryInAnOpenSession( mock( Result.class ), sessionMock, "neo4j-version" );

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
        Result resultMock = mock( Result.class );
        Record recordMock = mock( Record.class );
        Value valueMock = mock( Value.class );

        Driver driverMock = stubResultSummaryInAnOpenSession( resultMock, sessionMock, "neo4j-version" );

        when( resultMock.list() ).thenReturn( asList( recordMock ) );

        when( valueMock.toString() ).thenReturn( "999" );
        when( recordMock.get( 0 ) ).thenReturn( valueMock );
        when( sessionMock.run( any( Query.class ) ) ).thenReturn( resultMock );

        OfflineBoltStateHandler boltStateHandler = new OfflineBoltStateHandler( driverMock );

        boltStateHandler.connect();

        BoltResult boltResult = boltStateHandler.runCypher( "RETURN 999",
                                                            new HashMap<>() ).get();
        verify( sessionMock ).run( any( Query.class ) );

        assertEquals( "999", boltResult.getRecords().get( 0 ).get( 0 ).toString() );
    }

    @Test
    public void triesAgainOnSessionExpired() throws Exception
    {
        Session sessionMock = mock( Session.class );
        Result resultMock = mock( Result.class );
        Record recordMock = mock( Record.class );
        Value valueMock = mock( Value.class );

        Driver driverMock = stubResultSummaryInAnOpenSession( resultMock, sessionMock, "neo4j-version" );

        when( resultMock.list() ).thenReturn( asList( recordMock ) );

        when( valueMock.toString() ).thenReturn( "999" );
        when( recordMock.get( 0 ) ).thenReturn( valueMock );
        when( sessionMock.run( any( Query.class ) ) )
                .thenThrow( new SessionExpiredException( "leaderswitch" ) )
                .thenReturn( resultMock );

        OfflineBoltStateHandler boltStateHandler = new OfflineBoltStateHandler( driverMock );

        boltStateHandler.connect();
        BoltResult boltResult = boltStateHandler.runCypher( "RETURN 999",
                                                            new HashMap<>() ).get();

        verify( driverMock, times( 2 ) ).session( any() );
        verify( sessionMock, times( 2 ) ).run( any( Query.class ) );

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
            fail( "Should not throw here: " + e );
        }

        boltStateHandler.connect();
    }

    @Test
    public void resetSessionOnReset() throws Exception
    {
        // given
        Session sessionMock = mock( Session.class );
        Driver driverMock = stubResultSummaryInAnOpenSession( mock( Result.class ), sessionMock, "neo4j-version" );

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
        BoltStateHandler handler = new BoltStateHandler( provider, false );

        handler.connect( config );
        assertFalse( provider.config.encrypted() );
    }

    @Test
    public void turnOnEncryptionIfRequested() throws CommandException
    {
        RecordingDriverProvider provider = new RecordingDriverProvider();
        BoltStateHandler handler = new BoltStateHandler( provider, false );
        ConnectionConfig config = new ConnectionConfig( "bolt", "", -1, "", "", Encryption.TRUE, ABSENT_DB_NAME );
        handler.connect( config );
        assertTrue( provider.config.encrypted() );
    }

    @Test
    public void fallbackToBolt() throws CommandException
    {
        final String[] uriScheme = new String[1];
        RecordingDriverProvider provider = new RecordingDriverProvider()
        {
            @Override
            public Driver apply( String uri, AuthToken authToken, Config config )
            {
                uriScheme[0] = uri.substring( 0, uri.indexOf( ':' ) );
                if ( uriScheme[0].equals( "neo4j" ) )
                {
                    throw new org.neo4j.driver.exceptions.ServiceUnavailableException( "Please fall back" );
                }
                super.apply( uri, authToken, config );
                return new FakeDriver();
            }
        };
        BoltStateHandler handler = new BoltStateHandler( provider, false );
        ConnectionConfig config = new ConnectionConfig( "neo4j", "", -1, "", "", Encryption.DEFAULT, ABSENT_DB_NAME );
        handler.connect( config );

        assertEquals( "bolt", uriScheme[0] );
    }

    @Test
    public void fallbackToBoltSSC() throws CommandException
    {
        final String[] uriScheme = new String[1];
        RecordingDriverProvider provider = new RecordingDriverProvider()
        {
            @Override
            public Driver apply( String uri, AuthToken authToken, Config config )
            {
                uriScheme[0] = uri.substring( 0, uri.indexOf( ':' ) );
                if ( uriScheme[0].equals( "neo4j+ssc" ) )
                {
                    throw new org.neo4j.driver.exceptions.ServiceUnavailableException( "Please fall back" );
                }
                super.apply( uri, authToken, config );
                return new FakeDriver();
            }
        };
        BoltStateHandler handler = new BoltStateHandler( provider, false );
        ConnectionConfig config = new ConnectionConfig( "neo4j+ssc", "", -1, "", "", Encryption.DEFAULT, ABSENT_DB_NAME );
        handler.connect( config );

        assertEquals( "bolt+ssc", uriScheme[0] );
    }

    @Test
    public void shouldChangePasswordAndKeepSystemDbBookmark() throws CommandException
    {
        // Given
        ConnectionConfig config = new ConnectionConfig( "bolt", "", -1, "", "", Encryption.DEFAULT, ABSENT_DB_NAME );
        config.setNewPassword( "newPW" );
        Bookmark bookmark = InternalBookmark.parse( "myBookmark" );

        Session sessionMock = mock( Session.class );
        Result resultMock = mock( Result.class );
        Driver driverMock = stubResultSummaryInAnOpenSession( resultMock, sessionMock, "Neo4j/9.4.1-ALPHA", "my_default_db" );
        when( sessionMock.run( "CALL dbms.security.changePassword($n)", Values.parameters( "n", config.newPassword() ) ) ).thenReturn( resultMock );
        when( sessionMock.lastBookmark() ).thenReturn( bookmark );
        BoltStateHandler handler = new OfflineBoltStateHandler( driverMock );

        // When
        handler.changePassword( config );

        // Then
        assertEquals( "newPW", config.password() );
        assertNull( config.newPassword() );
        assertNull( handler.session );

        // When connecting to system db again
        handler.connect( new ConnectionConfig( "bolt", "", -1, "", "", Encryption.DEFAULT, SYSTEM_DB_NAME ) );

        // Then use bookmark for system DB
        verify( driverMock ).session( SessionConfig.builder()
                                                   .withDefaultAccessMode( AccessMode.WRITE )
                                                   .withDatabase( SYSTEM_DB_NAME )
                                                   .withBookmarks( bookmark )
                                                   .build() );
    }

    @SuppressWarnings( "OptionalGetWithoutIsPresent" )
    @Test
    public void shouldKeepOneBookmarkPerDatabase() throws CommandException
    {
        ConnectionConfig config = new ConnectionConfig( "bolt", "", -1, "", "", Encryption.DEFAULT, "database1" );
        Bookmark db1Bookmark = InternalBookmark.parse( "db1" );
        Bookmark db2Bookmark = InternalBookmark.parse( "db2" );

        // A couple of these mock calls are now redundant with what is called in stubResultSummaryInAnOpenSession
        Result resultMock = mock( Result.class );
        Session db1SessionMock = mock( Session.class );
        when( db1SessionMock.isOpen() ).thenReturn( true );
        when( db1SessionMock.lastBookmark() ).thenReturn( db1Bookmark );
        when( db1SessionMock.run( "RETURN 1" ) ).thenReturn( resultMock );
        Session db2SessionMock = mock( Session.class );
        when( db2SessionMock.isOpen() ).thenReturn( true );
        when( db2SessionMock.lastBookmark() ).thenReturn( db2Bookmark );
        when( db2SessionMock.run( "RETURN 1" ) ).thenReturn( resultMock );

        Driver driverMock = stubResultSummaryInAnOpenSession( resultMock, db1SessionMock, "Neo4j/9.4.1-ALPHA", "database1" );
        when( driverMock.session( any() ) ).thenAnswer( arg ->
                                                        {
                                                            SessionConfig sc = (SessionConfig) arg.getArguments()[0];
                                                            switch ( sc.database().get() )
                                                            {
                                                            case "database1":
                                                                return db1SessionMock;
                                                            case "database2":
                                                                return db2SessionMock;
                                                            default:
                                                                return null;
                                                            }
                                                        } );

        BoltStateHandler handler = new OfflineBoltStateHandler( driverMock );

        // When
        handler.connect( config );

        // Then no bookmark yet for db1
        verify( driverMock ).session( SessionConfig.builder()
                                                   .withDefaultAccessMode( AccessMode.WRITE )
                                                   .withDatabase( "database1" )
                                                   .build() );

        // When
        handler.setActiveDatabase( "database2" );

        // Then no bookmark yet for db2
        verify( driverMock ).session( SessionConfig.builder()
                                                   .withDefaultAccessMode( AccessMode.WRITE )
                                                   .withDatabase( "database2" )
                                                   .build() );

        // When
        handler.setActiveDatabase( "database1" );

        // Then use bookmark for db1
        verify( driverMock ).session( SessionConfig.builder()
                                                   .withDefaultAccessMode( AccessMode.WRITE )
                                                   .withDatabase( "database1" )
                                                   .withBookmarks( db1Bookmark )
                                                   .build() );

        // When
        handler.setActiveDatabase( "database2" );

        // Then use bookmark for db2
        verify( driverMock ).session( SessionConfig.builder()
                                                   .withDefaultAccessMode( AccessMode.WRITE )
                                                   .withDatabase( "database2" )
                                                   .withBookmarks( db2Bookmark )
                                                   .build() );
    }

    @Test
    public void fallbackToBoltS() throws CommandException
    {
        final String[] uriScheme = new String[1];
        RecordingDriverProvider provider = new RecordingDriverProvider()
        {
            @Override
            public Driver apply( String uri, AuthToken authToken, Config config )
            {
                uriScheme[0] = uri.substring( 0, uri.indexOf( ':' ) );
                if ( uriScheme[0].equals( "neo4j+s" ) )
                {
                    throw new org.neo4j.driver.exceptions.ServiceUnavailableException( "Please fall back" );
                }
                super.apply( uri, authToken, config );
                return new FakeDriver();
            }
        };
        BoltStateHandler handler = new BoltStateHandler( provider, false );
        ConnectionConfig config = new ConnectionConfig( "neo4j+s", "", -1, "", "", Encryption.DEFAULT, ABSENT_DB_NAME );
        handler.connect( config );

        assertEquals( "bolt+s", uriScheme[0] );
    }

    private Driver stubResultSummaryInAnOpenSession( Result resultMock, Session sessionMock, String version )
    {
        return stubResultSummaryInAnOpenSession( resultMock, sessionMock, version, DEFAULT_DEFAULT_DB_NAME );
    }

    private Driver stubResultSummaryInAnOpenSession( Result resultMock, Session sessionMock, String version, String databaseName )
    {
        Driver driverMock = mock( Driver.class );
        ResultSummary resultSummary = mock( ResultSummary.class );
        ServerInfo serverInfo = mock( ServerInfo.class );
        DatabaseInfo databaseInfo = mock( DatabaseInfo.class );

        when( resultSummary.server() ).thenReturn( serverInfo );
        when( serverInfo.version() ).thenReturn( version );
        when( resultMock.consume() ).thenReturn( resultSummary );
        when( resultSummary.database() ).thenReturn( databaseInfo );
        when( databaseInfo.name() ).thenReturn( databaseName );

        when( sessionMock.isOpen() ).thenReturn( true );
        when( sessionMock.run( "RETURN 1" ) ).thenReturn( resultMock );
        when( sessionMock.run( "CALL db.indexes()" ) ).thenReturn( resultMock );
        when( driverMock.session( any() ) ).thenReturn( sessionMock );

        return driverMock;
    }

    /**
     * Bolt state with faked bolt interactions
     */
    private static class OfflineBoltStateHandler extends BoltStateHandler
    {

        OfflineBoltStateHandler( Driver driver )
        {
            super( ( uri, authToken, config ) -> driver, false );
        }

        public void connect() throws CommandException
        {
            connect( new ConnectionConfig( "bolt", "", 1, "", "", Encryption.DEFAULT, ABSENT_DB_NAME ) );
        }
    }

    private static class RecordingDriverProvider implements TriFunction<String, AuthToken, Config, Driver>
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
