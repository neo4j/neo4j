/*
 * Copyright (c) "Neo4j"
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

import static java.util.Collections.singletonList;
import static java.util.Optional.empty;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;
import static org.neo4j.shell.DatabaseManager.DEFAULT_DEFAULT_DB_NAME;
import static org.neo4j.shell.DatabaseManager.SYSTEM_DB_NAME;
import static org.neo4j.shell.test.Util.testConnectionConfig;

import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
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
import org.neo4j.driver.Transaction;
import org.neo4j.driver.TransactionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.SessionExpiredException;
import org.neo4j.driver.internal.InternalBookmark;
import org.neo4j.driver.summary.DatabaseInfo;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.ServerInfo;
import org.neo4j.shell.ConnectionConfig;
import org.neo4j.shell.TriFunction;
import org.neo4j.shell.build.Build;
import org.neo4j.shell.cli.Encryption;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.test.bolt.FakeDriver;
import org.neo4j.shell.test.bolt.FakeSession;

class BoltStateHandlerTest {
    private final Driver mockDriver = mock(Driver.class);
    private final OfflineBoltStateHandler boltStateHandler = new OfflineBoltStateHandler(mockDriver);
    private final ConnectionConfig config = testConnectionConfig("bolt://localhost");
    private final TransactionConfig systemTxConf = TransactionConfig.builder()
            .withMetadata(Map.of("type", "system", "app", "cypher-shell_v" + Build.version()))
            .build();
    private final TransactionConfig userTxConf = TransactionConfig.builder()
            .withMetadata(Map.of("type", "user-direct", "app", "cypher-shell_v" + Build.version()))
            .build();
    private final TransactionConfig userActionTxConf = TransactionConfig.builder()
            .withMetadata(Map.of("type", "user-action", "app", "cypher-shell_v" + Build.version()))
            .build();

    @BeforeEach
    void setup() {
        when(mockDriver.session(any())).thenReturn(new FakeSession());
    }

    @Test
    void protocolVersionIsEmptyBeforeConnect() {
        assertFalse(boltStateHandler.isConnected());
        assertEquals("", boltStateHandler.getProtocolVersion());
    }

    @Test
    void protocolVersionIsEmptyIfDriverReturnsNull() throws CommandException {
        RecordingDriverProvider provider = new RecordingDriverProvider() {
            @Override
            public Driver apply(URI uri, AuthToken authToken, Config config) {
                super.apply(uri, authToken, config);
                return new FakeDriver();
            }
        };
        BoltStateHandler handler = new BoltStateHandler(provider, false);
        handler.connect(config);

        assertEquals("", handler.getProtocolVersion());
    }

    @Test
    void protocolVersionIsNotEmptyAfterConnect() throws CommandException {
        Driver driverMock = stubResultSummaryInAnOpenSession(mock(Result.class), mock(Session.class), "9.4.1-ALPHA");

        BoltStateHandler handler = new BoltStateHandler((s, authToken, config) -> driverMock, false);
        handler.connect(config);

        assertEquals("9.4.1-ALPHA", handler.getProtocolVersion());
    }

    @Test
    void serverVersionIsEmptyBeforeConnect() {
        assertFalse(boltStateHandler.isConnected());
        assertEquals("", boltStateHandler.getServerVersion());
    }

    @Test
    void serverVersionIsNotEmptyAfterConnect() throws CommandException {
        Driver fakeDriver = new FakeDriver();

        BoltStateHandler handler = new BoltStateHandler((s, authToken, config) -> fakeDriver, false);
        handler.connect(config);

        assertEquals("4.3.0", handler.getServerVersion());
    }

    @Test
    void actualDatabaseNameIsNotEmptyAfterConnect() throws CommandException {
        Driver driverMock = stubResultSummaryInAnOpenSession(
                mock(Result.class), mock(Session.class), "9.4.1-ALPHA", "my_default_db");

        BoltStateHandler handler = new BoltStateHandler((s, authToken, config) -> driverMock, false);
        handler.connect(config);

        assertEquals("my_default_db", handler.getActualDatabaseAsReportedByServer());
    }

    @Test
    void exceptionFromRunQueryDoesNotResetActualDatabaseNameToUnresolved() throws CommandException {
        Session sessionMock = mock(Session.class);
        Result resultMock = mock(Result.class);
        Driver driverMock = stubResultSummaryInAnOpenSession(resultMock, sessionMock, "9.4.1-ALPHA", "my_default_db");

        ClientException databaseNotFound = new ClientException("Neo.ClientError.Database.DatabaseNotFound", "blah");

        when(sessionMock.run(any(Query.class), eq(userTxConf)))
                .thenThrow(databaseNotFound)
                .thenReturn(resultMock);

        BoltStateHandler handler = new BoltStateHandler((s, authToken, config) -> driverMock, false);
        handler.connect(config);

        try {
            handler.runUserCypher("RETURN \"hello\"", Collections.emptyMap());
            fail("should fail on runCypher");
        } catch (Exception e) {
            assertThat(e, is(databaseNotFound));
            assertEquals("my_default_db", handler.getActualDatabaseAsReportedByServer());
        }
    }

    @Test
    void closeTransactionAfterRollback() throws CommandException {
        boltStateHandler.connect();
        boltStateHandler.beginTransaction();

        assertTrue(boltStateHandler.isTransactionOpen());

        boltStateHandler.rollbackTransaction();

        assertFalse(boltStateHandler.isTransactionOpen());
    }

    @Test
    void exceptionsFromSilentDisconnectAreSuppressedToReportOriginalErrors() {
        Session session = mock(Session.class);
        Result resultMock = mock(Result.class);

        RuntimeException originalException = new RuntimeException("original exception");
        RuntimeException thrownFromSilentDisconnect = new RuntimeException("exception from silent disconnect");

        Driver mockedDriver = stubResultSummaryInAnOpenSession(resultMock, session, "neo4j-version");
        OfflineBoltStateHandler boltStateHandler = new OfflineBoltStateHandler(mockedDriver);

        when(resultMock.consume()).thenThrow(originalException);
        doThrow(thrownFromSilentDisconnect).when(session).close();

        try {
            boltStateHandler.connect();
            fail("should fail on silent disconnect");
        } catch (Exception e) {
            assertThat(e.getSuppressed()[0], is(thrownFromSilentDisconnect));
            assertThat(e, is(originalException));
        }
    }

    @Test
    void closeTransactionAfterCommit() throws CommandException {
        boltStateHandler.connect();
        boltStateHandler.beginTransaction();
        assertTrue(boltStateHandler.isTransactionOpen());

        boltStateHandler.commitTransaction();

        assertFalse(boltStateHandler.isTransactionOpen());
    }

    @Test
    void beginNeedsToBeConnected() {
        assertFalse(boltStateHandler.isConnected());

        var exception = assertThrows(CommandException.class, boltStateHandler::beginTransaction);
        assertThat(exception.getMessage(), containsString("Not connected to Neo4j"));
    }

    @Test
    void commitNeedsToBeConnected() {
        assertFalse(boltStateHandler.isConnected());

        var exception = assertThrows(CommandException.class, boltStateHandler::commitTransaction);
        assertThat(exception.getMessage(), containsString("Not connected to Neo4j"));
    }

    @Test
    void beginNeedsToInitialiseTransactionStatements() throws CommandException {
        boltStateHandler.connect();

        boltStateHandler.beginTransaction();
        assertTrue(boltStateHandler.isTransactionOpen());
    }

    @Test
    void whenInTransactionHandlerLetsTransactionDoTheWork() throws CommandException {
        Transaction transactionMock = mock(Transaction.class);
        Session sessionMock = mock(Session.class);
        when(sessionMock.beginTransaction(any())).thenReturn(transactionMock);
        Driver driverMock = stubResultSummaryInAnOpenSession(mock(Result.class), sessionMock, "neo4j-version");

        Result result = mock(Result.class);

        when(transactionMock.run(any(Query.class))).thenReturn(result);

        OfflineBoltStateHandler boltStateHandler = new OfflineBoltStateHandler(driverMock);
        boltStateHandler.connect();
        boltStateHandler.beginTransaction();
        BoltResult boltResult = boltStateHandler
                .runUserCypher("UNWIND [1,2] as num RETURN *", Collections.emptyMap())
                .get();
        assertEquals(result, boltResult.iterate());

        boltStateHandler.commitTransaction();

        assertFalse(boltStateHandler.isTransactionOpen());
    }

    @Test
    void rollbackNeedsToBeConnected() {
        assertFalse(boltStateHandler.isConnected());

        var exception = assertThrows(CommandException.class, boltStateHandler::rollbackTransaction);
        assertThat(exception.getMessage(), containsString("Not connected to Neo4j"));
    }

    @Test
    void executeNeedsToBeConnected() {
        var exception =
                assertThrows(CommandException.class, () -> boltStateHandler.runUserCypher("", Collections.emptyMap()));
        assertThat(exception.getMessage(), containsString("Not connected to Neo4j"));
    }

    @Test
    void shouldExecuteInTransactionIfOpen() throws CommandException {
        boltStateHandler.connect();
        boltStateHandler.beginTransaction();

        assertTrue(boltStateHandler.isTransactionOpen(), "Expected a transaction");
    }

    @Test
    void shouldRunCypherQuery() throws CommandException {
        Session sessionMock = mock(Session.class);
        Result resultMock = mock(Result.class);
        Record recordMock = mock(Record.class);
        Value valueMock = mock(Value.class);

        Driver driverMock = stubResultSummaryInAnOpenSession(resultMock, sessionMock, "neo4j-version");

        when(resultMock.list()).thenReturn(singletonList(recordMock));

        when(valueMock.toString()).thenReturn("999");
        when(recordMock.get(0)).thenReturn(valueMock);
        when(sessionMock.run(any(Query.class), eq(userTxConf))).thenReturn(resultMock);

        OfflineBoltStateHandler boltStateHandler = new OfflineBoltStateHandler(driverMock);

        boltStateHandler.connect();

        BoltResult boltResult =
                boltStateHandler.runUserCypher("RETURN 999", new HashMap<>()).get();
        verify(sessionMock).run(any(Query.class), eq(userTxConf));

        assertEquals("999", boltResult.getRecords().get(0).get(0).toString());
    }

    @Test
    void triesAgainOnSessionExpired() throws Exception {
        Session sessionMock = mock(Session.class);
        Result resultMock = mock(Result.class);
        Record recordMock = mock(Record.class);
        Value valueMock = mock(Value.class);

        Driver driverMock = stubResultSummaryInAnOpenSession(resultMock, sessionMock, "neo4j-version");

        when(resultMock.list()).thenReturn(singletonList(recordMock));

        when(valueMock.toString()).thenReturn("999");
        when(recordMock.get(0)).thenReturn(valueMock);
        when(sessionMock.run(any(Query.class), eq(userTxConf)))
                .thenThrow(new SessionExpiredException("leaderswitch"))
                .thenReturn(resultMock);

        OfflineBoltStateHandler boltStateHandler = new OfflineBoltStateHandler(driverMock);

        boltStateHandler.connect();
        BoltResult boltResult =
                boltStateHandler.runUserCypher("RETURN 999", new HashMap<>()).get();

        verify(driverMock, times(2)).session(any());
        verify(sessionMock, times(2)).run(any(Query.class), eq(userTxConf));

        assertEquals("999", boltResult.getRecords().get(0).get(0).toString());
    }

    @Test
    void shouldExecuteInSessionByDefault() throws CommandException {
        boltStateHandler.connect();

        assertFalse(boltStateHandler.isTransactionOpen(), "Did not expect a transaction");
    }

    @Test
    void canOnlyConnectOnce() throws CommandException {
        boltStateHandler.connect();
        var exception = assertThrows(CommandException.class, boltStateHandler::connect);
        assertThat(exception.getMessage(), containsString("Already connected"));
    }

    @Test
    void resetSessionOnReset() throws Exception {
        // given
        Session sessionMock = mock(Session.class);
        Driver driverMock = stubResultSummaryInAnOpenSession(mock(Result.class), sessionMock, "neo4j-version");

        OfflineBoltStateHandler boltStateHandler = new OfflineBoltStateHandler(driverMock);

        boltStateHandler.connect();
        boltStateHandler.beginTransaction();

        // when
        boltStateHandler.reset();

        // then
        verify(sessionMock, times(1)).run(anyString(), eq(systemTxConf));
        verify(sessionMock, times(2)).isOpen();
        verify(sessionMock, times(1)).beginTransaction(eq(userTxConf));
        verifyNoMoreInteractions(sessionMock);
    }

    @Test
    void silentDisconnectCleansUp() throws Exception {
        // given
        boltStateHandler.connect();

        Session session = boltStateHandler.session;
        assertNotNull(session);
        assertNotNull(boltStateHandler.driver);

        assertTrue(boltStateHandler.session.isOpen());

        // when
        boltStateHandler.silentDisconnect();

        // then
        assertFalse(session.isOpen());
    }

    @Test
    void turnOffEncryptionIfRequested() throws CommandException {
        RecordingDriverProvider provider = new RecordingDriverProvider();
        BoltStateHandler handler = new BoltStateHandler(provider, false);

        handler.connect(config);
        assertFalse(provider.config.encrypted());
    }

    @Test
    void turnOnEncryptionIfRequested() throws CommandException {
        RecordingDriverProvider provider = new RecordingDriverProvider();
        BoltStateHandler handler = new BoltStateHandler(provider, false);
        ConnectionConfig config = testConnectionConfig("bolt://localhost", Encryption.TRUE);
        handler.connect(config);
        assertTrue(provider.config.encrypted());
    }

    @Test
    void fallbackToBolt() throws CommandException {
        fallbackTest("neo4j", "bolt", () -> {
            throw new ServiceUnavailableException("Please fall back");
        });
        fallbackTest("neo4j", "bolt", () -> {
            throw new SessionExpiredException("Please fall back");
        });
    }

    @Test
    void fallbackToBoltSSC() throws CommandException {
        fallbackTest("neo4j+ssc", "bolt+ssc", () -> {
            throw new ServiceUnavailableException("Please fall back");
        });
        fallbackTest("neo4j+ssc", "bolt+ssc", () -> {
            throw new SessionExpiredException("Please fall back");
        });
    }

    @Test
    void fallbackToBoltS() throws CommandException {
        fallbackTest("neo4j+s", "bolt+s", () -> {
            throw new ServiceUnavailableException("Please fall back");
        });
        fallbackTest("neo4j+s", "bolt+s", () -> {
            throw new SessionExpiredException("Please fall back");
        });
    }

    @Test
    void fallbackToLegacyPing() throws CommandException {
        // given
        Session sessionMock = mock(Session.class);
        Result failing = mock(Result.class);
        Result other = mock(Result.class, RETURNS_DEEP_STUBS);
        when(failing.consume())
                .thenThrow(new ClientException(
                        "Neo.ClientError.Procedure.ProcedureNotFound", "No procedure CALL db.ping(()"));
        when(sessionMock.run(eq("CALL db.ping()"), eq(systemTxConf))).thenReturn(failing);
        when(sessionMock.run(eq("RETURN 1"), eq(systemTxConf))).thenReturn(other);
        Driver driverMock = mock(Driver.class);
        when(driverMock.session(any())).thenReturn(sessionMock);
        OfflineBoltStateHandler boltStateHandler = new OfflineBoltStateHandler(driverMock);

        // when
        boltStateHandler.connect();

        // then
        verify(sessionMock).run(eq("RETURN 1"), eq(systemTxConf));
    }

    @Test
    void shouldChangePasswordAndKeepSystemDbBookmark() throws CommandException {
        // Given
        ConnectionConfig config =
                testConnectionConfig("bolt://localhost").withUsernameAndPasswordAndDatabase("", "", ABSENT_DB_NAME);
        Bookmark bookmark = InternalBookmark.parse("myBookmark");
        var newPassword = "newPW";

        Session sessionMock = mock(Session.class);
        Result resultMock = mock(Result.class);
        Driver driverMock =
                stubResultSummaryInAnOpenSession(resultMock, sessionMock, "Neo4j/9.4.1-ALPHA", "my_default_db");
        when(sessionMock.run(
                        eq(new Query(
                                "ALTER CURRENT USER SET PASSWORD FROM $o TO $n",
                                Values.parameters("o", config.password(), "n", newPassword))),
                        eq(userActionTxConf)))
                .thenReturn(resultMock);
        when(sessionMock.lastBookmark()).thenReturn(bookmark);
        BoltStateHandler handler = new OfflineBoltStateHandler(driverMock);

        // When
        handler.changePassword(config, newPassword);

        // Then
        assertNull(handler.session);

        // When connecting to system db again
        handler.connect(config.withUsernameAndPasswordAndDatabase("", "", SYSTEM_DB_NAME));

        // Then use bookmark for system DB
        verify(driverMock)
                .session(SessionConfig.builder()
                        .withDefaultAccessMode(AccessMode.WRITE)
                        .withDatabase(SYSTEM_DB_NAME)
                        .withBookmarks(bookmark)
                        .build());
    }

    @SuppressWarnings("OptionalGetWithoutIsPresent")
    @Test
    void shouldKeepOneBookmarkPerDatabase() throws CommandException {
        ConnectionConfig config = testConnectionConfig("bolt://localhost")
                .withUsernameAndPasswordAndDatabase("user", "pass", "database1");
        Bookmark db1Bookmark = InternalBookmark.parse("db1");
        Bookmark db2Bookmark = InternalBookmark.parse("db2");

        // A couple of these mock calls are now redundant with what is called in stubResultSummaryInAnOpenSession
        Result resultMock = mock(Result.class);
        Session db1SessionMock = mock(Session.class);
        when(db1SessionMock.isOpen()).thenReturn(true);
        when(db1SessionMock.lastBookmark()).thenReturn(db1Bookmark);
        when(db1SessionMock.run(eq("CALL db.ping()"), eq(systemTxConf))).thenReturn(resultMock);
        Session db2SessionMock = mock(Session.class);
        when(db2SessionMock.isOpen()).thenReturn(true);
        when(db2SessionMock.lastBookmark()).thenReturn(db2Bookmark);
        when(db2SessionMock.run(eq("CALL db.ping()"), eq(systemTxConf))).thenReturn(resultMock);

        Driver driverMock =
                stubResultSummaryInAnOpenSession(resultMock, db1SessionMock, "Neo4j/9.4.1-ALPHA", "database1");
        when(driverMock.session(any())).thenAnswer(arg -> {
            SessionConfig sc = (SessionConfig) arg.getArguments()[0];
            switch (sc.database().get()) {
                case "database1":
                    return db1SessionMock;
                case "database2":
                    return db2SessionMock;
                default:
                    return null;
            }
        });

        BoltStateHandler handler = new OfflineBoltStateHandler(driverMock);

        // When
        handler.connect(config);

        // Then no bookmark yet for db1
        verify(driverMock)
                .session(SessionConfig.builder()
                        .withDefaultAccessMode(AccessMode.WRITE)
                        .withDatabase("database1")
                        .build());

        // When
        handler.setActiveDatabase("database2");

        // Then no bookmark yet for db2
        verify(driverMock)
                .session(SessionConfig.builder()
                        .withDefaultAccessMode(AccessMode.WRITE)
                        .withDatabase("database2")
                        .build());

        // When
        handler.setActiveDatabase("database1");

        // Then use bookmark for db1
        verify(driverMock)
                .session(SessionConfig.builder()
                        .withDefaultAccessMode(AccessMode.WRITE)
                        .withDatabase("database1")
                        .withBookmarks(db1Bookmark)
                        .build());

        // When
        handler.setActiveDatabase("database2");

        // Then use bookmark for db2
        verify(driverMock)
                .session(SessionConfig.builder()
                        .withDefaultAccessMode(AccessMode.WRITE)
                        .withDatabase("database2")
                        .withBookmarks(db2Bookmark)
                        .build());
    }

    @Test
    void provideUserAgentstring() throws CommandException {
        RecordingDriverProvider provider = new RecordingDriverProvider() {
            @Override
            public Driver apply(URI uri, AuthToken authToken, Config config) {
                super.apply(uri, authToken, config);
                return new FakeDriver();
            }
        };
        BoltStateHandler handler = new BoltStateHandler(provider, false);
        handler.connect(config);

        assertTrue(provider.config.userAgent().startsWith("neo4j-cypher-shell/v"));
    }

    @Test
    void handleErrorsOnCommit() throws CommandException {
        reset(mockDriver);
        var mockSession = spy(FakeSession.class);
        var mockTx = mock(Transaction.class);
        doThrow(new ClientException("Failed to commit :(")).when(mockTx).commit();
        when(mockSession.beginTransaction(any())).thenReturn(mockTx);
        when(mockDriver.session(any())).thenReturn(mockSession);

        boltStateHandler.connect();
        boltStateHandler.beginTransaction();
        assertThrows(ClientException.class, boltStateHandler::commitTransaction);
        assertFalse(boltStateHandler.isTransactionOpen());
    }

    @Test
    void handleErrorsOnRollback() throws CommandException {
        reset(mockDriver);
        var mockSession = spy(FakeSession.class);
        var mockTx = mock(Transaction.class);
        doThrow(new ClientException("Failed to rollback :(")).when(mockTx).rollback();
        when(mockSession.beginTransaction(any())).thenReturn(mockTx);
        when(mockDriver.session(any())).thenReturn(mockSession);

        boltStateHandler.connect();
        boltStateHandler.beginTransaction();
        assertThrows(ClientException.class, boltStateHandler::rollbackTransaction);
        assertFalse(boltStateHandler.isTransactionOpen());
    }

    @Test
    void noImpersonation() throws CommandException {
        var fakeDriver = new FakeDriver();

        BoltStateHandler handler = new BoltStateHandler((s, authToken, config) -> fakeDriver, false);
        handler.connect(config);

        assertEquals(1, fakeDriver.sessionConfigs.size());
        assertEquals(empty(), fakeDriver.sessionConfigs.get(0).impersonatedUser());
    }

    @Test
    void impersonation() throws CommandException {
        var fakeDriver = new FakeDriver();

        BoltStateHandler handler = new BoltStateHandler((s, authToken, config) -> fakeDriver, false);
        handler.connect(config.withImpersonatedUser("emil"));

        assertEquals(1, fakeDriver.sessionConfigs.size());
        assertEquals(Optional.of("emil"), fakeDriver.sessionConfigs.get(0).impersonatedUser());
    }

    private Driver stubResultSummaryInAnOpenSession(Result resultMock, Session sessionMock, String version) {
        return stubResultSummaryInAnOpenSession(resultMock, sessionMock, version, DEFAULT_DEFAULT_DB_NAME);
    }

    private Driver stubResultSummaryInAnOpenSession(
            Result resultMock, Session sessionMock, String protocolVersion, String databaseName) {
        Driver driverMock = mock(Driver.class);
        ResultSummary resultSummary = mock(ResultSummary.class);
        ServerInfo serverInfo = mock(ServerInfo.class);
        DatabaseInfo databaseInfo = mock(DatabaseInfo.class);

        when(resultSummary.server()).thenReturn(serverInfo);
        when(serverInfo.protocolVersion()).thenReturn(protocolVersion);
        when(resultMock.consume()).thenReturn(resultSummary);
        when(resultSummary.database()).thenReturn(databaseInfo);
        when(databaseInfo.name()).thenReturn(databaseName);

        when(sessionMock.isOpen()).thenReturn(true);
        when(sessionMock.run(eq("CALL db.ping()"), eq(systemTxConf))).thenReturn(resultMock);
        when(sessionMock.run(anyString(), any(Value.class))).thenReturn(resultMock);
        when(driverMock.session(any())).thenReturn(sessionMock);

        return driverMock;
    }

    private void fallbackTest(String initialScheme, String fallbackScheme, Runnable failer) throws CommandException {
        final String[] uriScheme = new String[1];
        RecordingDriverProvider provider = new RecordingDriverProvider() {
            @Override
            public Driver apply(URI uri, AuthToken authToken, Config config) {
                uriScheme[0] = uri.getScheme();
                if (uri.getScheme().equals(initialScheme)) {
                    failer.run();
                }
                super.apply(uri, authToken, config);
                return new FakeDriver();
            }
        };
        BoltStateHandler handler = new BoltStateHandler(provider, false);
        ConnectionConfig config = testConnectionConfig(initialScheme + "://localhost");
        handler.connect(config);

        assertEquals(fallbackScheme, uriScheme[0]);
    }

    /**
     * Bolt state with faked bolt interactions
     */
    private static class OfflineBoltStateHandler extends BoltStateHandler {

        OfflineBoltStateHandler(Driver driver) {
            super((uri, authToken, config) -> driver, false);
        }

        public void connect() throws CommandException {
            connect(testConnectionConfig("bolt://localhost"));
        }
    }

    private static class RecordingDriverProvider implements TriFunction<URI, AuthToken, Config, Driver> {
        Config config;

        @Override
        public Driver apply(URI uri, AuthToken authToken, Config config) {
            this.config = config;
            return new FakeDriver();
        }
    }
}
