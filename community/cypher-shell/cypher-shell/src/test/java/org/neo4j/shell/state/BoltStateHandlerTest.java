/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.shell.state;

import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
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
import org.neo4j.driver.internal.value.IntegerValue;
import org.neo4j.driver.internal.value.StringValue;
import org.neo4j.driver.summary.DatabaseInfo;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.driver.summary.ServerInfo;
import org.neo4j.shell.ConnectionConfig;
import org.neo4j.shell.TriFunction;
import org.neo4j.shell.build.Build;
import org.neo4j.shell.cli.Encryption;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.test.bolt.FakeDriver;
import org.neo4j.shell.test.bolt.FakeRecord;
import org.neo4j.shell.test.bolt.FakeResult;
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
        when(mockDriver.session(any(SessionConfig.class))).thenReturn(new FakeSession());
    }

    @Test
    void protocolVersionIsEmptyBeforeConnect() {
        assertThat(boltStateHandler.isConnected()).isFalse();
        assertThat(boltStateHandler.getProtocolVersion()).isEmpty();
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

        assertThat(handler.getProtocolVersion()).isEmpty();
    }

    @Test
    void protocolVersionIsNotEmptyAfterConnect() throws CommandException {
        Driver driverMock = stubResultSummaryInAnOpenSession(mock(Result.class), mock(Session.class), "9.4.1-ALPHA");

        BoltStateHandler handler = new BoltStateHandler((s, authToken, config) -> driverMock, false);
        handler.connect(config);

        assertThat(handler.getProtocolVersion()).isEqualTo("9.4.1-ALPHA");
    }

    @Test
    void serverVersionIsEmptyBeforeConnect() {
        assertThat(boltStateHandler.isConnected()).isFalse();
        assertThat(boltStateHandler.getServerVersion()).isEmpty();
    }

    @Test
    void serverVersionIsNotEmptyAfterConnect() throws CommandException {
        Driver fakeDriver = new FakeDriver();

        BoltStateHandler handler = new BoltStateHandler((s, authToken, config) -> fakeDriver, false);
        handler.connect(config);

        assertThat(handler.getServerVersion()).isEqualTo("4.3.0");
    }

    @Test
    void actualDatabaseNameIsNotEmptyAfterConnect() throws CommandException {
        Driver driverMock = stubResultSummaryInAnOpenSession(
                mock(Result.class), mock(Session.class), "9.4.1-ALPHA", "my_default_db");

        BoltStateHandler handler = new BoltStateHandler((s, authToken, config) -> driverMock, false);
        handler.connect(config);

        assertThat(handler.getActualDatabaseAsReportedByServer()).isEqualTo("my_default_db");
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

        assertThatThrownBy(
                        () -> handler.runUserCypher("RETURN \"hello\"", Collections.emptyMap()),
                        "should fail on runCypher")
                .isEqualTo(databaseNotFound);

        assertThat(handler.getActualDatabaseAsReportedByServer()).isEqualTo("my_default_db");
    }

    @Test
    void closeTransactionAfterRollback() throws CommandException {
        boltStateHandler.connect();
        boltStateHandler.beginTransaction();

        assertThat(boltStateHandler.isTransactionOpen()).isTrue();

        boltStateHandler.rollbackTransaction();

        assertThat(boltStateHandler.isTransactionOpen()).isFalse();
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

        assertThatThrownBy(boltStateHandler::connect, "should fail on silent disconnect")
                .isEqualTo(originalException)
                .hasSuppressedException(thrownFromSilentDisconnect);
    }

    @Test
    void closeTransactionAfterCommit() throws CommandException {
        boltStateHandler.connect();
        boltStateHandler.beginTransaction();
        assertThat(boltStateHandler.isTransactionOpen()).isTrue();

        boltStateHandler.commitTransaction();

        assertThat(boltStateHandler.isTransactionOpen()).isFalse();
    }

    @Test
    void beginNeedsToBeConnected() {
        assertThat(boltStateHandler.isConnected()).isFalse();

        assertThatThrownBy(boltStateHandler::beginTransaction)
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Not connected to Neo4j");
    }

    @Test
    void commitNeedsToBeConnected() {
        assertThat(boltStateHandler.isConnected()).isFalse();

        assertThatThrownBy(boltStateHandler::commitTransaction)
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Not connected to Neo4j");
    }

    @Test
    void beginNeedsToInitialiseTransactionStatements() throws CommandException {
        boltStateHandler.connect();

        boltStateHandler.beginTransaction();
        assertThat(boltStateHandler.isTransactionOpen()).isTrue();
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
        assertThat(boltResult.iterate()).isEqualTo(result);

        boltStateHandler.commitTransaction();

        assertThat(boltStateHandler.isTransactionOpen()).isFalse();
    }

    @Test
    void rollbackNeedsToBeConnected() {
        assertThat(boltStateHandler.isConnected()).isFalse();

        assertThatThrownBy(boltStateHandler::rollbackTransaction)
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Not connected to Neo4j");
    }

    @Test
    void executeNeedsToBeConnected() {
        assertThatThrownBy(() -> boltStateHandler.runUserCypher("", Collections.emptyMap()))
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Not connected to Neo4j");
    }

    @Test
    void shouldExecuteInTransactionIfOpen() throws CommandException {
        boltStateHandler.connect();
        boltStateHandler.beginTransaction();

        assertThat(boltStateHandler.isTransactionOpen())
                .as("Expected a transaction")
                .isTrue();
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

        assertThat(boltResult.getRecords().get(0).get(0).toString()).isEqualTo("999");
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

        verify(driverMock, times(2)).session(any(SessionConfig.class));
        verify(sessionMock, times(2)).run(any(Query.class), eq(userTxConf));

        assertThat(boltResult.getRecords().get(0).get(0).toString()).isEqualTo("999");
    }

    @Test
    void shouldExecuteInSessionByDefault() throws CommandException {
        boltStateHandler.connect();

        assertThat(boltStateHandler.isTransactionOpen())
                .as("Did not expect a transaction")
                .isFalse();
    }

    @Test
    void canOnlyConnectOnce() throws CommandException {
        boltStateHandler.connect();
        assertThatThrownBy(boltStateHandler::connect)
                .isInstanceOf(CommandException.class)
                .hasMessageContaining("Already connected");
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
        verify(sessionMock, times(1)).run(eq("CALL db.ping()"), eq(systemTxConf));
        verify(sessionMock, times(1)).run(eq("CALL dbms.licenseAgreementDetails()"), eq(systemTxConf));
        verify(sessionMock, times(2)).isOpen();
        verify(sessionMock, times(1)).beginTransaction(eq(userTxConf));
        verifyNoMoreInteractions(sessionMock);
    }

    @Test
    void silentDisconnectCleansUp() throws Exception {
        // given
        boltStateHandler.connect();

        Session session = boltStateHandler.session;
        assertThat(session).isNotNull();
        assertThat(boltStateHandler.driver).isNotNull();
        assertThat(boltStateHandler.session.isOpen()).isTrue();

        // when
        boltStateHandler.silentDisconnect();

        // then
        assertThat(session.isOpen()).isFalse();
    }

    @Test
    void turnOffEncryptionIfRequested() throws CommandException {
        RecordingDriverProvider provider = new RecordingDriverProvider();
        BoltStateHandler handler = new BoltStateHandler(provider, false);

        handler.connect(config);
        assertThat(provider.config.encrypted()).isFalse();
    }

    @Test
    void turnOnEncryptionIfRequested() throws CommandException {
        RecordingDriverProvider provider = new RecordingDriverProvider();
        BoltStateHandler handler = new BoltStateHandler(provider, false);
        ConnectionConfig config = testConnectionConfig("bolt://localhost", Encryption.TRUE);
        handler.connect(config);
        assertThat(provider.config.encrypted()).isTrue();
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
        when(driverMock.session(any(SessionConfig.class))).thenReturn(sessionMock);
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
        assertThat(handler.session).isNull();

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
        when(driverMock.session(any(SessionConfig.class))).thenAnswer(arg -> {
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

        assertThat(provider.config.userAgent()).startsWith("neo4j-cypher-shell/v");
    }

    @Test
    void handleErrorsOnCommit() throws CommandException {
        reset(mockDriver);
        var mockSession = spy(FakeSession.class);
        var mockTx = mock(Transaction.class);
        doThrow(new ClientException("Failed to commit :(")).when(mockTx).commit();
        when(mockSession.beginTransaction(any())).thenReturn(mockTx);
        when(mockDriver.session(any(SessionConfig.class))).thenReturn(mockSession);

        boltStateHandler.connect();
        boltStateHandler.beginTransaction();
        assertThatThrownBy(boltStateHandler::commitTransaction).isInstanceOf(ClientException.class);
        assertThat(boltStateHandler.isTransactionOpen()).isFalse();
    }

    @Test
    void handleErrorsOnRollback() throws CommandException {
        reset(mockDriver);
        var mockSession = spy(FakeSession.class);
        var mockTx = mock(Transaction.class);
        doThrow(new ClientException("Failed to rollback :(")).when(mockTx).rollback();
        when(mockSession.beginTransaction(any())).thenReturn(mockTx);
        when(mockDriver.session(any(SessionConfig.class))).thenReturn(mockSession);

        boltStateHandler.connect();
        boltStateHandler.beginTransaction();
        assertThatThrownBy(boltStateHandler::rollbackTransaction).isInstanceOf(ClientException.class);
        assertThat(boltStateHandler.isTransactionOpen()).isFalse();
    }

    @Test
    void noImpersonation() throws CommandException {
        var fakeDriver = new FakeDriver();

        BoltStateHandler handler = new BoltStateHandler((s, authToken, config) -> fakeDriver, false);
        handler.connect(config);

        assertThat(fakeDriver.sessionConfigs).hasSize(1);
        assertThat(fakeDriver.sessionConfigs.get(0).impersonatedUser()).isEmpty();
    }

    @Test
    void impersonation() throws CommandException {
        var fakeDriver = new FakeDriver();

        BoltStateHandler handler = new BoltStateHandler((s, authToken, config) -> fakeDriver, false);
        handler.connect(config.withImpersonatedUser("emil"));

        assertThat(fakeDriver.sessionConfigs).hasSize(1);
        assertThat(fakeDriver.sessionConfigs.get(0).impersonatedUser()).hasValue("emil");
    }

    @Test
    void licenseStatusExpired() throws CommandException {
        final var session = mockSessionWithLicensing("expired", -1);
        Driver driverMock =
                stubResultSummaryInAnOpenSession(mock(Result.class), session, "5.4.0", DEFAULT_DEFAULT_DB_NAME);

        BoltStateHandler handler = new BoltStateHandler((s, authToken, config) -> driverMock, true);
        handler.connect(config);

        assertThat(handler.licenseDetails().status()).isEqualTo(LicenseDetails.Status.EXPIRED);
        assertThat(handler.licenseDetails().daysLeft()).contains(0L);
    }

    @Test
    void licenseStatusDaysLeft() throws CommandException {
        final var session = mockSessionWithLicensing("eval", 5);
        Driver driverMock =
                stubResultSummaryInAnOpenSession(mock(Result.class), session, "5.4.0", DEFAULT_DEFAULT_DB_NAME);

        BoltStateHandler handler = new BoltStateHandler((s, authToken, config) -> driverMock, true);
        handler.connect(config);

        assertThat(handler.licenseDetails().status()).isEqualTo(LicenseDetails.Status.EVAL);
        assertThat(handler.licenseDetails().daysLeft()).contains(5L);
    }

    @Test
    void licenseStatusUnknown() throws CommandException {
        final var session = mockSessionWithLicensing("unexpected", 0);
        Driver driverMock =
                stubResultSummaryInAnOpenSession(mock(Result.class), session, "5.4.0", DEFAULT_DEFAULT_DB_NAME);

        BoltStateHandler handler = new BoltStateHandler((s, authToken, config) -> driverMock, true);
        handler.connect(config);

        assertThat(handler.licenseDetails().status()).isEqualTo(LicenseDetails.Status.YES);
        assertThat(handler.licenseDetails().daysLeft()).isEmpty();
    }

    private Session mockSessionWithLicensing(String status, long daysLeftOnTrial) {
        final var session = mock(Session.class);
        FakeRecord record = FakeRecord.of(Map.of(
                "status", new StringValue(status),
                "daysLeftOnTrial", new IntegerValue(daysLeftOnTrial),
                "totalTrialDays", new IntegerValue(30)));
        final var licenseResult = new FakeResult(Collections.singletonList(record));
        when(session.run(eq("CALL dbms.licenseAgreementDetails()"), eq(systemTxConf)))
                .thenReturn(licenseResult);
        return session;
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
        when(driverMock.session(any(SessionConfig.class))).thenReturn(sessionMock);

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

        assertThat(uriScheme[0]).isEqualTo(fallbackScheme);
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
