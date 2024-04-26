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
package org.neo4j.shell.cli;

import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;

import java.net.URI;
import java.util.List;
import java.util.Optional;
import java.util.logging.Handler;
import org.neo4j.shell.ConnectionConfig;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.terminal.CypherShellTerminal;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class CliArgs {
    static final String DEFAULT_SCHEME = "neo4j";
    static final String DEFAULT_HOST = "localhost";
    static final int DEFAULT_PORT = 7687;
    static final int DEFAULT_NUM_SAMPLE_ROWS = 1000;

    private URI uri;
    private String username = "";
    private Optional<String> impersonatedUser = Optional.empty();
    private String password = "";
    private String databaseName = ABSENT_DB_NAME;
    private AccessMode accessMode = AccessMode.WRITE;
    private FailBehavior failBehavior = FailBehavior.FAIL_FAST;
    private Format format = Format.AUTO;
    private Optional<String> cypher = Optional.empty();
    private Encryption encryption = Encryption.DEFAULT;
    private boolean nonInteractive;
    private boolean version;
    private boolean driverVersion;
    private int numSampleRows = DEFAULT_NUM_SAMPLE_ROWS;
    private boolean wrap = true;
    private String inputFilename;
    private List<ParameterService.RawParameters> parameters;
    private boolean changePassword;
    private CypherShellTerminal.HistoryBehaviour historyBehaviour;
    private Handler logHandler;
    private boolean notificationsEnabled;

    /**
     * Set the username to the primary value, or if null, the fallback value.
     */
    public void setUsername(String primary, String fallback) {
        username = primary == null ? fallback : primary;
    }

    public void setImpersonatedUser(String impersonatedUser) {
        this.impersonatedUser = Optional.ofNullable(impersonatedUser);
    }

    /**
     * Set the password to the primary value, or if null, the fallback value.
     */
    public void setPassword(String primary, String fallback) {
        password = primary == null ? fallback : primary;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public String getDatabase() {
        return databaseName;
    }

    /**
     * Set the database to connect to.
     */
    public void setDatabase(String databaseName) {
        this.databaseName = databaseName;
    }

    public FailBehavior getFailBehavior() {
        return failBehavior;
    }

    /**
     * Set the desired fail behavior
     */
    void setFailBehavior(FailBehavior failBehavior) {
        this.failBehavior = failBehavior;
    }

    public Optional<String> getCypher() {
        return cypher;
    }

    /**
     * Set the specified cypher string to execute
     */
    public void setCypher(String cypher) {
        this.cypher = Optional.ofNullable(cypher);
    }

    public Format getFormat() {
        return format;
    }

    /**
     * Set the desired format
     */
    public void setFormat(Format format) {
        this.format = format;
    }

    public void setParameters(List<ParameterService.RawParameters> parameters) {
        this.parameters = parameters;
    }

    public void setAccessMode(AccessMode accessMode) {
        this.accessMode = accessMode;
    }

    public AccessMode getAccessMode() {
        return accessMode;
    }

    public Encryption getEncryption() {
        return encryption;
    }

    /**
     * Set whether the connection should be encrypted
     */
    public void setEncryption(Encryption encryption) {
        this.encryption = encryption;
    }

    public boolean getNonInteractive() {
        return nonInteractive;
    }

    /**
     * Force the shell to use non-interactive mode. Only useful on systems where auto-detection fails, such as Windows.
     */
    public void setNonInteractive(boolean nonInteractive) {
        this.nonInteractive = nonInteractive;
    }

    public String getInputFilename() {
        return inputFilename;
    }

    /**
     * Sets a filename where to read Cypher statements from, much like piping statements from a file.
     */
    public void setInputFilename(String inputFilename) {
        this.inputFilename = inputFilename;
    }

    public boolean getVersion() {
        return version;
    }

    public void setVersion(boolean version) {
        this.version = version;
    }

    public boolean getDriverVersion() {
        return driverVersion;
    }

    public void setDriverVersion(boolean version) {
        this.driverVersion = version;
    }

    public boolean isStringShell() {
        return cypher.isPresent();
    }

    public boolean getWrap() {
        return wrap;
    }

    public void setWrap(boolean wrap) {
        this.wrap = wrap;
    }

    public int getNumSampleRows() {
        return numSampleRows;
    }

    public void setNumSampleRows(Integer numSampleRows) {
        if (numSampleRows != null && numSampleRows > 0) {
            this.numSampleRows = numSampleRows;
        }
    }

    public List<ParameterService.RawParameters> getParameters() {
        return parameters;
    }

    public void setChangePassword(boolean changePassword) {
        this.changePassword = changePassword;
    }

    public boolean getChangePassword() {
        return changePassword;
    }

    public ConnectionConfig connectionConfig() {
        return new ConnectionConfig(
                getUri(), getUsername(), getPassword(), getEncryption(), getDatabase(), impersonatedUser);
    }

    public CypherShellTerminal.HistoryBehaviour getHistoryBehaviour() {
        return historyBehaviour;
    }

    public void setHistoryBehaviour(CypherShellTerminal.HistoryBehaviour historyBehaviour) {
        this.historyBehaviour = historyBehaviour;
    }

    public URI getUri() {
        return uri;
    }

    public void setUri(URI uri) {
        this.uri = uri;
    }

    public Optional<Handler> logHandler() {
        return Optional.ofNullable(logHandler);
    }

    public void setLogHandler(Handler handler) {
        this.logHandler = handler;
    }

    public boolean getNotificationsEnabled() {
        return notificationsEnabled;
    }

    public void setNotificationsEnabled(boolean enabled) {
        this.notificationsEnabled = enabled;
    }
}
