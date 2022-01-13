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
package org.neo4j.shell.cli;

import java.io.File;
import java.util.List;
import java.util.Optional;

import org.neo4j.shell.ConnectionConfig;
import org.neo4j.shell.Environment;
import org.neo4j.shell.Historian;
import org.neo4j.shell.parameter.ParameterService.RawParameter;

import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;

public class CliArgs
{
    static final String DEFAULT_SCHEME = "neo4j";
    static final String DEFAULT_HOST = "localhost";
    static final int DEFAULT_PORT = 7687;
    static final int DEFAULT_NUM_SAMPLE_ROWS = 1000;

    private String scheme = DEFAULT_SCHEME;
    private String host = DEFAULT_HOST;
    private int port = DEFAULT_PORT;
    private String username = "";
    private String password = "";
    private String databaseName = ABSENT_DB_NAME;
    private FailBehavior failBehavior = FailBehavior.FAIL_FAST;
    private Format format = Format.AUTO;
    @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" )
    private Optional<String> cypher = Optional.empty();
    private Encryption encryption = Encryption.DEFAULT;
    private boolean debugMode;
    private boolean nonInteractive;
    private boolean version;
    private boolean driverVersion;
    private int numSampleRows = DEFAULT_NUM_SAMPLE_ROWS;
    private boolean wrap = true;
    private String inputFilename;
    private List<RawParameter> parameters;
    private boolean changePassword;
    private File historyFile = Historian.defaultHistoryFile();

    /**
     * Set the scheme to the primary value, or if null, the fallback value.
     */
    public void setScheme( String primary, String fallback )
    {
        scheme = primary == null ? fallback : primary;
    }

    /**
     * Set the host to the primary value, or if null, the fallback value.
     */
    void setHost( String primary, String fallback )
    {
        host = primary == null ? fallback : primary;
    }

    /**
     * Set the username to the primary value, or if null, the fallback value.
     */
    public void setUsername( String primary, String fallback )
    {
        username = primary == null ? fallback : primary;
    }

    /**
     * Set the password to the primary value, or if null, the fallback value.
     */
    public void setPassword( String primary, String fallback )
    {
        password = primary == null ? fallback : primary;
    }

    public String getScheme()
    {
        return scheme;
    }

    public String getHost()
    {
        return host;
    }

    public int getPort()
    {
        return port;
    }

    /**
     * Set the port to the value.
     */
    public void setPort( int port )
    {
        this.port = port;
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    public String getDatabase()
    {
        return databaseName;
    }

    /**
     * Set the database to connect to.
     */
    public void setDatabase( String databaseName )
    {
        this.databaseName = databaseName;
    }

    public FailBehavior getFailBehavior()
    {
        return failBehavior;
    }

    /**
     * Set the desired fail behavior
     */
    void setFailBehavior( FailBehavior failBehavior )
    {
        this.failBehavior = failBehavior;
    }

    public Optional<String> getCypher()
    {
        return cypher;
    }

    /**
     * Set the specified cypher string to execute
     */
    public void setCypher( String cypher )
    {
        this.cypher = Optional.ofNullable( cypher );
    }

    public Format getFormat()
    {
        return format;
    }

    /**
     * Set the desired format
     */
    public void setFormat( Format format )
    {
        this.format = format;
    }

    public void setParameters( List<RawParameter> parameters )
    {
        this.parameters = parameters;
    }

    public Encryption getEncryption()
    {
        return encryption;
    }

    /**
     * Set whether the connection should be encrypted
     */
    public void setEncryption( Encryption encryption )
    {
        this.encryption = encryption;
    }

    public boolean getDebugMode()
    {
        return debugMode;
    }

    /**
     * Enable/disable debug mode
     */
    void setDebugMode( boolean enabled )
    {
        this.debugMode = enabled;
    }

    public boolean getNonInteractive()
    {
        return nonInteractive;
    }

    /**
     * Force the shell to use non-interactive mode. Only useful on systems where auto-detection fails, such as Windows.
     */
    public void setNonInteractive( boolean nonInteractive )
    {
        this.nonInteractive = nonInteractive;
    }

    public String getInputFilename()
    {
        return inputFilename;
    }

    /**
     * Sets a filename where to read Cypher statements from, much like piping statements from a file.
     */
    public void setInputFilename( String inputFilename )
    {
        this.inputFilename = inputFilename;
    }

    public boolean getVersion()
    {
        return version;
    }

    public void setVersion( boolean version )
    {
        this.version = version;
    }

    public boolean getDriverVersion()
    {
        return driverVersion;
    }

    public void setDriverVersion( boolean version )
    {
        this.driverVersion = version;
    }

    public boolean isStringShell()
    {
        return cypher.isPresent();
    }

    public boolean getWrap()
    {
        return wrap;
    }

    public void setWrap( boolean wrap )
    {
        this.wrap = wrap;
    }

    public int getNumSampleRows()
    {
        return numSampleRows;
    }

    public void setNumSampleRows( Integer numSampleRows )
    {
        if ( numSampleRows != null && numSampleRows > 0 )
        {
            this.numSampleRows = numSampleRows;
        }
    }

    public List<RawParameter> getParameters()
    {
        return parameters;
    }

    public void setChangePassword( boolean changePassword )
    {
        this.changePassword = changePassword;
    }

    public boolean getChangePassword()
    {
        return changePassword;
    }

    public ConnectionConfig connectionConfig()
    {
        return new ConnectionConfig( getScheme(), getHost(), getPort(), getUsername(), getPassword(), getEncryption(), getDatabase(), new Environment() );
    }

    public File getHistoryFile()
    {
        return historyFile;
    }

    public void setHistoryFile( File historyFile )
    {
        this.historyFile = historyFile;
    }
}
