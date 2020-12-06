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
package org.neo4j.shell.cli;

import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.neo4j.shell.ParameterMap;
import org.neo4j.shell.ShellParameterMap;

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
    private ParameterMap parameters = new ShellParameterMap();

    /**
     * Set the scheme to the primary value, or if null, the fallback value.
     */
    public void setScheme( @Nullable String primary, @Nonnull String fallback )
    {
        scheme = primary == null ? fallback : primary;
    }

    /**
     * Set the host to the primary value, or if null, the fallback value.
     */
    void setHost( @Nullable String primary, @Nonnull String fallback )
    {
        host = primary == null ? fallback : primary;
    }

    /**
     * Set the username to the primary value, or if null, the fallback value.
     */
    public void setUsername( @Nullable String primary, @Nonnull String fallback )
    {
        username = primary == null ? fallback : primary;
    }

    /**
     * Set the password to the primary value, or if null, the fallback value.
     */
    public void setPassword( @Nullable String primary, @Nonnull String fallback )
    {
        password = primary == null ? fallback : primary;
    }

    @Nonnull
    public String getScheme()
    {
        return scheme;
    }

    @Nonnull
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

    @Nonnull
    public String getUsername()
    {
        return username;
    }

    @Nonnull
    public String getPassword()
    {
        return password;
    }

    @Nonnull
    public String getDatabase()
    {
        return databaseName;
    }

    /**
     * Set the database to connect to.
     */
    public void setDatabase( @Nullable String databaseName )
    {
        this.databaseName = databaseName;
    }

    @Nonnull
    public FailBehavior getFailBehavior()
    {
        return failBehavior;
    }

    /**
     * Set the desired fail behavior
     */
    void setFailBehavior( @Nonnull FailBehavior failBehavior )
    {
        this.failBehavior = failBehavior;
    }

    @Nonnull
    public Optional<String> getCypher()
    {
        return cypher;
    }

    /**
     * Set the specified cypher string to execute
     */
    public void setCypher( @Nullable String cypher )
    {
        this.cypher = Optional.ofNullable( cypher );
    }

    @Nonnull
    public Format getFormat()
    {
        return format;
    }

    /**
     * Set the desired format
     */
    public void setFormat( @Nonnull Format format )
    {
        this.format = format;
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

    public ParameterMap getParameters()
    {
        return parameters;
    }
}
