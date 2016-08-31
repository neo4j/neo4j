/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.enterprise;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Consumer;

import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.FormattedLog;
import org.neo4j.logging.Log;
import org.neo4j.logging.Logger;

import static org.neo4j.io.file.Files.createOrOpenAsOuputStream;
import static org.neo4j.kernel.impl.enterprise.configuration.EnterpriseEditionSettings.security_log_filename;

public class SecurityLog implements Log
{
    private final Log inner;

    SecurityLog( Config config, FileSystemAbstraction fileSystem )
    {
        inner = createLog( config, fileSystem );
    }

    private Log createLog( Config config, FileSystemAbstraction fileSystem )
    {
        FormattedLog.Builder builder = FormattedLog.withUTCTimeZone();
        File logFile = config.get( security_log_filename );
        OutputStream ouputStream;
        try
        {
            ouputStream = createOrOpenAsOuputStream( fileSystem, logFile, true );
        }
        catch ( IOException e )
        {
            throw new AssertionError( "File not possible to create", e );
        }
        return builder.toOutputStream( ouputStream );
    }

    @Override
    public boolean isDebugEnabled()
    {
        return inner.isDebugEnabled();
    }

    @Override
    public Logger debugLogger()
    {
        return inner.debugLogger();
    }

    @Override
    public void debug( String message )
    {
        inner.debug( message );
    }

    @Override
    public void debug( String message, Throwable throwable )
    {
        inner.debug( message, throwable );
    }

    @Override
    public void debug( String format, Object... arguments )
    {
        inner.debug( format, arguments );
    }

    @Override
    public Logger infoLogger()
    {
        return inner.infoLogger();
    }

    @Override
    public void info( String message )
    {
        inner.info( message );
    }

    @Override
    public void info( String message, Throwable throwable )
    {
        inner.info( message, throwable );
    }

    @Override
    public void info( String format, Object... arguments )
    {
        inner.info( format, arguments );
    }

    @Override
    public Logger warnLogger()
    {
        return inner.warnLogger();
    }

    @Override
    public void warn( String message )
    {
        inner.warn( message );
    }

    @Override
    public void warn( String message, Throwable throwable )
    {
        inner.warn( message, throwable );
    }

    @Override
    public void warn( String format, Object... arguments )
    {
        inner.warn( format, arguments );
    }

    @Override
    public Logger errorLogger()
    {
        return inner.errorLogger();
    }

    @Override
    public void error( String message )
    {
        inner.error( message );
    }

    @Override
    public void error( String message, Throwable throwable )
    {
        inner.error( message, throwable );
    }

    @Override
    public void error( String format, Object... arguments )
    {
        inner.error( format, arguments );
    }

    @Override
    public void bulk( Consumer<Log> consumer )
    {
        inner.bulk( consumer );
    }

}
