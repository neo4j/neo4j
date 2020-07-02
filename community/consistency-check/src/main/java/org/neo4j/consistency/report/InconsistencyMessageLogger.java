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
package org.neo4j.consistency.report;

import org.neo4j.consistency.RecordType;
import org.neo4j.internal.helpers.Strings;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.logging.Log;

import static org.neo4j.internal.helpers.Strings.TAB;

public class InconsistencyMessageLogger implements InconsistencyLogger
{
    private final Log log;

    public InconsistencyMessageLogger( Log log )
    {
        this.log = log;
    }

    @Override
    public void error( RecordType recordType, AbstractBaseRecord record, String message, Object... args )
    {
        log.error( buildMessage( message, record, args ) );
    }

    @Override
    public void error( RecordType recordType, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord,
                       String message, Object... args )
    {
        log.error( buildMessage( message, oldRecord, newRecord, args ) );
    }

    @Override
    public void error( String message )
    {
        log.error( buildMessage( message ) );
    }

    @Override
    public void warning( RecordType recordType, AbstractBaseRecord record, String message, Object... args )
    {
        log.warn( buildMessage( message, record, args ) );
    }

    @Override
    public void warning( RecordType recordType, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord,
                         String message, Object... args )
    {
        log.warn( buildMessage( message, oldRecord, newRecord, args ) );
    }

    @Override
    public void warning( String message )
    {
        log.warn( buildMessage( message ) );
    }

    private static String buildMessage( String message )
    {
        StringBuilder builder = tabAfterLinebreak( message );
        return builder.toString();
    }

    private static String buildMessage( String message, AbstractBaseRecord record, Object... args )
    {
        StringBuilder builder = joinLines( message ).append( System.lineSeparator() ).append( TAB ).append( safeToString( record ) );
        appendArgs( builder, args );
        return builder.toString();
    }

    private static String safeToString( AbstractBaseRecord record )
    {
        try
        {
            return record.toString();
        }
        catch ( Exception e )
        {
            return String.format( "%s[%d,Error generating toString: %s]", record.getClass().getSimpleName(), record.getId(), e );
        }
    }

    private static String buildMessage( String message, AbstractBaseRecord oldRecord, AbstractBaseRecord newRecord, Object... args )
    {
        StringBuilder builder = joinLines( message );
        builder.append( System.lineSeparator() ).append( TAB ).append( "- " ).append( oldRecord );
        builder.append( System.lineSeparator() ).append( TAB ).append( "+ " ).append( newRecord );
        appendArgs( builder, args );
        return builder.toString();
    }

    private static StringBuilder tabAfterLinebreak( String message )
    {
        String[] lines = message.split( "\n" );
        StringBuilder builder = new StringBuilder( lines[0].trim() );
        for ( int i = 1; i < lines.length; i++ )
        {
            builder.append( System.lineSeparator() ).append( TAB ).append( lines[i].trim() );
        }
        return builder;
    }

    private static StringBuilder joinLines( String message )
    {
        String[] lines = message.split( "\n" );
        StringBuilder builder = new StringBuilder( lines[0].trim() );
        for ( int i = 1; i < lines.length; i++ )
        {
            builder.append( ' ' ).append( lines[i].trim() );
        }
        return builder;
    }

    private static StringBuilder appendArgs( StringBuilder builder, Object[] args )
    {
        if ( args == null || args.length == 0 )
        {
            return builder;
        }
        builder.append( System.lineSeparator() ).append( TAB ).append( "Inconsistent with:" );
        for ( Object arg : args )
        {
            builder.append( ' ' ).append( Strings.prettyPrint( arg ) );
        }
        return builder;
    }
}
