/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.messaging;

import java.util.Arrays;

import org.neo4j.logging.Log;

import static java.lang.String.format;

public class LoggingEventHandler implements EventHandler
{
    private final EventId id;
    private Log log;

    public static LoggingEventHandler newEvent( EventId id, Log log )
    {
        return new LoggingEventHandler( id, log );
    }

    public static LoggingEventHandler newEvent( Log log )
    {
        return newEvent( EventId.create(), log );
    }

    private LoggingEventHandler( EventId id, Log log )
    {
        this.id = id;
        this.log = log;
    }

    @Override
    public void on( EventState eventState, String message, Throwable throwable, Param... params )
    {
        checkForNull( eventState, "eventState", EventState.class );
        checkForNull( params, "params", Param.class );
        if ( eventState == EventState.Info || eventState == EventState.Begin || eventState == EventState.End )
        {
            log.info( asString( eventState, message, throwable, params ) );
        }
        else if ( eventState == EventState.Warn )
        {
            if ( throwable == null )
            {
                log.warn( asString( eventState, message, null, params ) );
            }
            else
            {
                log.warn( asString( eventState, message, null, params ), throwable );
            }
        }
        else if ( eventState == EventState.Error )
        {
            if ( throwable == null )
            {
                log.error( asString( eventState, message, null, params ) );
            }
            else
            {
                log.error( asString( eventState, message, null, params ), throwable );
            }
        }
    }

    private void checkForNull( Object arg, String name, Class aClass )
    {
        if ( arg == null )
        {
            throw new IllegalArgumentException( format( "%s cannot be null [%s]", name, aClass ) );
        }
    }

    private String asString( EventState eventState, String message, Throwable throwable, Param[] params )
    {
        return format( "%s - %s - %s%s%s", id, padRight( eventState.name(), 5 ), message, printArray( params ), maybeFormatExcpetion( throwable ) );
    }

    private String printArray( Object[] objects )
    {
        if ( objects != null && objects.length != 0 )
        {
            return " " + Arrays.toString( objects );
        }
        else
        {
            return "";
        }
    }

    private StringBuilder padRight( String name, int size )
    {
        int pad = Integer.max( size - name.length(), 0 );
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append( name );
        for ( int i = 0; i < pad; i++ )
        {
            stringBuilder.append( " " );
        }
        return stringBuilder;
    }

    private String maybeFormatExcpetion( Throwable throwable )
    {
        return throwable == null ? "" : format( ". %s", throwable );
    }
}
