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

import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;

import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.neo4j.causalclustering.messaging.EventHandler.Param.param;

public class LoggingEventHandlerTest
{
    @Test
    public void shouldLogAsExpected() throws IOException
    {
        StringWriter stringWriter = new StringWriter();
        LogProvider logProvider = FormattedLogProvider.toWriter( stringWriter );

        LoggingEventHandler loggingEventHandler = LoggingEventHandler.newEvent( EventId.from( "id" ), logProvider.getLog( "logName" ) );

        IllegalStateException exception = new IllegalStateException( "Exception Message" );
        loggingEventHandler.on( EventHandler.EventState.Info, "Message", param( "one", 1 ) );
        loggingEventHandler.on( EventHandler.EventState.Begin, "Message", exception, param( "one", 1 ) );
        loggingEventHandler.on( EventHandler.EventState.End, "Message", param( "one", 1 ) );
        loggingEventHandler.on( EventHandler.EventState.Warn, param( "one", 3 ) );
        loggingEventHandler.on( EventHandler.EventState.Error, "Message" );

        stringWriter.close();
        String output = stringWriter.getBuffer().toString();

        String[] split = output.split( System.lineSeparator() );
        assertEquals( 5, split.length );
        assertThat( split[0], endsWith( "[logName] id - Info  - Message [one: 1]" ) );
        assertThat( split[0], containsString( "INFO" ) );
        assertThat( split[1], endsWith( "[logName] id - Begin - Message [one: 1]. " + exception ) );
        assertThat( split[1], containsString( "INFO" ) );
        assertThat( split[2], endsWith( "[logName] id - End   - Message [one: 1]" ) );
        assertThat( split[2], containsString( "INFO" ) );
        assertThat( split[3], endsWith( "[logName] id - Warn  -  [one: 3]" ) );
        assertThat( split[3], containsString( "WARN" ) );
        assertThat( split[4], endsWith( "[logName] id - Error - Message" ) );
        assertThat( split[4], containsString( "ERROR" ) );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldThrowIllegalArgumentExceptionOnNullEventType()
    {
        LogProvider logProvider = NullLogProvider.getInstance();
        LoggingEventHandler loggingEventHandler = LoggingEventHandler.newEvent( EventId.from( "id" ), logProvider.getLog( "logName" ) );

        loggingEventHandler.on( null, "" );
    }

    @Test( expected = IllegalArgumentException.class )
    public void shouldThrowIllegalArgumentOnNullParam()
    {
        LogProvider logProvider = NullLogProvider.getInstance();
        LoggingEventHandler loggingEventHandler = LoggingEventHandler.newEvent( EventId.from( "id" ), logProvider.getLog( "logName" ) );

        loggingEventHandler.on( EventHandler.EventState.Info, "", null );
    }
}
