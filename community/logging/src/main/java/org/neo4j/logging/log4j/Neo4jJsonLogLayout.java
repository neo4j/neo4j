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
package org.neo4j.logging.log4j;

import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.DefaultConfiguration;
import org.apache.logging.log4j.core.config.Node;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.pattern.DatePatternConverter;
import org.apache.logging.log4j.core.pattern.NameAbbreviator;
import org.apache.logging.log4j.core.pattern.ThrowablePatternConverter;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.util.StringBuilderFormattable;

import static org.apache.logging.log4j.util.StringBuilders.escapeJson;

@Plugin( name = "Neo4jJsonLogLayout", category = Node.CATEGORY, elementType = Layout.ELEMENT_TYPE, printObject = true )
public class Neo4jJsonLogLayout extends Neo4jLogLayout
{
    private final DatePatternConverter datePatternConverter;
    private final ThrowablePatternConverter throwablePatternConverter;
    private final NameAbbreviator abbreviator;
    private final boolean includeCategory;

    protected Neo4jJsonLogLayout( String datePattern, String timeZone, boolean includeCategory, DefaultConfiguration config )
    {
        super( "", config );
        this.includeCategory = includeCategory;
        datePatternConverter = DatePatternConverter.newInstance( new String[]{datePattern, timeZone} );
        throwablePatternConverter = ThrowablePatternConverter.newInstance( config, null );
        abbreviator = NameAbbreviator.getAbbreviator( "1." );
    }

    @PluginFactory
    public static Neo4jJsonLogLayout createLayout(
            @PluginAttribute( "datePattern" ) String datePattern,
            @PluginAttribute( "timeZone" ) String timeZone,
            @PluginAttribute( value = "includeCategory", defaultBoolean = true ) boolean includeCategory )
    {
        return new Neo4jJsonLogLayout( datePattern, timeZone, includeCategory, new DefaultConfiguration() );
    }

    @Override
    public String toSerializable( LogEvent event )
    {
        StringBuilder buffer = new StringBuilder( 128 );
        buffer.append( "{\"time\":\"" );
        datePatternConverter.format( event, buffer );
        buffer.append( "\",\"level\":\"" );
        buffer.append( event.getLevel().toString() );
        buffer.append( '"' );
        if ( includeCategory )
        {
            buffer.append( ",\"category\":\"" );
            abbreviator.abbreviate( event.getLoggerName(), buffer );
            buffer.append( '"' );
        }

        Message message = event.getMessage();
        if ( message instanceof StructureAwareMessage )
        {
            StructureAwareMessage msg = (StructureAwareMessage) message;
            msg.asStructure( new JsonFieldConsumer( buffer ) );
        }
        else  // Normal message
        {
            buffer.append( ",\"message\":\"" );
            final int start = buffer.length();
            if ( message instanceof StringBuilderFormattable )
            {
                ((StringBuilderFormattable) message).formatTo( buffer ); // Garbage-free optimized message
            }
            else
            {
                buffer.append( message.getFormattedMessage() );
            }
            escapeJson( buffer, start );
            buffer.append( '"' );
        }

        if ( event.getThrown() != null )
        {
            buffer.append( ",\"stacktrace\":\"" );
            int start = buffer.length();
            throwablePatternConverter.format( event, buffer );
            escapeJson( buffer, start );
            buffer.append( '"' );
        }
        buffer.append( '}' ).append( System.lineSeparator() );
        return buffer.toString();
    }

    private static class JsonFieldConsumer implements StructureAwareMessage.FieldConsumer
    {
        private final StringBuilder buffer;

        private JsonFieldConsumer( StringBuilder buffer )
        {
            this.buffer = buffer;
        }

        @Override
        public void add( String field, String value )
        {
            addField( buffer, field );
            formatString( buffer, value );
        }

        @Override
        public void add( String field, long value )
        {
            addField( buffer, field );
            buffer.append( value );
        }

        private static void addField( StringBuilder buffer, String field )
        {
            buffer.append( ",\"" ).append( field ).append( "\":" );
        }

        private static void formatString( StringBuilder buffer, Object value )
        {
            buffer.append( '"' );
            int startIndex = buffer.length();
            buffer.append( value );
            escapeJson( buffer, startIndex );
            buffer.append( '"' );
        }
    }
}
