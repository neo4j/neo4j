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
import org.apache.logging.log4j.core.layout.PatternLayout;

import static org.apache.logging.log4j.core.layout.PatternLayout.newSerializerBuilder;

@Plugin( name = "Neo4jJsonLogLayout", category = Node.CATEGORY, elementType = Layout.ELEMENT_TYPE, printObject = true )
public class Neo4jJsonLogLayout extends Neo4jLogLayout
{
    private final Serializer eventSerializerThrowable;

    protected Neo4jJsonLogLayout( String datepattern, boolean category, DefaultConfiguration config )
    {

        super( category ?
               "{\"time\": \"" + datepattern + "\", \"level\": \"%p\", \"category\": \"%c{1.}\", \"message\": \"%enc{%m}{JSON}\"}%n" :
               "{\"time\": \"" + datepattern + "\", \"level\": \"%p\", \"message\": \"%enc{%m}{JSON}\"}%n",
                config );

        PatternLayout.SerializerBuilder patternBuilder = newSerializerBuilder()
                .setConfiguration( config )
                .setAlwaysWriteExceptions( true )
                .setDisableAnsi( false )
                .setNoConsoleNoAnsi( false );

        if ( category )
        {
            patternBuilder.setPattern( "{\"time\": \"" + datepattern +
                                       "\", \"level\": \"%p\", \"category\": \"%c{1.}\", \"message\": \"%enc{%m}{JSON}\"," +
                                       "%throwable{none} \"stacktrace\": \"%enc{%throwable}{JSON}\"}%n" );
        }
        else
        {
            patternBuilder.setPattern( "{\"time\": \"" + datepattern +
                                       "\", \"level\": \"%p\", \"message\": \"%enc{%m}{JSON}\"," +
                                       "%throwable{none} \"stacktrace\": \"%enc{%throwable}{JSON}\"}%n" );
        }

        this.eventSerializerThrowable = patternBuilder.build();
    }

    @PluginFactory
    public static Neo4jJsonLogLayout createLayout( @PluginAttribute( "datepattern" ) String datepattern,
            @PluginAttribute( value = "category", defaultBoolean = true ) boolean category )
    {
        return new Neo4jJsonLogLayout( datepattern, category, new DefaultConfiguration() );
    }

    @Override
    public String toSerializable( LogEvent event )
    {
        if ( event.getThrown() == null )
        {
            return eventSerializer.toSerializable( event );
        }
        return eventSerializerThrowable.toSerializable( event );
    }
}
