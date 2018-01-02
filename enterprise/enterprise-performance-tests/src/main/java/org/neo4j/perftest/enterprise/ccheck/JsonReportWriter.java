/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.perftest.enterprise.ccheck;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.DefaultPrettyPrinter;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.perftest.enterprise.generator.DataGenerator;
import org.neo4j.perftest.enterprise.util.Configuration;
import org.neo4j.perftest.enterprise.util.Setting;

import static org.neo4j.io.fs.FileUtils.newFilePrintWriter;
import static org.neo4j.kernel.impl.util.Charsets.UTF_8;
import static org.neo4j.perftest.enterprise.util.Configuration.settingsOf;

class JsonReportWriter implements TimingProgress.Visitor
{
    private final File target;
    private JsonGenerator json;
    private boolean writeRecordsPerSecond = true;
    private final Configuration configuration;
    private final Config tuningConfiguration;

    JsonReportWriter( Configuration configuration, Config tuningConfiguration )
    {
        this.configuration = configuration;
        this.tuningConfiguration = tuningConfiguration;
        target = new File( configuration.get( ConsistencyPerformanceCheck.report_file ) );
    }

    @Override
    public void beginTimingProgress( long totalElementCount, long totalTimeNanos ) throws IOException
    {
        ensureOpen( false );
        json = new JsonFactory().configure( JsonGenerator.Feature.AUTO_CLOSE_TARGET, true )
                                .createJsonGenerator( newFilePrintWriter( target, UTF_8 ) );
        json.setPrettyPrinter( new DefaultPrettyPrinter() );
        json.writeStartObject();
        {
            json.writeFieldName( "config" );
            json.writeStartObject();
            emitConfiguration();
            json.writeEndObject();
        }
        {
            json.writeFieldName( "tuningConfig" );
            json.writeStartObject();
            emitTuningConfiguration();
            json.writeEndObject();
        }
        {
            json.writeFieldName( "total" );
            json.writeStartObject();
            emitTime( totalElementCount, totalTimeNanos );
            json.writeEndObject();
        }
        json.writeFieldName( "phases" );
        json.writeStartArray();
    }

    private void emitConfiguration() throws IOException
    {
        for ( Setting<?> setting : settingsOf( DataGenerator.class, ConsistencyPerformanceCheck.class ) )
        {
            emitSetting( setting );
        }
    }

    private <T> void emitSetting( Setting<T> setting ) throws IOException
    {
        json.writeStringField( setting.name(), setting.asString( configuration.get( setting ) ) );
    }

    private void emitTuningConfiguration() throws IOException
    {
        Map<String,String> params = new TreeMap<String,String>(tuningConfiguration.getParams());
        for ( String key : params.keySet() )
        {
            json.writeStringField( key, params.get( key ) );
        }
    }

    @Override
    public void phaseTimingProgress( String phase, long elementCount, long timeNanos ) throws IOException
    {
        ensureOpen( true );
        json.writeStartObject();
        json.writeStringField( "name", phase );
        emitTime( elementCount, timeNanos );
        json.writeEndObject();
    }

    private void emitTime( long elementCount, long timeNanos ) throws IOException
    {
        json.writeNumberField( "elementCount", elementCount );
        double millis = TimeLogger.nanosToMillis( timeNanos );
        json.writeNumberField( "time", millis );
        if ( writeRecordsPerSecond )
        {
            json.writeNumberField( "recordsPerSecond", (elementCount * 1000.0) / millis );
        }
    }

    @Override
    public void endTimingProgress() throws IOException
    {
        ensureOpen( true );
        json.writeEndArray();
        json.writeEndObject();
        json.close();
    }

    private void ensureOpen( boolean open ) throws IOException
    {
        if ( (json == null) == open )
        {
            throw new IOException(
                    new IllegalStateException( String.format( "Writing %s started.", open ? "not" : "already" ) ) );
        }
    }
}
