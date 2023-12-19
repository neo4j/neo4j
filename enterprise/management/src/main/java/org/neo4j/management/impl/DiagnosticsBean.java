/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.management.impl;

import java.io.StringWriter;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import javax.management.NotCompliantMBeanException;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Service;
import org.neo4j.jmx.impl.ManagementBeanProvider;
import org.neo4j.jmx.impl.ManagementData;
import org.neo4j.jmx.impl.Neo4jMBean;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.info.DiagnosticsManager;
import org.neo4j.kernel.info.DiagnosticsProvider;
import org.neo4j.logging.FormattedLog;
import org.neo4j.management.Diagnostics;

@Service.Implementation( ManagementBeanProvider.class )
public class DiagnosticsBean extends ManagementBeanProvider
{
    public DiagnosticsBean()
    {
        super( Diagnostics.class );
    }

    @Override
    protected Neo4jMBean createMBean( ManagementData management ) throws NotCompliantMBeanException
    {
        return new DiagnosticsImpl( management );
    }

    private static class DiagnosticsImpl extends Neo4jMBean implements Diagnostics
    {
        private final DiagnosticsManager diagnostics;
        private Config config;

        DiagnosticsImpl( ManagementData management ) throws NotCompliantMBeanException
        {
            super( management );
            DependencyResolver resolver = management.getKernelData().graphDatabase().getDependencyResolver();
            config = resolver.resolveDependency( Config.class );
            this.diagnostics = resolver.resolveDependency( DiagnosticsManager.class );
        }

        @Override
        public void dumpToLog()
        {
            diagnostics.dumpAll();
        }

        @Override
        public List<String> getDiagnosticsProviders()
        {
            List<String> result = new ArrayList<>();
            for ( DiagnosticsProvider provider : diagnostics )
            {
                result.add( provider.getDiagnosticsIdentifier() );
            }
            return result;
        }

        @Override
        public void dumpToLog( String providerId )
        {
            diagnostics.dump( providerId );
        }

        @Override
        public String dumpAll(  )
        {
            StringWriter stringWriter = new StringWriter();
            ZoneId zoneId = config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
            FormattedLog.Builder logBuilder = FormattedLog.withZoneId( zoneId );
            diagnostics.dumpAll( logBuilder.toWriter( stringWriter ) );
            return stringWriter.toString();
        }

        @Override
        public String extract( String providerId )
        {
            StringWriter stringWriter = new StringWriter();
            ZoneId zoneId = config.get( GraphDatabaseSettings.db_timezone ).getZoneId();
            FormattedLog.Builder logBuilder = FormattedLog.withZoneId( zoneId );
            diagnostics.extract( providerId, logBuilder.toWriter( stringWriter ) );
            return stringWriter.toString();
        }
    }
}
