/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
            ZoneId zoneId = config.get( GraphDatabaseSettings.log_timezone ).getZoneId();
            FormattedLog.Builder logBuilder = FormattedLog.withZoneId( zoneId );
            diagnostics.dumpAll( logBuilder.toWriter( stringWriter ) );
            return stringWriter.toString();
        }

        @Override
        public String extract( String providerId )
        {
            StringWriter stringWriter = new StringWriter();
            ZoneId zoneId = config.get( GraphDatabaseSettings.log_timezone ).getZoneId();
            FormattedLog.Builder logBuilder = FormattedLog.withZoneId( zoneId );
            diagnostics.extract( providerId, logBuilder.toWriter( stringWriter ) );
            return stringWriter.toString();
        }
    }
}
