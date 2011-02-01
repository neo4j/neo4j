/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package org.neo4j.server.osgi.bundles.noisy;

import org.neo4j.server.osgi.bundles.BundleJarProducer;
import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.service.log.LogService;
import org.osgi.util.tracker.ServiceTracker;

/**
 * A bundle that logs to the OSGi LogService
 */
public class NoisyBundle extends BundleJarProducer implements BundleActivator
{

    @Override
    public String getBundleSymbolicName()
    {
        return "NoisyBundle";
    }

    @Override
    protected Class[] getExtraBundleClasses()
    {
        return new Class[0];
    }

    @Override
    public String getImportedPackages()
    {
        return super.getImportedPackages() + ", org.osgi.util.tracker";
    }

    @Override
    public void start( BundleContext bundleContext ) throws Exception
    {
        ServiceTracker logServiceTracker = new ServiceTracker( bundleContext, org.osgi.service.log.LogService.class.getName(), null );
        logServiceTracker.open();
        LogService logservice = (LogService) logServiceTracker.getService();

        if ( logservice != null )
            logservice.log( LogService.LOG_INFO, getMessageSentToLogService() );

    }

    @Override
    public void stop( BundleContext bundleContext ) throws Exception
    {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    public String getMessageSentToLogService()
    {
        return "Singing do-wa-diddy diddy-dum diddy-do";
    }
}
