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
package org.neo4j.server.osgi.bundles;

import org.ops4j.io.StreamUtils;
import org.ops4j.pax.swissbox.tinybundles.core.TinyBundle;
import org.osgi.framework.Constants;

import java.io.*;

import static org.ops4j.pax.swissbox.tinybundles.core.TinyBundles.newBundle;
import static org.ops4j.pax.swissbox.tinybundles.core.TinyBundles.withBnd;

/**
 * Base class for producing a bundle jar file.
 */
public abstract class BundleJarProducer
{
    public abstract String getBundleSymbolicName();

    protected abstract Class[] getExtraBundleClasses();

    public String getImportedPackages()
    {
        return "org.osgi.framework, org.osgi.service.log," + this.getClass().getPackage().getName();
    }

    public String getExportedPackages()
    {
        return this.getClass().getPackage().getName();
    }

    public void produceJar( String bundleDirectory, String jarName ) throws IOException
    {
        TinyBundle bundle = newBundle()
                .add( BundleJarProducer.class )
                .add( this.getClass() )
                .set( Constants.BUNDLE_SYMBOLICNAME, getBundleSymbolicName() )
                .set( Constants.EXPORT_PACKAGE, getExportedPackages() )
                .set( Constants.IMPORT_PACKAGE, getImportedPackages() )
                .set( Constants.BUNDLE_ACTIVATOR, this.getClass().getName() );

        for (Class bundleClass : getExtraBundleClasses()) {
            bundle.add( bundleClass);
        }
        InputStream bundleStream = bundle
                .build( withBnd() );
        File jarFile = new File( bundleDirectory, jarName );
        OutputStream jarOutputStream = new FileOutputStream( jarFile );
        StreamUtils.copyStream( bundleStream, jarOutputStream, true );

    }

}
