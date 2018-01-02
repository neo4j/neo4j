/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.kernel.extension;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;

public abstract class KernelExtensionFactory<DEPENDENCIES> extends Service
{
    protected KernelExtensionFactory( String key )
    {
        super( key );
    }

    /**
     * Return the class that contains GraphDatabaseSetting fields that define
     * the properties needed by this extension.
     *
     * @return a class or null if no settings are needed
     */
    public Class getSettingsClass()
    {
        return null;
    }
    
    /**
     * Create a new instance of this kernel extension.
     *
     * @param dependencies Dependencies
     * @return the lifecycle for the instance
     * @deprecated use {@link #newInstance(KernelContext, Object)} instead
     * @throws Throwable depends on the implementation
     */
    @Deprecated
    public Lifecycle newKernelExtension( DEPENDENCIES dependencies )
            throws Throwable
    {
        throw new UnsupportedOperationException( "deprecated" );
    }

    /**
     * Create a new instance of this kernel extension.
     *
     * @param context the context the extension should be created for
     * @param dependencies deprecated
     * @return the {@link Lifecycle} for the extension
     * @throws Throwable if there is an error
     */
    public Lifecycle newInstance( @SuppressWarnings( "unused" ) KernelContext context, DEPENDENCIES dependencies ) throws Throwable
    {
        return newKernelExtension( dependencies );
    }

    @Override
    public String toString()
    {
        return "KernelExtension:" + getClass().getSimpleName() + getKeys();
    }
}
