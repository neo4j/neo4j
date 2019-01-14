/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.upgrade.lucene;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.file.Path;
import java.util.function.Supplier;

import org.neo4j.upgrade.loader.EmbeddedJarLoader;

/**
 * Index upgrader is a container for loaded lucene native upgrader.
 * <p>
 * During first attempt of index upgrade original migrator will be loaded and will be reused during further
 * invocations.
 * As soon as upgrade completed, index upgrader should be closed.
 *
 * @see EmbeddedJarLoader
 */
class IndexUpgraderWrapper implements AutoCloseable
{
    private static final String LUCENE_INDEX_UPGRADER_CLASS_NAME = "org.apache.lucene.index.IndexUpgrader";

    private EmbeddedJarLoader luceneLoader;
    private MethodHandle mainMethod;
    private Supplier<EmbeddedJarLoader> jarLoaderSupplier;

    IndexUpgraderWrapper( Supplier<EmbeddedJarLoader> jarLoaderSupplier )
    {
        this.jarLoaderSupplier = jarLoaderSupplier;
    }

    public void upgradeIndex( Path indexPath ) throws Throwable
    {
        // since lucene use ServiceLocator to load services, context class loader need to be replaced as well
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try
        {
            if ( mainMethod == null )
            {
                luceneLoader = jarLoaderSupplier.get();
                Class upgrader = luceneLoader.loadEmbeddedClass( LUCENE_INDEX_UPGRADER_CLASS_NAME );
                MethodHandles.Lookup lookup = MethodHandles.lookup();
                mainMethod = lookup.findStatic( upgrader, "main", MethodType.methodType( void.class, String[].class ) );
            }
            Thread.currentThread().setContextClassLoader( luceneLoader.getJarsClassLoader() );
            mainMethod.invokeExact( new String[]{indexPath.toString()} );
        }
        finally
        {
            Thread.currentThread().setContextClassLoader( contextClassLoader );
        }
    }

    @Override
    public void close() throws Exception
    {
        if ( luceneLoader != null )
        {
            luceneLoader.close();
        }
    }
}
