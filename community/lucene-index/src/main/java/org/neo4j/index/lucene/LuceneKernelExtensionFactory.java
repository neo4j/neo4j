/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.index.lucene;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import org.neo4j.index.impl.lucene.explicit.LuceneIndexImplementation;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.spi.KernelContext;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.spi.explicitindex.IndexProviders;

/**
 * @deprecated removed in 4.0
 */
@Deprecated
public class LuceneKernelExtensionFactory extends KernelExtensionFactory<LuceneKernelExtensionFactory.Dependencies>
{
    /**
     * @deprecated removed in 4.0
     */
    @Deprecated
    public interface Dependencies
    {
        Config getConfig();

        org.neo4j.kernel.spi.legacyindex.IndexProviders getIndexProviders();

        IndexConfigStore getIndexStore();

        FileSystemAbstraction fileSystem();
    }

    /**
     * @deprecated removed in 4.0
     */
    @Deprecated
    public LuceneKernelExtensionFactory()
    {
        super( LuceneIndexImplementation.SERVICE_NAME );
    }

    @Override
    public Lifecycle newInstance( KernelContext context, Dependencies dependencies ) throws Throwable
    {
        IndexProviders indexProvider = createImposterOf( IndexProviders.class, dependencies.getIndexProviders() );
        return new org.neo4j.kernel.api.impl.index.LuceneKernelExtension(
                context.storeDir(),
                dependencies.getConfig(),
                dependencies::getIndexStore,
                dependencies.fileSystem(),
                indexProvider,
                context.databaseInfo().operationalMode );
    }

    /**
     * Create an imposter of an interface. This is effectively used to mimic duck-typing.
     *
     * @param target the interface to mimic.
     * @param imposter the instance of any class, it has to implement all methods of the interface provided by {@code target}.
     * @param <T> the type of interface to mimic.
     * @param <F> the actual type of the imposter.
     * @return an imposter that can be passed as the type of mimicked interface.
     *
     * @implNote Method conformity is never checked, this is up to the user of the function to ensure. Sharp tool, use
     * with caution.
     */
    @SuppressWarnings( "unchecked" )
    private static <T,F> T createImposterOf( Class<T> target, F imposter )
    {
        return (T)Proxy.newProxyInstance( target.getClassLoader(), new Class<?>[]{target}, new MirroredInvocationHandler<>( imposter ) );
    }

    /**
     * Will pass through everything, as is, to the wrapped instance.
     */
    private static class MirroredInvocationHandler<F> implements InvocationHandler
    {
        private final F wrapped;

        MirroredInvocationHandler( F wrapped )
        {
            this.wrapped = wrapped;
        }

        @Override
        public Object invoke( Object proxy, Method method, Object[] args ) throws Throwable
        {
            Method match = wrapped.getClass().getMethod(method.getName(), method.getParameterTypes());
            return match.invoke( wrapped, args);
        }
    }
}
