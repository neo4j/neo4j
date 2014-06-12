/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.index;

import java.util.Map;

import org.neo4j.graphdb.index.IndexProviders;
import org.neo4j.graphdb.index.IndexTransactionSPI;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.api.LegacyIndexChanges;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.lifecycle.Lifecycle;

public class DummyIndexExtensionFactory extends
        KernelExtensionFactory<DummyIndexExtensionFactory.Dependencies> implements IndexTransactionSPI, Lifecycle
{
    static final String IDENTIFIER = "test-dummy-neo-index";
    private InternalAbstractGraphDatabase db;
    private IndexProviders indexProviders;

    public DummyIndexExtensionFactory()
    {
        super( IDENTIFIER );
    }

    public interface Dependencies
    {
        InternalAbstractGraphDatabase getDatabase();

        IndexProviders getIndexProviders();
    }

    @Override
    public Lifecycle newKernelExtension( Dependencies dependencies ) throws Throwable
    {
        db = dependencies.getDatabase();
        indexProviders = dependencies.getIndexProviders();
        return this;
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        indexProviders.registerIndexProvider( IDENTIFIER, this );
    }

    @Override
    public void stop() throws Throwable
    {
        indexProviders.unregisterIndexProvider( IDENTIFIER );
    }

    @Override
    public void shutdown() throws Throwable
    {
    }

    @Override
    public Map<String, String> fillInDefaults( Map<String, String> config )
    {
        return config;
    }

    @Override
    public boolean configMatches( Map<String, String> storedConfig, Map<String, String> config )
    {
        return true;
    }

    @Override
    public LegacyIndexChanges newTransactionState()
    {
        throw new UnsupportedOperationException( "Please implement" );
    }
}
