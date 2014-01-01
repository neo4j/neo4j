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
package org.neo4j.test;

import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.graphdb.index.IndexProvider;
import org.neo4j.helpers.Service;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.IdGeneratorFactory;
import org.neo4j.kernel.InternalAbstractGraphDatabase;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.impl.cache.CacheProvider;
import org.neo4j.kernel.impl.nioneo.store.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.xaframework.TransactionInterceptorProvider;

import static org.neo4j.helpers.collection.MapUtil.stringMap;

public class IdJumpingGraphDatabase extends InternalAbstractGraphDatabase
{
    private final int sizePerJump;

    public IdJumpingGraphDatabase( String path, Map<String, String> params, int sizePerJump )
    {
        super( path, disableMemoryMapping( params ),
                Iterables.<Class<?>, Class<?>>iterable( (Class<?>) GraphDatabaseSettings.class ),
                Service.load( IndexProvider.class ), Iterables.<KernelExtensionFactory<?>,
                KernelExtensionFactory>cast( Service.load( KernelExtensionFactory.class ) ),
                Service.load( CacheProvider.class ), Service.load( TransactionInterceptorProvider.class ) );
        this.sizePerJump = sizePerJump;
        run();
    }

    private static Map<String, String> disableMemoryMapping( Map<String, String> params )
    {
        return stringMap( new HashMap<String,String>( params ),
                Config.USE_MEMORY_MAPPED_BUFFERS, "false",
                "neostore.nodestore.db.mapped_memory", "0M",
                "neostore.relationshipstore.db.mapped_memory", "0M",
                "neostore.propertystore.db.mapped_memory", "0M",
                "neostore.propertystore.db.strings.mapped_memory", "0M",
                "neostore.propertystore.db.arrays.mapped_memory", "0M" );
    }

    @Override
    protected IdGeneratorFactory createIdGeneratorFactory()
    {
        return new IdJumpingIdGeneratorFactory( sizePerJump );
    }

    @Override
    protected FileSystemAbstraction createFileSystemAbstraction()
    {
        return life.add( new IdJumpingFileSystemAbstraction( sizePerJump ) );
    }
}