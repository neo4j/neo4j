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
package org.neo4j.unsafe.batchinsert;

import static org.junit.Assert.assertEquals;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.TargetDirectory.forTest;
import static org.neo4j.unsafe.batchinsert.BatchInserters.inserter;

import java.io.File;

import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.Predicate;
import org.neo4j.kernel.DefaultFileSystemAbstraction;
import org.neo4j.kernel.DefaultIdGeneratorFactory;
import org.neo4j.kernel.DefaultTxHook;
import org.neo4j.kernel.api.impl.index.DirectoryFactory;
import org.neo4j.kernel.api.impl.index.LuceneSchemaIndexProvider;
import org.neo4j.kernel.api.index.InternalIndexState;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.nioneo.store.DefaultWindowPoolFactory;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule;
import org.neo4j.kernel.impl.nioneo.store.SchemaRule.Kind;
import org.neo4j.kernel.impl.nioneo.store.StoreFactory;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;

public class TestLuceneSchemaBatchInsertIT
{
    @Test
    public void shouldLoadAndUseLuceneProvider() throws Exception
    {
        // GIVEN
        String storeDir = forTest( getClass() ).graphDbDir( true ).getAbsolutePath();
        BatchInserter inserter = inserter( storeDir );
        inserter.createDeferredSchemaIndex( LABEL ).on( "name" ).create();
        
        // WHEN
        inserter.createNode( map( "name", "Mattias" ), LABEL );
        inserter.shutdown();

        // THEN
        Config config = new Config( stringMap( GraphDatabaseSettings.store_dir.name(), storeDir ) );
        LifeSupport life = new LifeSupport();
        LuceneSchemaIndexProvider provider = life.add( new LuceneSchemaIndexProvider( DirectoryFactory.PERSISTENT, config ) );
        StoreFactory storeFactory = new StoreFactory( config, new DefaultIdGeneratorFactory(),
                new DefaultWindowPoolFactory(), new DefaultFileSystemAbstraction(), StringLogger.DEV_NULL,
                new DefaultTxHook() );
        NeoStore neoStore = storeFactory.newNeoStore( new File( storeDir, NeoStore.DEFAULT_NAME ) );
        life.start();
        SchemaRule rule = single( filter( new Predicate<SchemaRule>()
        {
            @Override
            public boolean accept( SchemaRule item )
            {
                return item.getKind() == Kind.INDEX_RULE;
            }
        }, neoStore.getSchemaStore() ) );
        InternalIndexState initialState = provider.getInitialState( rule.getId() );
        assertEquals( InternalIndexState.ONLINE, initialState );
        
        // CLEANUP
        life.shutdown();
        neoStore.close();
    }
    
    private static final Label LABEL = label( "Person" );
}
