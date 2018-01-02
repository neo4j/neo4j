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
package org.neo4j.kernel.impl.api;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexCommand.AddRelationshipCommand;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.impl.transaction.command.CommandHandler;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.test.EphemeralFileSystemRule;

import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.api.TransactionApplicationMode.INTERNAL;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.kernel.impl.util.IdOrderingQueue.BYPASS;

public class LegacyIndexApplierTest
{
    @Rule
    public final LifeRule life = new LifeRule( true );
    @Rule
    public final EphemeralFileSystemRule fs = new EphemeralFileSystemRule();

    @Test
    public void shouldOnlyCreateOneApplierPerProvider() throws Exception
    {
        // GIVEN
        Map<String,Integer> names = MapUtil.genericMap( "first", 0, "second", 1 );
        Map<String,Integer> keys = MapUtil.genericMap( "key", 0 );
        String applierName = "test-applier";
        IndexConfigStore config = newIndexConfigStore( names, applierName );
        LegacyIndexApplierLookup applierLookup = mock( LegacyIndexApplierLookup.class );
        when( applierLookup.newApplier( anyString(), anyBoolean() ) ).thenReturn( mock( CommandHandler.class ) );
        try ( LegacyIndexApplier applier = new LegacyIndexApplier( config, applierLookup, BYPASS, BASE_TX_ID, INTERNAL ) )
        {
            // WHEN
            IndexDefineCommand definitions = definitions( names, keys );
            applier.visitIndexDefineCommand( definitions );
            applier.visitIndexAddNodeCommand( addNodeToIndex( definitions, "first" ) );
            applier.visitIndexAddNodeCommand( addNodeToIndex( definitions, "second" ) );
            applier.visitIndexAddRelationshipCommand( addRelationshipToIndex( definitions, "second" ) );
            applier.apply();
        }

        // THEN
        verify( applierLookup, times( 1 ) ).newApplier( eq( applierName ), anyBoolean() );
    }

    private static AddRelationshipCommand addRelationshipToIndex( IndexDefineCommand definitions, String indexName )
    {
        AddRelationshipCommand command = new AddRelationshipCommand();
        command.init( definitions.getOrAssignIndexNameId( indexName ), 0L, (byte) 0, null, 1, 2 );
        return command;
    }

    private static AddNodeCommand addNodeToIndex( IndexDefineCommand definitions, String indexName )
    {
        AddNodeCommand command = new AddNodeCommand();
        command.init( definitions.getOrAssignIndexNameId( indexName ), 0L, (byte) 0, null );
        return command;
    }

    private static IndexDefineCommand definitions( Map<String,Integer> names, Map<String,Integer> keys )
    {
        IndexDefineCommand definitions = new IndexDefineCommand();
        definitions.init( names, keys );
        return definitions;
    }

    private IndexConfigStore newIndexConfigStore( Map<String,Integer> names, String providerName )
    {
        File dir = new File( "conf" );
        EphemeralFileSystemAbstraction fileSystem = fs.get();
        fileSystem.mkdirs( dir );
        IndexConfigStore store = life.add( new IndexConfigStore( dir, fileSystem ) );
        for ( Map.Entry<String,Integer> name : names.entrySet() )
        {
            store.set( Node.class, name.getKey(), stringMap( IndexManager.PROVIDER, providerName ) );
            store.set( Relationship.class, name.getKey(), stringMap( IndexManager.PROVIDER, providerName ) );
        }
        return store;
    }
}
