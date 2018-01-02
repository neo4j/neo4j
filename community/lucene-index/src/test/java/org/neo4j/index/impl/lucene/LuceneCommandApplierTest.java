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
package org.neo4j.index.impl.lucene;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.lifecycle.LifeRule;
import org.neo4j.test.EphemeralFileSystemRule;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.index.impl.lucene.LuceneIndexImplementation.EXACT_CONFIG;

public class LuceneCommandApplierTest
{
    public final @Rule EphemeralFileSystemRule fs = new EphemeralFileSystemRule();
    public final @Rule LifeRule life = new LifeRule( true );
    private final File dir = new File( "dir" );

    @Test
    public void shouldHandleMultipleIdSpaces() throws Exception
    {
        // GIVEN
        fs.get().mkdirs( dir );
        String indexName = "name", key = "key";
        IndexConfigStore configStore = new IndexConfigStore( dir, fs.get() );
        configStore.set( Node.class, indexName, EXACT_CONFIG );
        LuceneDataSource dataSource = life.add( spy( new LuceneDataSource( dir, new Config( stringMap(
                LuceneDataSource.Configuration.ephemeral.name(), Settings.TRUE ) ),
                configStore, fs.get() ) ) );

        try ( LuceneCommandApplier applier = new LuceneCommandApplier( dataSource, false ) )
        {
            // WHEN issuing a command where the index name is mapped to a certain id
            IndexDefineCommand definitions = definitions(
                    MapUtil.<String,Integer>genericMap( indexName, 0 ),
                    MapUtil.<String,Integer>genericMap( key, 0 ) );
            applier.visitIndexDefineCommand( definitions );
            applier.visitIndexAddNodeCommand( addNodeToIndex( definitions, indexName, 0L ) );
            // and then later issuing a command for that same index, but in another transaction where
            // the local index name id is a different one
            definitions = definitions(
                    MapUtil.<String,Integer>genericMap( indexName, 1 ),
                    MapUtil.<String,Integer>genericMap( key, 0 ) );
            applier.visitIndexDefineCommand( definitions );
            applier.visitIndexAddNodeCommand( addNodeToIndex( definitions, indexName, 1L ) );
            applier.apply();
        }

        // THEN both those updates should have been directed to the same index
        verify( dataSource, times( 1 ) ).getIndexSearcher( any( IndexIdentifier.class ) );
    }

    private static AddNodeCommand addNodeToIndex( IndexDefineCommand definitions, String indexName, long nodeId )
    {
        AddNodeCommand command = new AddNodeCommand();
        command.init( definitions.getOrAssignIndexNameId( indexName ), nodeId, (byte) 0, "some value" );
        return command;
    }

    private static IndexDefineCommand definitions( Map<String,Integer> names, Map<String,Integer> keys )
    {
        IndexDefineCommand definitions = new IndexDefineCommand();
        definitions.init( names, keys );
        return definitions;
    }
}
