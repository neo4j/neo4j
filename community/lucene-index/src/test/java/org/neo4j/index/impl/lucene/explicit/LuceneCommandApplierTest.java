/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.index.impl.lucene.explicit;

import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.mockfs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.factory.OperationalMode;
import org.neo4j.kernel.impl.index.IndexCommand.AddNodeCommand;
import org.neo4j.kernel.impl.index.IndexConfigStore;
import org.neo4j.kernel.impl.index.IndexDefineCommand;
import org.neo4j.kernel.lifecycle.Lifespan;
import org.neo4j.test.extension.EphemeralFileSystemExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.TestDirectoryExtension;
import org.neo4j.test.rule.TestDirectory;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.neo4j.index.impl.lucene.explicit.LuceneIndexImplementation.EXACT_CONFIG;

@ExtendWith( {EphemeralFileSystemExtension.class, TestDirectoryExtension.class} )
class LuceneCommandApplierTest
{
    @Inject
    private EphemeralFileSystemAbstraction fs;
    @Inject
    private TestDirectory testDirectory;

    @Test
    void shouldHandleMultipleIdSpaces() throws Exception
    {
        // GIVEN
        String indexName = "name";
        String key = "key";
        DatabaseLayout databaseLayout = testDirectory.databaseLayout();
        IndexConfigStore configStore = new IndexConfigStore( databaseLayout, fs );
        configStore.set( Node.class, indexName, EXACT_CONFIG );
        try ( Lifespan lifespan = new Lifespan() )
        {
            Config dataSourceConfig = Config.defaults( LuceneDataSource.Configuration.ephemeral, Settings.TRUE );
            LuceneDataSource originalDataSource = new LuceneDataSource( databaseLayout, dataSourceConfig, configStore, fs, OperationalMode.single );
            LuceneDataSource dataSource = lifespan.add( spy( originalDataSource ) );

            try ( LuceneCommandApplier applier = new LuceneCommandApplier( dataSource, false ) )
            {
                // WHEN issuing a command where the index name is mapped to a certain id
                IndexDefineCommand definitions =
                        definitions( ObjectIntHashMap.newWithKeysValues( indexName, 0 ), ObjectIntHashMap.newWithKeysValues( key, 0 ) );
                applier.visitIndexDefineCommand( definitions );
                applier.visitIndexAddNodeCommand( addNodeToIndex( definitions, indexName, 0L ) );
                // and then later issuing a command for that same index, but in another transaction where
                // the local index name id is a different one
                definitions = definitions( ObjectIntHashMap.newWithKeysValues( indexName, 1 ), ObjectIntHashMap.newWithKeysValues( key, 0 ) );
                applier.visitIndexDefineCommand( definitions );
                applier.visitIndexAddNodeCommand( addNodeToIndex( definitions, indexName, 1L ) );
            }

            // THEN both those updates should have been directed to the same index
            verify( dataSource, times( 1 ) ).getIndexSearcher( any( IndexIdentifier.class ) );
        }
    }

    private static AddNodeCommand addNodeToIndex( IndexDefineCommand definitions, String indexName, long nodeId )
    {
        AddNodeCommand command = new AddNodeCommand();
        command.init( definitions.getOrAssignIndexNameId( indexName ), nodeId, (byte) 0, "some value" );
        return command;
    }

    private static IndexDefineCommand definitions( MutableObjectIntMap<String> names, MutableObjectIntMap<String> keys )
    {
        IndexDefineCommand definitions = new IndexDefineCommand();
        definitions.init( names, keys );
        return definitions;
    }
}
