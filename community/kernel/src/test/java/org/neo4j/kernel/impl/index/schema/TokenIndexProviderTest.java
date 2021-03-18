/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.kernel.impl.index.schema;

import java.util.List;

import org.neo4j.common.EntityType;
import org.neo4j.configuration.Config;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.IndexPrototype;
import org.neo4j.internal.schema.IndexType;
import org.neo4j.io.fs.FileSystemAbstraction;

import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.schema.IndexPrototype.forSchema;
import static org.neo4j.internal.schema.IndexPrototype.uniqueForSchema;
import static org.neo4j.internal.schema.SchemaDescriptor.forAllEntityTokens;
import static org.neo4j.internal.schema.SchemaDescriptor.forLabel;

public class TokenIndexProviderTest extends IndexProviderTests
{
    private static final ProviderFactory factory =
            ( pageCache, fs, dir, monitors, collector, readOnly, databaseLayout ) -> {
                DatabaseIndexContext context = DatabaseIndexContext.builder( pageCache, fs ).withMonitors( monitors ).withReadOnly( readOnly )
                        .withDatabaseName( DEFAULT_DATABASE_NAME ).build();
                return new TokenIndexProvider( context, dir, collector, Config.defaults(), databaseLayout );
            };

    TokenIndexProviderTest()
    {
        super( factory );
    }

    @Override
    void setupIndexFolders( FileSystemAbstraction fs )
    {
        //Token indexes doesn't use a separate folder.
    }

    @Override
    IndexDescriptor descriptor()
    {
        return completeConfiguration( forSchema( forAllEntityTokens( EntityType.NODE ) ).withName( "labelIndex" ).materialise( indexId ) );
    }

    @Override
    IndexDescriptor otherDescriptor()
    {
        return completeConfiguration( forSchema( forAllEntityTokens( EntityType.RELATIONSHIP ) ).withName( "relTypeIndex" ).materialise( indexId ) );
    }

    @Override
    IndexPrototype validPrototype()
    {
        return forSchema( forAllEntityTokens( EntityType.NODE ), TokenIndexProvider.DESCRIPTOR ).withIndexType( IndexType.LOOKUP ).withName( "index" );
    }

    @Override
    List<IndexPrototype> invalidPrototypes()
    {
        return List.of( forSchema( forAllEntityTokens( EntityType.NODE ), TokenIndexProvider.DESCRIPTOR ).withIndexType( IndexType.BTREE )
                        .withName( "unsupported" ),
                forSchema( forAllEntityTokens( EntityType.NODE ), GenericNativeIndexProvider.DESCRIPTOR ).withIndexType( IndexType.LOOKUP )
                        .withName( "unsupported" ),
                forSchema( forLabel( labelId, propId ), TokenIndexProvider.DESCRIPTOR ).withIndexType( IndexType.LOOKUP ).withName( "unsupported" ),
                uniqueForSchema( forLabel( labelId, propId ), TokenIndexProvider.DESCRIPTOR ).withIndexType( IndexType.LOOKUP ).withName( "unsupported" ) );
    }
}
