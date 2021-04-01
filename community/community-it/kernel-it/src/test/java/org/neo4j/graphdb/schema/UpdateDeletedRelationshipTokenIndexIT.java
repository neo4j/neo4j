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

package org.neo4j.graphdb.schema;

import org.junit.jupiter.api.BeforeEach;

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;

@ImpermanentDbmsExtension( configurationCallback = "configuration" )
class UpdateDeletedRelationshipTokenIndexIT extends UpdateDeletedRelationshipIndexBase
{
    @ExtensionCallback
    void configuration( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( RelationshipTypeScanStoreSettings.enable_scan_stores_as_token_indexes, true );
    }

    @BeforeEach
    void before()
    {
        try ( var tx = db.beginTx() )
        {
            tx.schema().getIndexes().forEach( IndexDefinition::drop );
            tx.commit();
        }
    }

    @Override
    protected IndexDefinition indexCreate()
    {
        IndexDefinition indexDefinition;
        try ( Transaction tx = db.beginTx() )
        {
            indexDefinition = tx.schema().indexFor( AnyTokens.ANY_RELATIONSHIP_TYPES ).create();
            tx.commit();
        }
        return indexDefinition;
    }
}
