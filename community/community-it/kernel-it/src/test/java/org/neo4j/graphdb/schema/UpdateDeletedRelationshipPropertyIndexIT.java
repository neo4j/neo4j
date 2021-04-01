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

import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.index.schema.RelationshipTypeScanStoreSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.ExtensionCallback;
import org.neo4j.test.extension.ImpermanentDbmsExtension;

@ImpermanentDbmsExtension( configurationCallback = "configuration" )
class UpdateDeletedRelationshipPropertyIndexIT extends UpdateDeletedRelationshipIndexBase
{
    @ExtensionCallback
    void configuration( TestDatabaseManagementServiceBuilder builder )
    {
        builder.setConfig( RelationshipTypeScanStoreSettings.enable_relationship_property_indexes, true );
    }

    @Override
    protected IndexDefinition indexCreate()
    {
        IndexDefinition indexDefinition;
        try ( Transaction tx = db.beginTx() )
        {
            indexDefinition = tx.schema().indexFor( TYPE_ONE ).on( KEY ).create();
            tx.commit();
        }
        return indexDefinition;
    }
}
