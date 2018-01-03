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
package org.neo4j.kernel.impl.newapi;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.internal.kernel.api.NodeValueIndexCursorTestBase;
import org.neo4j.kernel.api.SchemaWriteOperations;
import org.neo4j.kernel.api.Statement;
import org.neo4j.kernel.api.TokenWriteOperations;
import org.neo4j.kernel.impl.core.ThreadToStatementContextBridge;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

abstract class AbstractNodeValueIndexCursorTest extends NodeValueIndexCursorTestBase<ReadTestSupport>
{
    @Override
    protected void createCompositeIndex( GraphDatabaseService graphDb, String label, String... properties )
            throws Exception
    {
        GraphDatabaseAPI internal = (GraphDatabaseAPI) graphDb;
        try ( Statement statement = internal.getDependencyResolver()
                .resolveDependency( ThreadToStatementContextBridge.class ).get() )
        {
            SchemaWriteOperations schemaWriteOperations =
                    statement.schemaWriteOperations();
            TokenWriteOperations token = statement.tokenWriteOperations();
            schemaWriteOperations.indexCreate(
                    new org.neo4j.kernel.api.schema.LabelSchemaDescriptor( token.labelGetOrCreateForName( "Person" ),
                            token.propertyKeyGetOrCreateForName( "firstname" ),
                            token.propertyKeyGetOrCreateForName( "surname" ) ) );
        }
    }
}
