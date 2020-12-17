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
package org.neo4j.dbms.database;

import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.event.TransactionData;
import org.neo4j.graphdb.event.TransactionEventListener;
import org.neo4j.internal.helpers.collection.Iterables;

import static org.neo4j.dbms.database.SystemGraphComponent.VERSION_LABEL;

/**
 * A version of {@link} DbmsRuntimeRepository for standalone editions.
 */
public class StandaloneDbmsRuntimeRepository extends DbmsRuntimeRepository implements TransactionEventListener<Object>
{

    public StandaloneDbmsRuntimeRepository( DatabaseManager<?> databaseManager, DbmsRuntimeSystemGraphComponent component )
    {
        super( databaseManager, component );
    }

    @Override
    public Object beforeCommit( TransactionData data, Transaction transaction, GraphDatabaseService databaseService ) throws Exception
    {
        // not interested in this event
        return null;
    }

    @Override
    public void afterCommit( TransactionData transactionData, Object state, GraphDatabaseService databaseService )
    {
        // no check is needed if we are at the latest version, because downgrade is not supported
        if ( transactionData == null || getVersion().isCurrent() )
        {
            return;
        }

        List<Long> nodesWithChangedProperties = Iterables.stream( transactionData.assignedNodeProperties() )
                                                         .map( nodePropertyEntry -> nodePropertyEntry.entity().getId() )
                                                         .collect( Collectors.toList() );

        var systemDatabase = getSystemDb();
        try ( var tx = systemDatabase.beginTx() )
        {
            nodesWithChangedProperties.stream()
                                      .map( tx::getNodeById )
                                      .filter( node -> node.hasLabel( VERSION_LABEL ) && node.hasProperty( component.componentName() ) )
                                      .map( dbmRuntime -> (int) dbmRuntime.getProperty( component.componentName() ) )
                                      .map( DbmsRuntimeVersion::fromVersionNumber )
                                      .forEach( this::setVersion );
        }
    }

    @Override
    public void afterRollback( TransactionData data, Object state, GraphDatabaseService databaseService )
    {
        // not interested in this event
    }
}
