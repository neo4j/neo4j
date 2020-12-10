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

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.util.Preconditions;

import static org.neo4j.dbms.database.ComponentVersion.DBMS_RUNTIME_COMPONENT;

public class DbmsRuntimeSystemGraphComponent extends AbstractVersionComponent<DbmsRuntimeVersion>
{
    public static final Label OLD_COMPONENT_LABEL = Label.label( "DbmsRuntime" );
    public static final String OLD_PROPERTY_NAME = "version";

    public DbmsRuntimeSystemGraphComponent( Config config )
    {
        super( DBMS_RUNTIME_COMPONENT, DbmsRuntimeVersion.LATEST_DBMS_RUNTIME_COMPONENT_VERSION, config, DbmsRuntimeVersion::fromVersionNumber );
    }

    @Override
    DbmsRuntimeVersion getFallbackVersion()
    {
        return DbmsRuntimeVersion.V4_1;
    }

    @Override
    public Integer getVersionNumber( Transaction tx, String componentName )
    {
        Integer result = null;
        try ( ResourceIterator<Node> nodes = tx.findNodes( OLD_COMPONENT_LABEL ) )
        {
            if ( nodes.hasNext() )
            {
                Node versionNode = nodes.next();
                if ( versionNode.hasProperty( OLD_PROPERTY_NAME ) )
                {
                    result = (Integer) versionNode.getProperty( OLD_PROPERTY_NAME );
                }
            }
        }
        return result != null ? result : super.getVersionNumber( tx, componentName );
    }

    @Override
    public void upgradeToCurrent( GraphDatabaseService systemDb ) throws Exception
    {
        SystemGraphComponent.executeWithFullAccess( systemDb, tx ->
        {
            tx.findNodes( OLD_COMPONENT_LABEL ).forEachRemaining( Node::delete );
            setToLatestVersion( tx );
        } );
    }

    @Override
    DbmsRuntimeVersion fetchStateFromSystemDatabase( GraphDatabaseService system )
    {
        DbmsRuntimeVersion result = null;
        try ( var tx = system.beginTx();
              var nodes = tx.findNodes( OLD_COMPONENT_LABEL ) )
        {
            if ( nodes.hasNext() )
            {
                Node versionNode = nodes.next();
                if ( versionNode.hasProperty( OLD_PROPERTY_NAME ) )
                {
                    result = convertToVersion.apply( (int) versionNode.getProperty( OLD_PROPERTY_NAME ) );
                }
                Preconditions.checkState( !nodes.hasNext(), "More than one version node in system database" );
            }
        }
        if ( result == null )
        {
            result = super.fetchStateFromSystemDatabase( system );
        }
        return result;
    }
}
