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
package org.neo4j.dbms.database;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterators;

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
        return DbmsRuntimeVersion.V4_2;
    }

    @Override
    public Integer getVersion( Transaction tx, String componentName )
    {
        Integer result = null;
        try ( ResourceIterator<Node> nodes = tx.findNodes( OLD_COMPONENT_LABEL ) )
        {
            if ( nodes.hasNext() )
            {
                Node versionNode = nodes.next();
                result = (Integer) versionNode.getProperty( OLD_PROPERTY_NAME, null );
            }
        }
        return result != null ? result : SystemGraphComponent.getVersionNumber( tx, componentName );
    }

    @Override
    public void upgradeToCurrent( GraphDatabaseService systemDb ) throws Exception
    {
        SystemGraphComponent.executeWithFullAccess( systemDb, tx ->
        {
            Iterators.forEachRemaining( tx.findNodes( OLD_COMPONENT_LABEL ), Node::delete );
            setToLatestVersion( tx );
        } );
    }
}
