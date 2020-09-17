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

import java.util.Optional;

import org.neo4j.configuration.Config;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;

public class DbmsRuntimeSystemGraphComponent extends AbstractSystemGraphComponent
{
    private static final Label versionLabel = Label.label( "Version" );
    public static final String COMPONENT_NAME = "dbms-runtime";

    public DbmsRuntimeSystemGraphComponent( Config config )
    {
        super( config );
    }

    @Override
    public String component()
    {
        return COMPONENT_NAME;
    }

    @Override
    public Status detect( Transaction tx )
    {
        Optional<DbmsRuntimeVersion> version = tx.findNodes( DbmsRuntimeRepository.DBMS_RUNTIME_LABEL ).stream()
                                                 .map( node -> (int) node.getProperty( DbmsRuntimeRepository.VERSION_PROPERTY ) )
                                                 .map( DbmsRuntimeVersion::fromVersionNumber )
                                                 .findFirst();
        if ( version.isEmpty() )
        {
            return Status.UNINITIALIZED;
        }

        if ( version.get() != DbmsRuntimeRepository.LATEST_VERSION )
        {
            return Status.REQUIRES_UPGRADE;
        }

        return Status.CURRENT;
    }

    @Override
    protected void initializeSystemGraphModel( Transaction tx )
    {
        DbmsRuntimeVersion dbmsRuntimeVersion = DbmsRuntimeRepository.LATEST_VERSION;
        if ( is41Database( tx ) )
        {
            dbmsRuntimeVersion = DbmsRuntimeVersion.V4_1;
        }

        tx.createNode( DbmsRuntimeRepository.DBMS_RUNTIME_LABEL )
          .setProperty( DbmsRuntimeRepository.VERSION_PROPERTY, dbmsRuntimeVersion.getVersionNumber() );
    }

    @Override
    public void upgradeToCurrent( GraphDatabaseService systemDb ) throws Exception
    {
        SystemGraphComponent.executeWithFullAccess( systemDb, tx ->
                tx.findNodes( DbmsRuntimeRepository.DBMS_RUNTIME_LABEL )
                  .stream()
                  .forEach( node -> node.setProperty( DbmsRuntimeRepository.VERSION_PROPERTY, DbmsRuntimeRepository.LATEST_VERSION.getVersionNumber() ) ) );
    }

    private boolean is41Database( Transaction tx )
    {
        // This detection is a bit hacky and relies on node (:Version)
        // introduced by the security module in 4.1.
        // The hacky part is that this System Graph Component
        // must be initialised before the Security System Graph Component
        // in order to be able distinguish between an older database
        // and a freshly initialised one.

        boolean result = false;
        ResourceIterator<Node> nodes = tx.findNodes( versionLabel );
        if ( nodes.hasNext() )
        {
            result = true;
        }
        nodes.close();

        return result;
    }
}
