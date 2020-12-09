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

import java.util.function.Function;

import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.util.Preconditions;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.dbms.database.ComponentVersion.Neo4jVersions.UNKNOWN_VERSION;

/**
 * These components only care about the version number
 */
public abstract class AbstractVersionComponent<T extends ComponentVersion> extends AbstractSystemGraphComponent
{
    private final String componentName;
    private final T latestVersion;
    protected final Function<Integer,T> convertFunction;
    protected volatile T currentVersion;

    public AbstractVersionComponent( String componentName, T latestVersion, Config config, Function<Integer, T> convertFunction )
    {
        super( config );
        this.componentName = componentName;
        this.latestVersion = latestVersion;
        this.convertFunction = convertFunction;
    }

    abstract T getFallbackVersion();

    @Override
    public String componentName()
    {
        return componentName;
    }

    @Override
    public Status detect( Transaction tx )
    {
        int version = getVersion( tx, componentName );
        if ( version == UNKNOWN_VERSION )
        {
            return Status.UNINITIALIZED;
        }
        else if ( version < latestVersion.getVersion() )
        {
            return Status.REQUIRES_UPGRADE;
        }
        else if ( version > latestVersion.getVersion() )
        {
            return Status.UNSUPPORTED_FUTURE;
        }

        return Status.CURRENT;
    }

    @Override
    public void initializeSystemGraph( GraphDatabaseService system ) throws Exception
    {
        boolean mayUpgrade = config.get( GraphDatabaseSettings.allow_single_automatic_upgrade );

        Preconditions.checkState( system.databaseName().equals( SYSTEM_DATABASE_NAME ),
                "Cannot initialize system graph on database '" + system.databaseName() + "'" );

        Status status;
        try ( Transaction tx = system.beginTx() )
        {
            status = detect( tx );
            tx.commit();
        }

        switch ( status )
        {
        case CURRENT:
            break;
        case UNINITIALIZED:
            if ( mayUpgrade )
            {
                initializeSystemGraphModel( system );
            }
            break;
        case REQUIRES_UPGRADE:
            if ( mayUpgrade )
            {
                upgradeToCurrent( system );
            }
            break;
        default:
            throw new IllegalStateException( String.format( "Unsupported component state for '%s': %s", componentName(), status.description() ) );
        }
    }

    @Override
    protected void initializeSystemGraphModel( GraphDatabaseService system ) throws Exception
    {
        SystemGraphComponent.executeWithFullAccess( system, tx ->
        {
            var node = tx.findNodes( VERSION_LABEL )
                         .stream()
                         .findFirst()
                         .orElseGet( () -> tx.createNode( VERSION_LABEL ) );

            node.setProperty( componentName, latestVersion.getVersion() );
        } );
    }

    @Override
    public void upgradeToCurrent( GraphDatabaseService system ) throws Exception
    {
        initializeSystemGraphModel( system );
    }

    protected T fetchStateFromSystemDatabase( GraphDatabaseService system )
    {
        T result = getFallbackVersion();
        try ( var tx = system.beginTx();
              var nodes = tx.findNodes( VERSION_LABEL ) )
        {
            if ( nodes.hasNext() )
            {
                Node versionNode = nodes.next();
                if ( versionNode.hasProperty( componentName ) )
                {
                    result = convertFunction.apply( (int) versionNode.getProperty( componentName ) );
                }
                Preconditions.checkState( !nodes.hasNext(), "More than one version node in system database" );
            }
        }
        return result;
    }
}
