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

/**
 * These components only care about the version number
 */
public abstract class AbstractVersionComponent<T extends ComponentVersion> extends AbstractSystemGraphComponent
{
    private final String componentName;
    private final T latestVersion;
    protected final Function<Integer,T> convertToVersion;
    protected volatile T currentVersion;

    public AbstractVersionComponent( String componentName, T latestVersion, Config config, Function<Integer, T> convertFunction )
    {
        super( config );
        this.componentName = componentName;
        this.latestVersion = latestVersion;
        this.convertToVersion = convertFunction;
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
        try
        {
            Integer versionNumber = getVersionNumber( tx, componentName );
            if ( versionNumber == null )
            {
                return Status.UNINITIALIZED;
            }
            else
            {
                T version = convertToVersion.apply( getVersionNumber( tx, componentName ) );
                if ( latestVersion.isGreaterThan( version ) )
                {
                    return Status.REQUIRES_UPGRADE;
                }
                else if ( latestVersion.equals( version ) )
                {
                    return Status.CURRENT;
                }
                else
                {
                    return Status.UNSUPPORTED_FUTURE;
                }
            }
        }
        catch ( IllegalArgumentException e )
        {
            return Status.UNSUPPORTED_FUTURE;
        }
    }

    @Override
    public void initializeSystemGraph( GraphDatabaseService system, boolean firstInitialization ) throws Exception
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
            if ( mayUpgrade || firstInitialization )
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
        SystemGraphComponent.executeWithFullAccess( system, this::setToLatestVersion );
    }

    void setToLatestVersion( Transaction tx )
    {
        var node = tx.findNodes( VERSION_LABEL )
                     .stream()
                     .findFirst()
                     .orElseGet( () -> tx.createNode( VERSION_LABEL ) );

        node.setProperty( componentName, latestVersion.getVersion() );
    }

    @Override
    public void upgradeToCurrent( GraphDatabaseService system ) throws Exception
    {
        initializeSystemGraphModel( system );
    }

    T fetchStateFromSystemDatabase( GraphDatabaseService system )
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
                    result = convertToVersion.apply( (int) versionNode.getProperty( componentName ) );
                }
                Preconditions.checkState( !nodes.hasNext(), "More than one version node in system database" );
            }
        }
        return result;
    }
}
