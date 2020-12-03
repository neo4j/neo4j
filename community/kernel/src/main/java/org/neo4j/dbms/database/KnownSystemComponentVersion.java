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

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.logging.Log;

import static org.neo4j.dbms.database.ComponentVersion.Neo4jVersions.UNKNOWN_VERSION;

/**
 * Version of a system graph component.
 * Components that due to breaking schema changes requires migrations also needs versions.
 * Each component has its own version scheme starting at 0 and increasing each release with breaking changes.
 * The versions should be on the format [component]Version_[versionNbr]_[neo4jRelease].
 * The version schemes are described in {@link ComponentVersion}.
 */
public abstract class KnownSystemComponentVersion
{
    private final Label versionLabel = Label.label( "Version" );
    private final ComponentVersion componentVersion;
    protected final String componentVersionProperty;
    public final int version;
    public final String description;
    protected final Log log;

    protected KnownSystemComponentVersion( ComponentVersion componentVersion, Log log )
    {
        this.componentVersion = componentVersion;
        this.componentVersionProperty = componentVersion.getComponentName();
        this.version = componentVersion.getVersion();
        this.description = componentVersion.getDescription();
        this.log = log;
    }

    public boolean isCurrent()
    {
        return componentVersion.isCurrent();
    }

    public boolean migrationSupported()
    {
        return componentVersion.migrationSupported();
    }

    public boolean runtimeSupported()
    {
        return componentVersion.runtimeSupported();
    }

    protected int getVersion( Transaction tx )
    {
        int result = UNKNOWN_VERSION;
        try ( ResourceIterator<Node> nodes = tx.findNodes( versionLabel ) )
        {
            if ( nodes.hasNext() )
            {
                Node versionNode = nodes.next();
                if ( versionNode.hasProperty( componentVersionProperty ) )
                {
                    result = (Integer) versionNode.getProperty( componentVersionProperty );
                }
            }
        }
        return result;
    }

    public boolean detected( Transaction tx )
    {
        return getVersion( tx ) == version;
    }

    public UnsupportedOperationException unsupported()
    {
        String message = String.format( "System graph version %d for component '%s' in '%s' is not supported", version, componentVersionProperty, description );
        log.error( message );
        return new UnsupportedOperationException( message );
    }

    public SystemGraphComponent.Status getStatus()
    {
        if ( this.version == UNKNOWN_VERSION )
        {
            return SystemGraphComponent.Status.UNINITIALIZED;
        }
        else if ( this.isCurrent() )
        {
            return SystemGraphComponent.Status.CURRENT;
        }
        else if ( this.migrationSupported() )
        {
            return this.runtimeSupported() ? SystemGraphComponent.Status.REQUIRES_UPGRADE : SystemGraphComponent.Status.UNSUPPORTED_BUT_CAN_UPGRADE;
        }
        else
        {
            return SystemGraphComponent.Status.UNSUPPORTED;
        }
    }

    protected static boolean nodesWithLabelExist( Transaction tx, Label label )
    {
        boolean result = false;
        ResourceIterator<Node> nodes = tx.findNodes( label );
        if ( nodes.hasNext() )
        {
            result = true;
        }
        nodes.close();
        return result;
    }

    public void setVersionProperty( Transaction tx, int newVersion )
    {
        Node versionNode = findOrCreateVersionNode( tx );
        if ( versionNode.hasProperty( componentVersionProperty ) )
        {
            int oldVersion = (Integer) versionNode.getProperty( componentVersionProperty );
            log.info( String.format( "Upgrading '%s' version property from %d to %d", componentVersionProperty, oldVersion, newVersion ) );
        }
        else
        {
            log.info( String.format( "Setting version for '%s' to %d", componentVersionProperty, newVersion ) );
        }
        versionNode.setProperty( componentVersionProperty, newVersion );
    }

    private Node findOrCreateVersionNode( Transaction tx )
    {
        ResourceIterator<Node> nodes = tx.findNodes( versionLabel );
        if ( nodes.hasNext() )
        {
            Node node = nodes.next();
            if ( nodes.hasNext() )
            {
                throw new IllegalStateException( "More than one Version node exists" );
            }
            return node;
        }
        return tx.createNode( versionLabel );
    }
}
