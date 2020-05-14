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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Pair;

import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;

/**
 * Central collection for managing multiple versioned system graph initializers. There could be several components in the DBMS that each have a requirement on
 * the system database to contain a graph with a specific schema. Each component needs to maintain that schema and support multiple versions of that schema in
 * order to allow rolling upgrades to be possible where newer versions of Neo4j will be able to run on older versions of the system database.
 * <p>
 * The core design is that each component is able to detect the version of their own sub-graph and from that decide if they can support it or not, and how to
 * upgrade from one version to another.
 */
public class SystemGraphComponents implements SystemGraphComponent
{
    private final HashMap<String,SystemGraphComponent> componentMap = new HashMap<>();
    private final ArrayList<SystemGraphComponent> components = new ArrayList<>();

    public void register( SystemGraphComponent initializer )
    {
        deregister( initializer.component() );
        componentMap.put( initializer.component(), initializer );
        components.add( initializer );
    }

    @SuppressWarnings( "WeakerAccess" )
    public void deregister( String key )
    {
        SystemGraphComponent removed = componentMap.remove( key );
        if ( removed != null )
        {
            components.remove( removed );
        }
    }

    public void forEach( Consumer<SystemGraphComponent> process )
    {
        components.forEach( process );
    }

    public String component()
    {
        return "system-graph";
    }

    public SystemGraphComponent.Status detect( Transaction tx )
    {
        return components.stream().map( c -> c.detect( tx ) ).reduce( SystemGraphComponent.Status::with ).orElse( SystemGraphComponent.Status.CURRENT );
    }

    @Override
    public Optional<Exception> initializeSystemGraph( GraphDatabaseService system )
    {
        assert system.databaseName().equals( SYSTEM_DATABASE_NAME );
        List<Pair<SystemGraphComponent,Exception>> errors =
                components.stream().flatMap( c -> c.initializeSystemGraph( system ).stream().map( e -> Pair.of( c, e ) ) ).collect( Collectors.toList() );
        if ( !errors.isEmpty() )
        {
            if ( errors.size() == 1 )
            {
                Pair<SystemGraphComponent,Exception> e = errors.get( 0 );
                return Optional.of( new IllegalStateException(
                        String.format( "Failed to %s system graph component '%s': %s", "initialize", e.first().component(), e.other().getMessage() ),
                        e.other() ) );
            }
            else
            {
                StringBuilder sb = new StringBuilder( String.format( "Multiple components failed to %s the system graph:", "initialize" ) );
                errors.forEach( e -> sb.append( "\n\t" ).append( e.first().component() ).append( ": " ).append( e.other().toString() ) );
                return Optional.of( new IllegalStateException( sb.toString() ) );
            }
        }
        return Optional.empty();
    }

    public Optional<Exception> upgradeToCurrent( GraphDatabaseService system )
    {
        List<SystemGraphComponent> componentsToUpgrade = new ArrayList<>();
        SystemGraphComponent.executeWithFullAccess( system, tx -> components.stream().filter( c ->
        {
            Status status = c.detect( tx );
            return status == Status.UNSUPPORTED_BUT_CAN_UPGRADE || status == Status.REQUIRES_UPGRADE;
        } ).forEach( componentsToUpgrade::add ) );

        List<Exception> errors = new ArrayList<>();
        for ( SystemGraphComponent component : componentsToUpgrade )
        {
            Optional<Exception> exception = component.upgradeToCurrent( system );
            exception.ifPresent( errors::add );
        }

        if ( !errors.isEmpty() )
        {
            if ( errors.size() == 1 )
            {
                return Optional.of( errors.get( 0 ) );
            }
            else
            {
                StringBuilder sb = new StringBuilder( String.format( "Multiple components failed to %s the system graph:", "upgrade" ) );
                errors.forEach( e -> sb.append( "\n\t" ).append( e.toString() ) );
                return Optional.of( new IllegalStateException( sb.toString() ) );
            }
        }
        return Optional.empty();
    }

    public static final SystemGraphComponents NO_OP = new SystemGraphComponents()
    {
        public void register( SystemGraphComponent initializer )
        {
            // No sub-components can exist in order to disabled initialization
        }
    };
}
