/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.kernel.extension.dependency;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensions;
import org.neo4j.kernel.impl.api.scan.LabelScanStoreProvider;

/**
 * Select the "correct" {@link LabelScanStoreProvider} among potentially several implementations
 * loaded as {@link KernelExtensions}.
 * <p>
 * Two parameters affects which {@link LabelScanStoreProvider} is selected:
 * <ol>
 * <li>Specifically configured label scan store name, see {@link GraphDatabaseSettings#label_index},
 * each {@link LabelScanStoreProvider} is instantiated with a name, which is matched with the configured name.
 * The setting can also be left unspecified, at which point the next parameter kicks in:</li>
 * <li>Highest prioritized instance, actually using {@link HighestSelectionStrategy}</li>
 */
public class NamedLabelScanStoreSelectionStrategy implements DependencyResolver.SelectionStrategy
{
    private final String specificallyConfigured;

    public NamedLabelScanStoreSelectionStrategy( String specificallyConfigured )
    {
        Objects.requireNonNull( specificallyConfigured );
        this.specificallyConfigured = specificallyConfigured;
    }

    public NamedLabelScanStoreSelectionStrategy( Config config )
    {
        this( config.get( GraphDatabaseSettings.label_index ) );
    }

    @Override
    public <T> T select( Class<T> type, Iterable<T> candidates ) throws IllegalArgumentException
    {
        if ( !type.equals( LabelScanStoreProvider.class ) )
        {
            throw new IllegalArgumentException( "Was expecting " + LabelScanStoreProvider.class );
        }

        List<String> candidateNames = new ArrayList<>();
        for ( T candidate : candidates )
        {
            LabelScanStoreProvider provider = (LabelScanStoreProvider) candidate;
            String candidateName = provider.getName();
            candidateNames.add( candidateName );
            if ( specificallyConfigured.equalsIgnoreCase( candidateName ) )
            {
                return candidate;
            }
        }
        throw new IllegalArgumentException( "Configured label index '" + specificallyConfigured +
                "', but couldn't find it among candidates " + candidateNames );
    }
}
