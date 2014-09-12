/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.kernel.impl.core;

import org.neo4j.kernel.api.properties.DefinedProperty;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.logging.ConsoleLogger;

class NoDuplicatesPropertyChainVerifier implements PropertyChainVerifier
{
    public static final String DUPLICATE_WARNING_MESSAGE = "WARNING: Duplicate property records have been detected in" +
            " this database store. For further details and resolution please refer to http://neo4j.com/technote/cr73nh";

    private final ConsoleLogger logger;

    public NoDuplicatesPropertyChainVerifier( StringLogger logger )
    {
        this.logger = new ConsoleLogger( logger );
    }

    @Override
    public void verifySortedPropertyChain( DefinedProperty[] propertyChain, Primitive entity )
    {
        if ( containsDuplicates( propertyChain ) )
        {
            logger.warn( DUPLICATE_WARNING_MESSAGE );
        }
    }

    private boolean containsDuplicates( DefinedProperty[] propertyChain )
    {
        boolean foundDuplicates = false;
        int previousKeyId = -1;
        for ( DefinedProperty property : propertyChain )
        {
            int keyId = property.propertyKeyId();
            if ( keyId == previousKeyId )
            {
                foundDuplicates = true;
            }
            previousKeyId = keyId;
        }
        return foundDuplicates;
    }
}
