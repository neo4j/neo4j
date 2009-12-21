/*
 * Copyright (c) 2008-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 * 
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 * 
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.remote.sites;

import java.net.URI;

import org.neo4j.remote.RemoteSite;
import org.neo4j.remote.RemoteSiteFactory;

/*public*/final class CorbaSiteFactory extends RemoteSiteFactory
{
    public CorbaSiteFactory()
    {
        super( "corbaname", "corbaloc" );
    }

    @Override
    protected RemoteSite create( URI resourceUri )
    {
        String scheme = resourceUri.getScheme();
        if ( "corbaloc".equals( scheme ) )
        {
            throw new UnsupportedOperationException(
                "CORBA support not implemented." );
        }
        else if ( "corbaname".equals( scheme ) )
        {
            throw new UnsupportedOperationException(
                "CORBA support not implemented." );
        }
        throw new IllegalArgumentException( "The resource URI \"" + resourceUri
            + "\" is not a CORBA URI." );
    }

    @Override
    protected boolean handlesUri( URI resourceUri )
    {
        String scheme = resourceUri.getScheme();
        if ( "corbaloc".equals( scheme ) )
        {
            return isValidCorbalocUri( resourceUri );
        }
        else if ( "corbaname".equals( scheme ) )
        {
            return isValidCorbanameUri( resourceUri );
        }
        else
        {
            return false;
        }
    }

    private boolean isValidCorbalocUri( URI resourceUri )
    {
        return true; // TODO: implement verification
    }

    private boolean isValidCorbanameUri( URI resourceUri )
    {
        return true; // TODO: implement verification
    }
}
