/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.helpers;

import java.net.BindException;

/**
 * A bind exception that includes which port we failed to bind to. Whenever possible, catch and rethrow bind exceptions as this, to make it possible to
 * sort out which address it is that is in use.
 */
public class PortBindException extends BindException
{
    public PortBindException( HostnamePort address, BindException original )
    {
        super( String.format("Address %s is already in use, cannot bind to it.", address) );
        setStackTrace( original.getStackTrace() );
    }
}
