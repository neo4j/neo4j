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
package org.neo4j.bolt.logging;

import java.util.function.Supplier;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.values.virtual.MapValue;

public class NullBoltMessageLogger implements BoltMessageLogger
{
    private static final NullBoltMessageLogger INSTANCE = new NullBoltMessageLogger();

    private NullBoltMessageLogger()
    {
    }

    public static NullBoltMessageLogger getInstance()
    {
        return INSTANCE;
    }

    @Override
    public void clientEvent( String eventName )
    {
    }

    @Override
    public void clientEvent( String eventName, Supplier<String> detailsSupplier )
    {
    }

    @Override
    public void clientError( String eventName, String errorMessage, Supplier<String> detailsSupplier )
    {
    }

    @Override
    public void serverEvent( String eventName )
    {
    }

    @Override
    public void serverEvent( String eventName, Supplier<String> detailsSupplier )
    {
    }

    @Override
    public void serverError( String eventName, String errorMessage )
    {
    }

    @Override
    public void serverError( String eventName, Status status )
    {
    }

    @Override
    public void logInit( String userAgent )
    {
    }

    public void logRun()
    {
    }

    @Override
    public void logPullAll()
    {
    }

    @Override
    public void logDiscardAll()
    {
    }

    @Override
    public void logAckFailure()
    {
    }

    @Override
    public void logReset()
    {
    }

    @Override
    public void logSuccess( Supplier<MapValue> metadataSupplier )
    {
    }

    @Override
    public void logFailure( Status status )
    {
    }

    @Override
    public void logIgnored()
    {
    }

}
