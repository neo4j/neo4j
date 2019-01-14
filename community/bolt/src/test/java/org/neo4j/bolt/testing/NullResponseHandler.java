/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.testing;

import org.neo4j.bolt.v1.runtime.BoltResponseHandler;
import org.neo4j.bolt.v1.runtime.Neo4jError;
import org.neo4j.bolt.v1.runtime.spi.BoltResult;
import org.neo4j.values.AnyValue;

/**
 * Used by tests when the response for a request is not relevant.
 */
public class NullResponseHandler implements BoltResponseHandler
{
    private static final NullResponseHandler INSTANCE = new NullResponseHandler();

    public static NullResponseHandler nullResponseHandler()
    {
        return INSTANCE;
    }

    private NullResponseHandler()
    {
    }

    @Override
    public void onStart()
    {
        // this page intentionally left blank
    }

    @Override
    public void onRecords( BoltResult result, boolean pull )
    {
        // this page intentionally left blank
    }

    @Override
    public void onMetadata( String key, AnyValue value )
    {
        // this page intentionally left blank
    }

    @Override
    public void markFailed( Neo4jError error )
    {
        // this page intentionally left blank
    }

    @Override
    public void onFinish()
    {
        // this page intentionally left blank
    }

    @Override
    public void markIgnored()
    {
        // this page intentionally left blank
    }

}
