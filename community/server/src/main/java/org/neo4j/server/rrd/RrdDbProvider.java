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
package org.neo4j.server.rrd;

import javax.ws.rs.ext.Provider;

import org.neo4j.server.database.InjectableProvider;
import org.neo4j.server.database.RrdDbWrapper;
import org.rrd4j.core.RrdDb;

import com.sun.jersey.api.core.HttpContext;

@Provider
public class RrdDbProvider extends InjectableProvider<RrdDb>
{
    private final RrdDbWrapper rrdDbWrapper;

    public RrdDbProvider( RrdDbWrapper rrdDbWrapper )
    {
        super( RrdDb.class );
        this.rrdDbWrapper = rrdDbWrapper;
    }

    @Override
    public RrdDb getValue( HttpContext c )
    {
        return rrdDbWrapper.get();
    }
}
