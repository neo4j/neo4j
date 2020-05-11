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
package org.neo4j.server.http.cypher.format.output.json;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import org.glassfish.hk2.utilities.binding.AbstractBinder;

/**
 * This configures a {@link JsonFactory} and binds it to the same class in the Jersey context.
 */
public class JsonFactoryBinder extends AbstractBinder
{
    @Override
    protected void configure()
    {
        var jsonFactory = new JsonFactory().disable( JsonGenerator.Feature.FLUSH_PASSED_TO_STREAM );
        bind( jsonFactory ).to( JsonFactory.class );
    }
}
