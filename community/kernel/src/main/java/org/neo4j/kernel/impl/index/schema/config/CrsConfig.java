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
package org.neo4j.kernel.impl.index.schema.config;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.GroupSetting;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.values.storable.CoordinateReferenceSystem;

import static org.neo4j.configuration.SettingConstraints.size;
import static org.neo4j.configuration.SettingValueParsers.DOUBLE;
import static org.neo4j.configuration.SettingValueParsers.listOf;

@ServiceProvider
public class CrsConfig extends GroupSetting
{
    private static final String PREFIX = "unsupported.dbms.db.spatial.crs";

    public final Setting<List<Double>> min;
    public final Setting<List<Double>> max;
    public final CoordinateReferenceSystem crs;

    public static CrsConfig group( CoordinateReferenceSystem crs )
    {
        return new CrsConfig( crs.getName() );
    }

    private CrsConfig( String name )
    {
        super( name );
        crs = CoordinateReferenceSystem.byName( name );
        List<Double> defaultValue = new ArrayList<>( Collections.nCopies( crs.getDimension(), Double.NaN ) );
        min = getBuilder( "min", listOf( DOUBLE ), defaultValue ).addConstraint( size( crs.getDimension() ) ).build();
        max = getBuilder( "max", listOf( DOUBLE ), defaultValue ).addConstraint( size( crs.getDimension() ) ).build();
    }

    public CrsConfig()
    {
        super( null );  // For ServiceLoader
        min = null;
        max = null;
        crs = null;
    }

    @Override
    public String getPrefix()
    {
        return PREFIX;
    }
}
