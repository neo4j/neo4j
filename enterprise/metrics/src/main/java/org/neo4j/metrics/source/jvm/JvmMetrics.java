/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.metrics.source.jvm;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

@Documented( "=== Java Virtual Machine Metrics\n\n" +
             "These metrics are environment dependent and they may vary on different hardware and with JVM configurations.\n" +
             "Typically these metrics will show information about garbage collections " +
             "(for example the number of events and time spent collecting), memory pools and buffers, and " +
             "finally the number of active threads running." )
public abstract class JvmMetrics extends LifecycleAdapter
{
    public static final String NAME_PREFIX = "vm";

    public static String prettifyName( String name )
    {
        return name.toLowerCase().replace( ' ', '_' );
    }
}
