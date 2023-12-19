/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
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
