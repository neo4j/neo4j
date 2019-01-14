/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
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
