/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.capabilities;

import static org.neo4j.capabilities.Type.STRING;

import org.neo4j.annotations.Description;
import org.neo4j.annotations.service.ServiceProvider;

@ServiceProvider
public class DBMSCapabilities implements CapabilityDeclaration {
    @Description("Neo4j version this instance is running")
    public static final Capability<String> dbms_instance_version =
            new Capability<>(Name.of("dbms.instance.version"), STRING);

    @Description("Kernel version this instance is running")
    public static final Capability<String> dbms_instance_kernel_version =
            new Capability<>(Name.of("dbms.instance.kernel.version"), STRING);

    @Description("DBMS Edition this instance is running")
    public static final Capability<String> dbms_instance_edition =
            new Capability<>(Name.of("dbms.instance.edition"), STRING);

    @Description("DBMS Operational Mode this instance is running")
    public static final Capability<String> dbms_instance_operational_mode =
            new Capability<>(Name.of("dbms.instance.operational_mode"), STRING);
}
