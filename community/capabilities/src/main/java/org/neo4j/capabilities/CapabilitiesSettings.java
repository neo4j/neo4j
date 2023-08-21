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

import static org.neo4j.configuration.SettingImpl.newBuilder;
import static org.neo4j.configuration.SettingValueParsers.STRING;
import static org.neo4j.configuration.SettingValueParsers.listOf;

import java.util.Collections;
import java.util.List;
import org.neo4j.annotations.service.ServiceProvider;
import org.neo4j.configuration.Description;
import org.neo4j.configuration.Internal;
import org.neo4j.configuration.SettingsDeclaration;
import org.neo4j.graphdb.config.Setting;

@ServiceProvider
public class CapabilitiesSettings implements SettingsDeclaration {

    @Internal
    @Description("List of capabilities to block access from capabilities API or procedures. Each entry can "
            + "be a wildcard expression containing '*' to match a single namespace entry and '**' to match "
            + "any number of consequent namespace entries. For example 'dbms.*.version' would match any of "
            + "the 'dbms.instance.version' or 'dbms.bolt.version' however would not match "
            + "'dbms.instance.kernel.version' - which could however be matched by 'dbms.**.version' pattern. "
            + "Note that capability names comply with the regular expression '^\\w+(.\\w+)*$' (a word followed "
            + "by any number of words, each separated by '.'.")
    public static final Setting<List<String>> dbms_capabilities_blocked = newBuilder(
                    "internal.dbms.capabilities.blocked", listOf(STRING), Collections.emptyList())
            .dynamic()
            .build();
}
