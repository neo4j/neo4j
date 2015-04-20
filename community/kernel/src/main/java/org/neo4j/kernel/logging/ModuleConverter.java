/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.logging;

import ch.qos.logback.classic.pattern.ClassicConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

import java.io.IOException;

/**
 * Logback converter that converts logger names to module names.
 * Module names are defined in modules.properties which must be in classpath
 */
public class ModuleConverter
    extends ClassicConverter
{
    ModuleMapper mapper;

    public ModuleConverter() throws IOException
    {
        mapper = new ModuleMapper();
    }

    @Override
    public String convert(ILoggingEvent iLoggingEvent)
    {
        return mapper.map(iLoggingEvent.getLoggerName());
    }
}
