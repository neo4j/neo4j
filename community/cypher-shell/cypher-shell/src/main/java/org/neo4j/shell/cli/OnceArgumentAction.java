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
package org.neo4j.shell.cli;

import java.util.Map;
import java.util.function.Consumer;
import net.sourceforge.argparse4j.inf.Argument;
import net.sourceforge.argparse4j.inf.ArgumentAction;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;

public class OnceArgumentAction implements ArgumentAction {
    @Override
    public void run(ArgumentParser parser, Argument arg, Map<String, Object> attrs, String flag, Object value)
            throws ArgumentParserException {
        run(parser, arg, attrs, flag, value, v -> attrs.put(arg.getDest(), v));
    }

    @Override
    public void run(
            ArgumentParser parser,
            Argument arg,
            Map<String, Object> attrs,
            String flag,
            Object value,
            Consumer<Object> valueSetter)
            throws ArgumentParserException {
        final String seenAttr = getClass().getName() + ".seen::" + arg.getDest();
        if (attrs.get(seenAttr) == Boolean.TRUE) {
            throw new ArgumentParserException("Specify one of " + arg.textualName(), parser);
        }
        attrs.put(seenAttr, true);
        valueSetter.accept(value);
    }

    @Override
    public boolean consumeArgument() {
        return true;
    }

    @Override
    public void onAttach(Argument arg) {}
}
