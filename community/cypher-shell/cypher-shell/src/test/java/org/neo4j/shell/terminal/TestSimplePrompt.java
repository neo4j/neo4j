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
package org.neo4j.shell.terminal;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;

public class TestSimplePrompt extends StreamPrompt {
    private final PrintWriter out;
    private boolean echo = true;

    public TestSimplePrompt(InputStream in, PrintWriter out) {
        super(in, out);
        this.out = out;
    }

    @Override
    protected void onRead(int read) {
        if (echo && read != -1) {
            out.print((char) read);
            out.flush();
        }
    }

    @Override
    protected void disableEcho() throws IOException {
        echo = false;
    }

    @Override
    protected void restoreTerminal() throws IOException {
        echo = true;
    }

    @Override
    protected Charset charset() {
        return Charset.defaultCharset();
    }

    @Override
    public void close() throws Exception {}
}
