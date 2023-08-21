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

import static java.nio.charset.Charset.defaultCharset;
import static org.jline.terminal.Attributes.InputFlag.IUTF8;

import java.io.ByteArrayOutputStream;
import java.io.Console;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import org.jline.terminal.Attributes;
import org.jline.terminal.impl.ExecPty;
import org.neo4j.util.VisibleForTesting;

/**
 * Simple prompt for basic user input.
 *
 * This is a workaround, I couldn't get jline to mask input when standard out is redirected.
 * See https://github.com/jline/jline3/issues/787.
 */
public interface SimplePrompt extends AutoCloseable {
    /** Reads line from user, returns null if end of stream is reached. */
    String readLine(String prompt) throws IOException;

    /**
     * Reads line from user with echoing disabled, returns null if end of stream is reached. */
    String readPassword(String prompt) throws IOException;

    static SimplePrompt defaultPrompt() {
        if (System.console() != null) {
            return new ConsolePrompt(System.console());
        }

        try {
            final var pty = (ExecPty) ExecPty.current();
            return new TtyPrompt(pty);
        } catch (Exception e) {
            // Ignore
        }

        return new WeakPrompt(System.in, new PrintWriter(OutputStream.nullOutputStream()), defaultCharset());
    }
}

class ConsolePrompt implements SimplePrompt {
    private final Console console;

    public ConsolePrompt(Console console) {
        this.console = console;
    }

    @Override
    public String readLine(String prompt) {
        return console.readLine(prompt);
    }

    @Override
    public String readPassword(String prompt) {
        final var read = console.readPassword(prompt);
        return read != null ? new String(read) : null;
    }

    @Override
    public void close() throws Exception {}
}

/**
 * Reads lines from a tty device directly, only for systems that has stty.
 *
 * Useful when System.console() is null because standard out is redirected.
 */
class TtyPrompt extends StreamPrompt {
    private final ExecPty pty;
    private final Attributes originalAttributes;
    private final Thread shutdownThread;
    private final Charset charset;

    TtyPrompt(ExecPty pty) throws IOException {
        this(pty, new FileInputStream(pty.getName()), new FileOutputStream(pty.getName()), defaultCharset());
    }

    @VisibleForTesting
    TtyPrompt(ExecPty pty, InputStream in, OutputStream out, Charset defaultCharset) throws IOException {
        this(pty, in, out, pty.getAttr(), getCharset(pty.getAttr(), defaultCharset));
    }

    private TtyPrompt(ExecPty pty, InputStream in, OutputStream out, Attributes originalAttributes, Charset charset) {
        super(in, new PrintWriter(out, false, charset));
        this.pty = pty;
        this.originalAttributes = originalAttributes;
        this.charset = charset;
        this.shutdownThread = new Thread(this::tryRestoringTerminal);
        Runtime.getRuntime().addShutdownHook(shutdownThread);
    }

    private static Charset getCharset(Attributes attributes, Charset defaultCharset) {
        return attributes.getInputFlag(IUTF8) ? StandardCharsets.UTF_8 : defaultCharset;
    }

    @Override
    protected void disableEcho() throws IOException {
        var attrs = pty.getAttr();
        attrs.setLocalFlag(Attributes.LocalFlag.ECHO, false);
        pty.setAttr(attrs);
    }

    @Override
    protected void restoreTerminal() throws IOException {
        pty.setAttr(originalAttributes);
    }

    private void tryRestoringTerminal() {
        try {
            restoreTerminal();
        } catch (Exception e) {
            // Ignore
        }
    }

    @Override
    protected Charset charset() {
        return charset;
    }

    @Override
    public void close() throws Exception {
        Runtime.getRuntime().removeShutdownHook(shutdownThread);
        pty.close();
    }
}

class WeakPrompt extends StreamPrompt {
    private final Charset charset;

    WeakPrompt(InputStream in, PrintWriter out, Charset charset) {
        super(in, out);
        this.charset = charset;
    }

    @Override
    protected void disableEcho() {}

    @Override
    protected Charset charset() {
        return charset;
    }

    @Override
    public void close() throws Exception {}

    @Override
    protected void restoreTerminal() {}
}

abstract class StreamPrompt implements SimplePrompt {
    private final InputStream in;
    private final PrintWriter out;

    StreamPrompt(InputStream in, PrintWriter out) {
        this.in = in;
        this.out = out;
    }

    protected abstract void disableEcho() throws IOException;

    protected abstract void restoreTerminal() throws IOException;

    protected abstract Charset charset();

    @Override
    public String readLine(String prompt) throws IOException {
        out.print(prompt);
        out.flush();
        return doReadLine();
    }

    @Override
    public String readPassword(String prompt) throws IOException {
        out.print(prompt);
        out.flush();

        try {
            disableEcho();
            final var read = doReadLine();
            if (read != null) {
                out.println();
                out.flush();
            }
            return read;
        } finally {
            restoreTerminal();
        }
    }

    protected void onRead(int read) {}

    protected String doReadLine() throws IOException {
        final var bytes = new ByteArrayOutputStream();
        int read;
        while ((read = in.read()) != -1 && read != '\n' && read != '\r') {
            bytes.write(read);
            onRead(read);
        }
        onRead(read);
        return read == -1 ? null : bytes.toString(charset());
    }
}
