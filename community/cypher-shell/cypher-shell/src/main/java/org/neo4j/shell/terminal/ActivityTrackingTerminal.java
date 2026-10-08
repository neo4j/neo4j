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
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;
import org.jline.terminal.Attributes;
import org.jline.terminal.Cursor;
import org.jline.terminal.MouseEvent;
import org.jline.terminal.Size;
import org.jline.terminal.Terminal;
import org.jline.utils.ColorPalette;
import org.jline.utils.InfoCmp.Capability;
import org.jline.utils.NonBlockingReader;

/**
 * Delegates everything to the wrapped {@link Terminal}, except that the reader it hands out reports every
 * character it reads to an {@link ActivityListener}.
 *
 * <p>This lets idle-timeout bookkeeping observe keystrokes directly, independent of masking state or
 * terminal type.
 */
class ActivityTrackingTerminal implements Terminal {
    private final Terminal delegate;
    private final ActivityListener listener;
    private NonBlockingReader reader;

    ActivityTrackingTerminal(Terminal delegate, ActivityListener listener) {
        this.delegate = delegate;
        this.listener = listener;
    }

    @Override
    public synchronized NonBlockingReader reader() {
        if (reader == null) {
            reader = new ActivityTrackingReader(delegate.reader(), listener);
        }
        return reader;
    }

    @Override
    public String getName() {
        return delegate.getName();
    }

    @Override
    public SignalHandler handle(Signal signal, SignalHandler handler) {
        return delegate.handle(signal, handler);
    }

    @Override
    public void raise(Signal signal) {
        delegate.raise(signal);
    }

    @Override
    public PrintWriter writer() {
        return delegate.writer();
    }

    @Override
    public Charset encoding() {
        return delegate.encoding();
    }

    @Override
    public Charset inputEncoding() {
        return delegate.inputEncoding();
    }

    @Override
    public Charset outputEncoding() {
        return delegate.outputEncoding();
    }

    @Override
    public Charset stdinEncoding() {
        return delegate.stdinEncoding();
    }

    @Override
    public Charset stdoutEncoding() {
        return delegate.stdoutEncoding();
    }

    @Override
    public Charset stderrEncoding() {
        return delegate.stderrEncoding();
    }

    @Override
    public InputStream input() {
        return delegate.input();
    }

    @Override
    public OutputStream output() {
        return delegate.output();
    }

    @Override
    public boolean canPauseResume() {
        return delegate.canPauseResume();
    }

    @Override
    public void pause() {
        delegate.pause();
    }

    @Override
    public void pause(boolean wait) throws InterruptedException {
        delegate.pause(wait);
    }

    @Override
    public void resume() {
        delegate.resume();
    }

    @Override
    public boolean paused() {
        return delegate.paused();
    }

    @Override
    public Attributes enterRawMode() {
        return delegate.enterRawMode();
    }

    @Override
    public boolean echo() {
        return delegate.echo();
    }

    @Override
    public boolean echo(boolean echo) {
        return delegate.echo(echo);
    }

    @Override
    public Attributes getAttributes() {
        return delegate.getAttributes();
    }

    @Override
    public void setAttributes(Attributes attr) {
        delegate.setAttributes(attr);
    }

    @Override
    public Size getSize() {
        return delegate.getSize();
    }

    @Override
    public void setSize(Size size) {
        delegate.setSize(size);
    }

    @Override
    public int getWidth() {
        return delegate.getWidth();
    }

    @Override
    public int getHeight() {
        return delegate.getHeight();
    }

    @Override
    public Size getBufferSize() {
        return delegate.getBufferSize();
    }

    @Override
    public void flush() {
        delegate.flush();
    }

    @Override
    public String getType() {
        return delegate.getType();
    }

    @Override
    public boolean puts(Capability capability, Object... params) {
        return delegate.puts(capability, params);
    }

    @Override
    public boolean getBooleanCapability(Capability capability) {
        return delegate.getBooleanCapability(capability);
    }

    @Override
    public Integer getNumericCapability(Capability capability) {
        return delegate.getNumericCapability(capability);
    }

    @Override
    public String getStringCapability(Capability capability) {
        return delegate.getStringCapability(capability);
    }

    @Override
    public Cursor getCursorPosition(IntConsumer discarded) {
        return delegate.getCursorPosition(discarded);
    }

    @Override
    public boolean hasMouseSupport() {
        return delegate.hasMouseSupport();
    }

    @Override
    public boolean trackMouse(MouseTracking tracking) {
        return delegate.trackMouse(tracking);
    }

    @Override
    public MouseTracking getCurrentMouseTracking() {
        return delegate.getCurrentMouseTracking();
    }

    @Override
    public MouseEvent readMouseEvent() {
        return delegate.readMouseEvent();
    }

    @Override
    public MouseEvent readMouseEvent(IntSupplier reader) {
        return delegate.readMouseEvent(reader);
    }

    @Override
    public MouseEvent readMouseEvent(String prefix) {
        return delegate.readMouseEvent(prefix);
    }

    @Override
    public MouseEvent readMouseEvent(IntSupplier reader, String prefix) {
        return delegate.readMouseEvent(reader, prefix);
    }

    @Override
    public boolean hasFocusSupport() {
        return delegate.hasFocusSupport();
    }

    @Override
    public boolean trackFocus(boolean tracking) {
        return delegate.trackFocus(tracking);
    }

    @Override
    public ColorPalette getPalette() {
        return delegate.getPalette();
    }

    @Override
    public int getDefaultForegroundColor() {
        return delegate.getDefaultForegroundColor();
    }

    @Override
    public int getDefaultBackgroundColor() {
        return delegate.getDefaultBackgroundColor();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    /** Fires {@link ActivityListener#fire()} on every character successfully read. */
    private static final class ActivityTrackingReader extends NonBlockingReader {
        private final NonBlockingReader delegate;
        private final ActivityListener listener;

        ActivityTrackingReader(NonBlockingReader delegate, ActivityListener listener) {
            this.delegate = delegate;
            this.listener = listener;
        }

        @Override
        public int readBuffered(char[] b, int off, int len, long timeout) throws IOException {
            int n = delegate.readBuffered(b, off, len, timeout);
            if (n > 0) {
                listener.fire();
            }
            return n;
        }

        // delegate.read(long, boolean) is protected and only callable on "this", so we go through the
        // delegate's public read(long)/peek(long) instead, which dispatch back to it internally.
        @Override
        protected int read(long timeout, boolean isPeek) throws IOException {
            int c = isPeek ? delegate.peek(timeout) : delegate.read(timeout);
            if (!isPeek && c >= 0) {
                listener.fire();
            }
            return c;
        }

        @Override
        public int available() {
            return delegate.available();
        }

        @Override
        public void shutdown() {
            delegate.shutdown();
        }

        @Override
        public void close() throws IOException {
            delegate.close();
        }
    }
}
