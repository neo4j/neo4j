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
package org.neo4j.shell.printer;

import org.fusesource.jansi.Ansi;

/**
 * A piece of text which can be rendered with Ansi format codes.
 */
public class AnsiFormattedText {
    private final Ansi ansi = Ansi.ansi();
    private final StringBuilder plain = new StringBuilder();
    private boolean needsReset = false;

    /** Returns a new empty instance */
    public static AnsiFormattedText s() {
        return new AnsiFormattedText();
    }

    public static AnsiFormattedText from(String string) {
        return new AnsiFormattedText().append(string != null ? string : "");
    }

    private AnsiFormattedText reset() {
        if (needsReset) {
            ansi.reset();
            needsReset = false;
        }
        return this;
    }

    /** Adds reset and returns the formatted string. */
    public String resetAndRender() {
        reset();
        return ansi.toString();
    }

    /** Returns the text without formatting. */
    public String plainString() {
        return plain.toString();
    }

    /** Append a string using the current formatting */
    public AnsiFormattedText append(CharSequence s) {
        ansi.a(s);
        plain.append(s);
        return this;
    }

    public AnsiFormattedText append(AnsiFormattedText s) {
        ansi.a(s.resetAndRender());
        plain.append(s.plain);
        needsReset = false;
        return this;
    }

    public AnsiFormattedText newLine() {
        return append(System.lineSeparator());
    }

    private AnsiFormattedText formatChange() {
        needsReset = true;
        return this;
    }

    /** Append bold ansi code. */
    public AnsiFormattedText bold() {
        ansi.bold();
        return formatChange();
    }

    public AnsiFormattedText bold(String bold) {
        return bold().append(bold).boldOff();
    }

    /** Append bold reset ansi code. */
    public AnsiFormattedText boldOff() {
        ansi.boldOff();
        return formatChange();
    }

    /** Append red foreground ansi code. */
    public AnsiFormattedText colorRed() {
        ansi.fgRed();
        return formatChange();
    }

    public AnsiFormattedText colorRed(CharSequence s) {
        return colorRed().append(s).colorDefault();
    }

    /** Append bright red foreground ansi code. */
    public AnsiFormattedText brightRed() {
        ansi.fgBrightRed();
        return formatChange();
    }

    public AnsiFormattedText brightRed(CharSequence s) {
        return brightRed().append(s).colorDefault();
    }

    /** Append yellow foreground ansi code. */
    public AnsiFormattedText colorOrange() {
        ansi.fgYellow();
        return formatChange();
    }

    public AnsiFormattedText orange(String s) {
        return colorOrange().append(s).colorDefault();
    }

    /** Append reset foreground color ansi code. */
    public AnsiFormattedText colorDefault() {
        ansi.fgDefault();
        return formatChange();
    }

    public int textLength() {
        return plain.length();
    }
}
