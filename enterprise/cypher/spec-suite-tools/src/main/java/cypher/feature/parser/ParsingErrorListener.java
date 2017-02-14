/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package cypher.feature.parser;

import java.util.BitSet;

import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.dfa.DFA;

public class ParsingErrorListener implements ANTLRErrorListener
{
    @Override
    public void syntaxError( Recognizer<?,?> recognizer, Object o, int i, int i1, String s, RecognitionException e )
    {
        throw new IllegalArgumentException( "Can not parse value in feature file", e );
    }

    @Override
    public void reportAmbiguity( Parser parser, DFA dfa, int i, int i1, boolean b, BitSet bitSet,
            ATNConfigSet atnConfigSet )
    {
        throw new IllegalArgumentException( "Can not parse value in feature file" );
    }

    @Override
    public void reportAttemptingFullContext( Parser parser, DFA dfa, int i, int i1, BitSet bitSet,
            ATNConfigSet atnConfigSet )
    {
        throw new IllegalArgumentException( "Can not parse value in feature file" );
    }

    @Override
    public void reportContextSensitivity( Parser parser, DFA dfa, int i, int i1, int i2, ATNConfigSet atnConfigSet )
    {
        throw new IllegalArgumentException( "Can not parse value in feature file" );
    }
}
