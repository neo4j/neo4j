/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
