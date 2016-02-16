/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
// Generated from /Users/mats/gitRoots/3.0-copy/neo4j/community/cypher/compatibility-suite/src/main/resources/FeatureResults.g4 by ANTLR 4.5.1
package cypher.feature.parser.generated;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class FeatureResultsParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.5.1", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, IntegerLiteral=14, DecimalLiteral=15, 
		DIGIT=16, NONZERODIGIT=17, INFINITY=18, FloatingPointLiteral=19, FloatingPointRepr=20, 
		EXPONENTPART=21, SymbolicNameString=22, WS=23, IDENTIFIER=24, StringLiteral=25;
	public static final int
		RULE_value = 0, RULE_node = 1, RULE_relationship = 2, RULE_path = 3, RULE_pathElement = 4, 
		RULE_integer = 5, RULE_floatingPoint = 6, RULE_bool = 7, RULE_nullValue = 8, 
		RULE_list = 9, RULE_listContents = 10, RULE_listElement = 11, RULE_map = 12, 
		RULE_propertyMap = 13, RULE_mapContents = 14, RULE_keyValuePair = 15, 
		RULE_propertyKey = 16, RULE_propertyValue = 17, RULE_relationshipType = 18, 
		RULE_relationshipTypeName = 19, RULE_label = 20, RULE_labelName = 21, 
		RULE_string = 22;
	public static final String[] ruleNames = {
		"value", "node", "relationship", "path", "pathElement", "integer", "floatingPoint", 
		"bool", "nullValue", "list", "listContents", "listElement", "map", "propertyMap", 
		"mapContents", "keyValuePair", "propertyKey", "propertyValue", "relationshipType", 
		"relationshipTypeName", "label", "labelName", "string"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'('", "')'", "'['", "']'", "'<'", "'>'", "', '", "'true'", "'false'", 
		"'null'", "'{'", "'}'", "':'", null, null, null, null, null, null, null, 
		null, null, "' '"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, "IntegerLiteral", "DecimalLiteral", "DIGIT", "NONZERODIGIT", 
		"INFINITY", "FloatingPointLiteral", "FloatingPointRepr", "EXPONENTPART", 
		"SymbolicNameString", "WS", "IDENTIFIER", "StringLiteral"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}

	@Override
	public String getGrammarFileName() { return "FeatureResults.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public FeatureResultsParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class ValueContext extends ParserRuleContext {
		public NodeContext node() {
			return getRuleContext(NodeContext.class,0);
		}
		public RelationshipContext relationship() {
			return getRuleContext(RelationshipContext.class,0);
		}
		public PathContext path() {
			return getRuleContext(PathContext.class,0);
		}
		public IntegerContext integer() {
			return getRuleContext(IntegerContext.class,0);
		}
		public FloatingPointContext floatingPoint() {
			return getRuleContext(FloatingPointContext.class,0);
		}
		public StringContext string() {
			return getRuleContext(StringContext.class,0);
		}
		public BoolContext bool() {
			return getRuleContext(BoolContext.class,0);
		}
		public NullValueContext nullValue() {
			return getRuleContext(NullValueContext.class,0);
		}
		public ListContext list() {
			return getRuleContext(ListContext.class,0);
		}
		public MapContext map() {
			return getRuleContext(MapContext.class,0);
		}
		public ValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_value; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitValue(this);
		}
	}

	public final ValueContext value() throws RecognitionException {
		ValueContext _localctx = new ValueContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_value);
		try {
			setState(56);
			switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(46);
				node();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(47);
				relationship();
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(48);
				path();
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(49);
				integer();
				}
				break;
			case 5:
				enterOuterAlt(_localctx, 5);
				{
				setState(50);
				floatingPoint();
				}
				break;
			case 6:
				enterOuterAlt(_localctx, 6);
				{
				setState(51);
				string();
				}
				break;
			case 7:
				enterOuterAlt(_localctx, 7);
				{
				setState(52);
				bool();
				}
				break;
			case 8:
				enterOuterAlt(_localctx, 8);
				{
				setState(53);
				nullValue();
				}
				break;
			case 9:
				enterOuterAlt(_localctx, 9);
				{
				setState(54);
				list();
				}
				break;
			case 10:
				enterOuterAlt(_localctx, 10);
				{
				setState(55);
				map();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NodeContext extends ParserRuleContext {
		public List<LabelContext> label() {
			return getRuleContexts(LabelContext.class);
		}
		public LabelContext label(int i) {
			return getRuleContext(LabelContext.class,i);
		}
		public TerminalNode WS() { return getToken(FeatureResultsParser.WS, 0); }
		public PropertyMapContext propertyMap() {
			return getRuleContext(PropertyMapContext.class,0);
		}
		public NodeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_node; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterNode(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitNode(this);
		}
	}

	public final NodeContext node() throws RecognitionException {
		NodeContext _localctx = new NodeContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_node);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(58);
			match(T__0);
			setState(62);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__12) {
				{
				{
				setState(59);
				label();
				}
				}
				setState(64);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(66);
			_la = _input.LA(1);
			if (_la==WS) {
				{
				setState(65);
				match(WS);
				}
			}

			setState(69);
			_la = _input.LA(1);
			if (_la==T__10) {
				{
				setState(68);
				propertyMap();
				}
			}

			setState(71);
			match(T__1);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationshipContext extends ParserRuleContext {
		public RelationshipTypeContext relationshipType() {
			return getRuleContext(RelationshipTypeContext.class,0);
		}
		public List<TerminalNode> WS() { return getTokens(FeatureResultsParser.WS); }
		public TerminalNode WS(int i) {
			return getToken(FeatureResultsParser.WS, i);
		}
		public List<PropertyMapContext> propertyMap() {
			return getRuleContexts(PropertyMapContext.class);
		}
		public PropertyMapContext propertyMap(int i) {
			return getRuleContext(PropertyMapContext.class,i);
		}
		public RelationshipContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationship; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterRelationship(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitRelationship(this);
		}
	}

	public final RelationshipContext relationship() throws RecognitionException {
		RelationshipContext _localctx = new RelationshipContext(_ctx, getState());
		enterRule(_localctx, 4, RULE_relationship);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(73);
			match(T__2);
			setState(74);
			relationshipType();
			setState(79);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==WS) {
				{
				{
				setState(75);
				match(WS);
				setState(76);
				propertyMap();
				}
				}
				setState(81);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			setState(82);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PathContext extends ParserRuleContext {
		public PathElementContext pathElement() {
			return getRuleContext(PathElementContext.class,0);
		}
		public PathContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_path; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterPath(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitPath(this);
		}
	}

	public final PathContext path() throws RecognitionException {
		PathContext _localctx = new PathContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_path);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(84);
			match(T__4);
			setState(86);
			_la = _input.LA(1);
			if (_la==T__0) {
				{
				setState(85);
				pathElement();
				}
			}

			setState(88);
			match(T__5);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PathElementContext extends ParserRuleContext {
		public List<NodeContext> node() {
			return getRuleContexts(NodeContext.class);
		}
		public NodeContext node(int i) {
			return getRuleContext(NodeContext.class,i);
		}
		public List<RelationshipContext> relationship() {
			return getRuleContexts(RelationshipContext.class);
		}
		public RelationshipContext relationship(int i) {
			return getRuleContext(RelationshipContext.class,i);
		}
		public PathElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_pathElement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterPathElement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitPathElement(this);
		}
	}

	public final PathElementContext pathElement() throws RecognitionException {
		PathElementContext _localctx = new PathElementContext(_ctx, getState());
		enterRule(_localctx, 8, RULE_pathElement);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(90);
			node();
			setState(98);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__6) {
				{
				{
				setState(91);
				match(T__6);
				setState(92);
				relationship();
				setState(93);
				match(T__6);
				setState(94);
				node();
				}
				}
				setState(100);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class IntegerContext extends ParserRuleContext {
		public TerminalNode IntegerLiteral() { return getToken(FeatureResultsParser.IntegerLiteral, 0); }
		public IntegerContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_integer; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterInteger(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitInteger(this);
		}
	}

	public final IntegerContext integer() throws RecognitionException {
		IntegerContext _localctx = new IntegerContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_integer);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(101);
			match(IntegerLiteral);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class FloatingPointContext extends ParserRuleContext {
		public TerminalNode FloatingPointLiteral() { return getToken(FeatureResultsParser.FloatingPointLiteral, 0); }
		public TerminalNode INFINITY() { return getToken(FeatureResultsParser.INFINITY, 0); }
		public FloatingPointContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_floatingPoint; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterFloatingPoint(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitFloatingPoint(this);
		}
	}

	public final FloatingPointContext floatingPoint() throws RecognitionException {
		FloatingPointContext _localctx = new FloatingPointContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_floatingPoint);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(103);
			_la = _input.LA(1);
			if ( !(_la==INFINITY || _la==FloatingPointLiteral) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class BoolContext extends ParserRuleContext {
		public BoolContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_bool; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterBool(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitBool(this);
		}
	}

	public final BoolContext bool() throws RecognitionException {
		BoolContext _localctx = new BoolContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_bool);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(105);
			_la = _input.LA(1);
			if ( !(_la==T__7 || _la==T__8) ) {
			_errHandler.recoverInline(this);
			} else {
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NullValueContext extends ParserRuleContext {
		public NullValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_nullValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterNullValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitNullValue(this);
		}
	}

	public final NullValueContext nullValue() throws RecognitionException {
		NullValueContext _localctx = new NullValueContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_nullValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(107);
			match(T__9);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ListContext extends ParserRuleContext {
		public ListContentsContext listContents() {
			return getRuleContext(ListContentsContext.class,0);
		}
		public ListContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_list; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterList(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitList(this);
		}
	}

	public final ListContext list() throws RecognitionException {
		ListContext _localctx = new ListContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_list);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(109);
			match(T__2);
			setState(111);
			_la = _input.LA(1);
			if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__0) | (1L << T__2) | (1L << T__4) | (1L << T__7) | (1L << T__8) | (1L << T__9) | (1L << T__10) | (1L << IntegerLiteral) | (1L << INFINITY) | (1L << FloatingPointLiteral) | (1L << StringLiteral))) != 0)) {
				{
				setState(110);
				listContents();
				}
			}

			setState(113);
			match(T__3);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ListContentsContext extends ParserRuleContext {
		public List<ListElementContext> listElement() {
			return getRuleContexts(ListElementContext.class);
		}
		public ListElementContext listElement(int i) {
			return getRuleContext(ListElementContext.class,i);
		}
		public ListContentsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_listContents; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterListContents(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitListContents(this);
		}
	}

	public final ListContentsContext listContents() throws RecognitionException {
		ListContentsContext _localctx = new ListContentsContext(_ctx, getState());
		enterRule(_localctx, 20, RULE_listContents);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(115);
			listElement();
			setState(120);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__6) {
				{
				{
				setState(116);
				match(T__6);
				setState(117);
				listElement();
				}
				}
				setState(122);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ListElementContext extends ParserRuleContext {
		public ValueContext value() {
			return getRuleContext(ValueContext.class,0);
		}
		public ListElementContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_listElement; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterListElement(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitListElement(this);
		}
	}

	public final ListElementContext listElement() throws RecognitionException {
		ListElementContext _localctx = new ListElementContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_listElement);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(123);
			value();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MapContext extends ParserRuleContext {
		public PropertyMapContext propertyMap() {
			return getRuleContext(PropertyMapContext.class,0);
		}
		public MapContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_map; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterMap(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitMap(this);
		}
	}

	public final MapContext map() throws RecognitionException {
		MapContext _localctx = new MapContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_map);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(125);
			propertyMap();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PropertyMapContext extends ParserRuleContext {
		public MapContentsContext mapContents() {
			return getRuleContext(MapContentsContext.class,0);
		}
		public PropertyMapContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyMap; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterPropertyMap(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitPropertyMap(this);
		}
	}

	public final PropertyMapContext propertyMap() throws RecognitionException {
		PropertyMapContext _localctx = new PropertyMapContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_propertyMap);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(127);
			match(T__10);
			setState(129);
			_la = _input.LA(1);
			if (_la==SymbolicNameString) {
				{
				setState(128);
				mapContents();
				}
			}

			setState(131);
			match(T__11);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MapContentsContext extends ParserRuleContext {
		public List<KeyValuePairContext> keyValuePair() {
			return getRuleContexts(KeyValuePairContext.class);
		}
		public KeyValuePairContext keyValuePair(int i) {
			return getRuleContext(KeyValuePairContext.class,i);
		}
		public MapContentsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_mapContents; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterMapContents(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitMapContents(this);
		}
	}

	public final MapContentsContext mapContents() throws RecognitionException {
		MapContentsContext _localctx = new MapContentsContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_mapContents);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(133);
			keyValuePair();
			setState(138);
			_errHandler.sync(this);
			_la = _input.LA(1);
			while (_la==T__6) {
				{
				{
				setState(134);
				match(T__6);
				setState(135);
				keyValuePair();
				}
				}
				setState(140);
				_errHandler.sync(this);
				_la = _input.LA(1);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class KeyValuePairContext extends ParserRuleContext {
		public PropertyKeyContext propertyKey() {
			return getRuleContext(PropertyKeyContext.class,0);
		}
		public PropertyValueContext propertyValue() {
			return getRuleContext(PropertyValueContext.class,0);
		}
		public KeyValuePairContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_keyValuePair; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterKeyValuePair(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitKeyValuePair(this);
		}
	}

	public final KeyValuePairContext keyValuePair() throws RecognitionException {
		KeyValuePairContext _localctx = new KeyValuePairContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_keyValuePair);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(141);
			propertyKey();
			setState(142);
			match(T__12);
			setState(143);
			propertyValue();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PropertyKeyContext extends ParserRuleContext {
		public TerminalNode SymbolicNameString() { return getToken(FeatureResultsParser.SymbolicNameString, 0); }
		public PropertyKeyContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyKey; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterPropertyKey(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitPropertyKey(this);
		}
	}

	public final PropertyKeyContext propertyKey() throws RecognitionException {
		PropertyKeyContext _localctx = new PropertyKeyContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_propertyKey);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(145);
			match(SymbolicNameString);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class PropertyValueContext extends ParserRuleContext {
		public ValueContext value() {
			return getRuleContext(ValueContext.class,0);
		}
		public PropertyValueContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_propertyValue; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterPropertyValue(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitPropertyValue(this);
		}
	}

	public final PropertyValueContext propertyValue() throws RecognitionException {
		PropertyValueContext _localctx = new PropertyValueContext(_ctx, getState());
		enterRule(_localctx, 34, RULE_propertyValue);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(147);
			value();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationshipTypeContext extends ParserRuleContext {
		public RelationshipTypeNameContext relationshipTypeName() {
			return getRuleContext(RelationshipTypeNameContext.class,0);
		}
		public RelationshipTypeContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationshipType; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterRelationshipType(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitRelationshipType(this);
		}
	}

	public final RelationshipTypeContext relationshipType() throws RecognitionException {
		RelationshipTypeContext _localctx = new RelationshipTypeContext(_ctx, getState());
		enterRule(_localctx, 36, RULE_relationshipType);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(149);
			match(T__12);
			setState(150);
			relationshipTypeName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationshipTypeNameContext extends ParserRuleContext {
		public TerminalNode SymbolicNameString() { return getToken(FeatureResultsParser.SymbolicNameString, 0); }
		public RelationshipTypeNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationshipTypeName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterRelationshipTypeName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitRelationshipTypeName(this);
		}
	}

	public final RelationshipTypeNameContext relationshipTypeName() throws RecognitionException {
		RelationshipTypeNameContext _localctx = new RelationshipTypeNameContext(_ctx, getState());
		enterRule(_localctx, 38, RULE_relationshipTypeName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(152);
			match(SymbolicNameString);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LabelContext extends ParserRuleContext {
		public LabelNameContext labelName() {
			return getRuleContext(LabelNameContext.class,0);
		}
		public LabelContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_label; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterLabel(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitLabel(this);
		}
	}

	public final LabelContext label() throws RecognitionException {
		LabelContext _localctx = new LabelContext(_ctx, getState());
		enterRule(_localctx, 40, RULE_label);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(154);
			match(T__12);
			setState(155);
			labelName();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LabelNameContext extends ParserRuleContext {
		public TerminalNode SymbolicNameString() { return getToken(FeatureResultsParser.SymbolicNameString, 0); }
		public LabelNameContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_labelName; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterLabelName(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitLabelName(this);
		}
	}

	public final LabelNameContext labelName() throws RecognitionException {
		LabelNameContext _localctx = new LabelNameContext(_ctx, getState());
		enterRule(_localctx, 42, RULE_labelName);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(157);
			match(SymbolicNameString);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class StringContext extends ParserRuleContext {
		public TerminalNode StringLiteral() { return getToken(FeatureResultsParser.StringLiteral, 0); }
		public StringContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_string; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).enterString(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof FeatureResultsListener ) ((FeatureResultsListener)listener).exitString(this);
		}
	}

	public final StringContext string() throws RecognitionException {
		StringContext _localctx = new StringContext(_ctx, getState());
		enterRule(_localctx, 44, RULE_string);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(159);
			match(StringLiteral);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static final String _serializedATN =
		"\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\33\u00a4\4\2\t\2"+
		"\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13"+
		"\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\3\2\3\2\3"+
		"\2\3\2\3\2\3\2\3\2\3\2\3\2\3\2\5\2;\n\2\3\3\3\3\7\3?\n\3\f\3\16\3B\13"+
		"\3\3\3\5\3E\n\3\3\3\5\3H\n\3\3\3\3\3\3\4\3\4\3\4\3\4\7\4P\n\4\f\4\16\4"+
		"S\13\4\3\4\3\4\3\5\3\5\5\5Y\n\5\3\5\3\5\3\6\3\6\3\6\3\6\3\6\3\6\7\6c\n"+
		"\6\f\6\16\6f\13\6\3\7\3\7\3\b\3\b\3\t\3\t\3\n\3\n\3\13\3\13\5\13r\n\13"+
		"\3\13\3\13\3\f\3\f\3\f\7\fy\n\f\f\f\16\f|\13\f\3\r\3\r\3\16\3\16\3\17"+
		"\3\17\5\17\u0084\n\17\3\17\3\17\3\20\3\20\3\20\7\20\u008b\n\20\f\20\16"+
		"\20\u008e\13\20\3\21\3\21\3\21\3\21\3\22\3\22\3\23\3\23\3\24\3\24\3\24"+
		"\3\25\3\25\3\26\3\26\3\26\3\27\3\27\3\30\3\30\3\30\2\2\31\2\4\6\b\n\f"+
		"\16\20\22\24\26\30\32\34\36 \"$&(*,.\2\4\3\2\24\25\3\2\n\13\u009f\2:\3"+
		"\2\2\2\4<\3\2\2\2\6K\3\2\2\2\bV\3\2\2\2\n\\\3\2\2\2\fg\3\2\2\2\16i\3\2"+
		"\2\2\20k\3\2\2\2\22m\3\2\2\2\24o\3\2\2\2\26u\3\2\2\2\30}\3\2\2\2\32\177"+
		"\3\2\2\2\34\u0081\3\2\2\2\36\u0087\3\2\2\2 \u008f\3\2\2\2\"\u0093\3\2"+
		"\2\2$\u0095\3\2\2\2&\u0097\3\2\2\2(\u009a\3\2\2\2*\u009c\3\2\2\2,\u009f"+
		"\3\2\2\2.\u00a1\3\2\2\2\60;\5\4\3\2\61;\5\6\4\2\62;\5\b\5\2\63;\5\f\7"+
		"\2\64;\5\16\b\2\65;\5.\30\2\66;\5\20\t\2\67;\5\22\n\28;\5\24\13\29;\5"+
		"\32\16\2:\60\3\2\2\2:\61\3\2\2\2:\62\3\2\2\2:\63\3\2\2\2:\64\3\2\2\2:"+
		"\65\3\2\2\2:\66\3\2\2\2:\67\3\2\2\2:8\3\2\2\2:9\3\2\2\2;\3\3\2\2\2<@\7"+
		"\3\2\2=?\5*\26\2>=\3\2\2\2?B\3\2\2\2@>\3\2\2\2@A\3\2\2\2AD\3\2\2\2B@\3"+
		"\2\2\2CE\7\31\2\2DC\3\2\2\2DE\3\2\2\2EG\3\2\2\2FH\5\34\17\2GF\3\2\2\2"+
		"GH\3\2\2\2HI\3\2\2\2IJ\7\4\2\2J\5\3\2\2\2KL\7\5\2\2LQ\5&\24\2MN\7\31\2"+
		"\2NP\5\34\17\2OM\3\2\2\2PS\3\2\2\2QO\3\2\2\2QR\3\2\2\2RT\3\2\2\2SQ\3\2"+
		"\2\2TU\7\6\2\2U\7\3\2\2\2VX\7\7\2\2WY\5\n\6\2XW\3\2\2\2XY\3\2\2\2YZ\3"+
		"\2\2\2Z[\7\b\2\2[\t\3\2\2\2\\d\5\4\3\2]^\7\t\2\2^_\5\6\4\2_`\7\t\2\2`"+
		"a\5\4\3\2ac\3\2\2\2b]\3\2\2\2cf\3\2\2\2db\3\2\2\2de\3\2\2\2e\13\3\2\2"+
		"\2fd\3\2\2\2gh\7\20\2\2h\r\3\2\2\2ij\t\2\2\2j\17\3\2\2\2kl\t\3\2\2l\21"+
		"\3\2\2\2mn\7\f\2\2n\23\3\2\2\2oq\7\5\2\2pr\5\26\f\2qp\3\2\2\2qr\3\2\2"+
		"\2rs\3\2\2\2st\7\6\2\2t\25\3\2\2\2uz\5\30\r\2vw\7\t\2\2wy\5\30\r\2xv\3"+
		"\2\2\2y|\3\2\2\2zx\3\2\2\2z{\3\2\2\2{\27\3\2\2\2|z\3\2\2\2}~\5\2\2\2~"+
		"\31\3\2\2\2\177\u0080\5\34\17\2\u0080\33\3\2\2\2\u0081\u0083\7\r\2\2\u0082"+
		"\u0084\5\36\20\2\u0083\u0082\3\2\2\2\u0083\u0084\3\2\2\2\u0084\u0085\3"+
		"\2\2\2\u0085\u0086\7\16\2\2\u0086\35\3\2\2\2\u0087\u008c\5 \21\2\u0088"+
		"\u0089\7\t\2\2\u0089\u008b\5 \21\2\u008a\u0088\3\2\2\2\u008b\u008e\3\2"+
		"\2\2\u008c\u008a\3\2\2\2\u008c\u008d\3\2\2\2\u008d\37\3\2\2\2\u008e\u008c"+
		"\3\2\2\2\u008f\u0090\5\"\22\2\u0090\u0091\7\17\2\2\u0091\u0092\5$\23\2"+
		"\u0092!\3\2\2\2\u0093\u0094\7\30\2\2\u0094#\3\2\2\2\u0095\u0096\5\2\2"+
		"\2\u0096%\3\2\2\2\u0097\u0098\7\17\2\2\u0098\u0099\5(\25\2\u0099\'\3\2"+
		"\2\2\u009a\u009b\7\30\2\2\u009b)\3\2\2\2\u009c\u009d\7\17\2\2\u009d\u009e"+
		"\5,\27\2\u009e+\3\2\2\2\u009f\u00a0\7\30\2\2\u00a0-\3\2\2\2\u00a1\u00a2"+
		"\7\33\2\2\u00a2/\3\2\2\2\r:@DGQXdqz\u0083\u008c";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}