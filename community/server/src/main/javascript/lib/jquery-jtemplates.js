/**
 * jTemplates 0.7.8 (http://jtemplates.tpython.com)
 * Copyright (c) 2007-2009 Tomasz Gloc (http://www.tpython.com)
 * 
 * Dual licensed under the MIT (MIT-LICENSE.txt)
 * and/or GPL (GPL-LICENSE.txt) licenses.
 *
 * Id: $Id: jquery-jtemplates_uncompressed.js 177 2009-04-02 17:36:36Z tom $
 */
 
 /**
 * @fileOverview Template engine in JavaScript.
 * @name jTemplates
 * @author Tomasz Gloc
 * @date $Date: 2009-04-02 19:36:36 +0200 (Cz, 02 kwi 2009) $
 */


if(window.jQuery && !window.jQuery.createTemplate) {(function(jQuery) {
	
	/**
	 * [abstract]
	 * @name BaseNode
	 * @class Abstract node. [abstract]
	 */
	
	/**
	 * Process node and get the html string. [abstract]
	 * @name get
	 * @function
	 * @param {object} d data
	 * @param {object} param parameters
	 * @param {Element} element a HTML element
	 * @param {Number} deep
	 * @return {String}
	 * @memberOf BaseNode
	 */
	
	/**
	 * [abstract]
	 * @name BaseArray
	 * @augments BaseNode
	 * @class Abstract array/collection. [abstract]
	 */
	
	/**
	 * Add node 'e' to array.
	 * @name push
	 * @function
	 * @param {BaseNode} e a node
	 * @memberOf BaseArray
	 */
	
	/**
	 * See (http://jquery.com/).
	 * @name jQuery
	 * @class jQuery Library (http://jquery.com/)
	 */
	
	/**
	 * See (http://jquery.com/)
	 * @name fn
	 * @class jQuery Library (http://jquery.com/)
	 * @memberOf jQuery
	 */
	
	
	/**
	 * Create new template from string s.
	 * @name Template
	 * @class A template or multitemplate.
	 * @param {string} s A template string (like: "Text: {$T.txt}.").
	 * @param {array} [includes] Array of included templates.
	 * @param {object} [settings] Settings.
	 * @config {boolean} [disallow_functions] Do not allow use function in data (default: true).
	 * @config {boolean} [filter_data] Enable filter data using escapeHTML (default: true).
	 * @config {boolean} [filter_params] Enable filter parameters using escapeHTML (default: false).
	 * @config {boolean} [runnable_functions] Automatically run function (from data) inside {} [default: false].
	 * @config {boolean} [clone_data] Clone input data [default: true]
	 * @config {boolean} [clone_params] Clone input parameters [default: true]
	 * @config {Function} [f_cloneData] Function using to data cloning
	 * @config {Function} [f_escapeString] Function using to escape strings
	 * @augments BaseNode
	 */
	var Template = function(s, includes, settings) {
		this._tree = [];
		this._param = {};
		this._includes = null;
		this._templates = {};
		this._templates_code = {};
		
		this.settings = jQuery.extend({
			disallow_functions: false,
			filter_data: true,
			filter_params: false,
			runnable_functions: false,
			clone_data: true,
			clone_params: true
		}, settings);
		
		this.f_cloneData = (this.settings.f_cloneData !== undefined) ? (this.settings.f_cloneData) : (TemplateUtils.cloneData);
		this.f_escapeString = (this.settings.f_escapeString !== undefined) ? (this.settings.f_escapeString) : (TemplateUtils.escapeHTML);
		
		this.splitTemplates(s, includes);
		
		if(s) {
			this.setTemplate(this._templates_code['MAIN'], includes, this.settings);
		}
		
		this._templates_code = null;
	};
	
	/**
	 * jTemplates version
	 * @type string
	 */
	Template.prototype.version = '0.7.8';
	
	/**
	 * Debug mode (all errors are on), default: on
	 * @type Boolean
	 */
	Template.DEBUG_MODE = true;
	
	/**
	 * Split multitemplate into multiple templates.
	 * @param {string} s A template string (like: "Text: {$T.txt}.").
	 * @param {array} includes Array of included templates.
	 */
	Template.prototype.splitTemplates = function(s, includes) {
		var reg = /\{#template *(\w*?)( .*)*\}/g;
		var iter, tname, se;
		var lastIndex = null;
		
		var _template_settings = [];
		
		while((iter = reg.exec(s)) != null) {
			lastIndex = reg.lastIndex;
			tname = iter[1];
			se = s.indexOf('{#/template ' + tname + '}', lastIndex);
			if(se == -1) {
				throw new Error('jTemplates: Template "' + tname + '" is not closed.');
			}
			this._templates_code[tname] = s.substring(lastIndex, se);
			_template_settings[tname] = TemplateUtils.optionToObject(iter[2]);
		}
		if(lastIndex === null) {
			this._templates_code['MAIN'] = s;
			return;
		}
		
		for(var i in this._templates_code) {
			if(i != 'MAIN') {
				this._templates[i] = new Template();
			}
		}
		for(var i in this._templates_code) {
			if(i != 'MAIN') {
				this._templates[i].setTemplate(this._templates_code[i], jQuery.extend({}, includes || {}, this._templates || {}), jQuery.extend({}, this.settings, _template_settings[i]));
				this._templates_code[i] = null;
			}
		}
	};
	
	/**
	 * Parse template. (should be template, not multitemplate).
	 * @param {string} s A template string (like: "Text: {$T.txt}.").
	 * @param {array} includes Array of included templates.
	 */
	Template.prototype.setTemplate = function(s, includes, settings) {
		if(s == undefined) {
			this._tree.push(new TextNode('', 1, this));
			return;
		}
		s = s.replace(/[\n\r]/g, '');
		s = s.replace(/\{\*.*?\*\}/g, '');
		this._includes = jQuery.extend({}, this._templates || {}, includes || {});
		this.settings = new Object(settings);
		var node = this._tree;
		var op = s.match(/\{#.*?\}/g);
		var ss = 0, se = 0;
		var e;
		var literalMode = 0;
		var elseif_level = 0;
		
		for(var i=0, l=(op)?(op.length):(0); i<l; ++i) {
			var this_op = op[i];
			
			if(literalMode) {
				se = s.indexOf('{#/literal}');
				if(se == -1) {
					throw new Error("jTemplates: No end of literal.");
				}
				if(se > ss) {
					node.push(new TextNode(s.substring(ss, se), 1, this));
				}
				ss = se + 11;
				literalMode = 0;
				i = jQuery.inArray('{#/literal}', op);
				continue;
			}
			se = s.indexOf(this_op, ss);
			if(se > ss) {
				node.push(new TextNode(s.substring(ss, se), literalMode, this));
			}
			var ppp = this_op.match(/\{#([\w\/]+).*?\}/);
			var op_ = RegExp.$1;
			switch(op_) {
				case 'elseif':
					++elseif_level;
					node.switchToElse();
					//no break
				case 'if':
					e = new opIF(this_op, node);
					node.push(e);
					node = e;
					break;
				case 'else':
					node.switchToElse();
					break;
				case '/if':
					while(elseif_level) {
						node = node.getParent();
						--elseif_level;
					}
					//no break
				case '/for':
				case '/foreach':
					node = node.getParent();
					break;
				case 'foreach':
					e = new opFOREACH(this_op, node, this);
					node.push(e);
					node = e;
					break;
				case 'for':
					e = opFORFactory(this_op, node, this);
					node.push(e);
					node = e;
					break;
				case 'continue':
				case 'break':
					node.push(new JTException(op_));
					break;
				case 'include':
					node.push(new Include(this_op, this._includes));
					break;
				case 'param':
					node.push(new UserParam(this_op));
					break;
				case 'cycle':
					node.push(new Cycle(this_op));
					break;
				case 'ldelim':
					node.push(new TextNode('{', 1, this));
					break;
				case 'rdelim':
					node.push(new TextNode('}', 1, this));
					break;
				case 'literal':
					literalMode = 1;
					break;
				case '/literal':
					if(Template.DEBUG_MODE) {
						throw new Error("jTemplates: Missing begin of literal.");
					}
					break;
				default:
					if(Template.DEBUG_MODE) {
						throw new Error('jTemplates: unknown tag: ' + op_ + '.');
					}
			}
	
			ss = se + this_op.length;
		}
	
		if(s.length > ss) {
			node.push(new TextNode(s.substr(ss), literalMode, this));
		}
	};
	
	/**
	 * Process template and get the html string.
	 * @param {object} d data
	 * @param {object} param parameters
	 * @param {Element} element a HTML element
	 * @param {Number} deep
	 * @return {String}
	 */
	Template.prototype.get = function(d, param, element, deep) {
		++deep;
		
		var $T = d, _param1, _param2;
		
		if(this.settings.clone_data) {
			$T = this.f_cloneData(d, {escapeData: (this.settings.filter_data && deep == 1), noFunc: this.settings.disallow_functions}, this.f_escapeString);
		}
		
		if(!this.settings.clone_params) {
			_param1 = this._param;
			_param2 = param;
		} else {
			_param1 = this.f_cloneData(this._param, {escapeData: (this.settings.filter_params), noFunc: false}, this.f_escapeString);
			_param2 = this.f_cloneData(param, {escapeData: (this.settings.filter_params && deep == 1), noFunc: false}, this.f_escapeString);
		}
		var $P = jQuery.extend({}, _param1, _param2);
		
		var $Q = (element != undefined) ? (element) : ({});
		$Q.version = this.version;
		
		var ret = '';
		for(var i=0, l=this._tree.length; i<l; ++i) {
			ret += this._tree[i].get($T, $P, $Q, deep);
		}
		
		--deep;
		return ret;
	};
	
	/**
	 * Set to parameter 'name' value 'value'.
	 * @param {string} name
	 * @param {object} value
	 */
	Template.prototype.setParam = function(name, value) {
		this._param[name] = value;
	};


	/**
	 * Template utilities.
	 * @namespace Template utilities.
	 */
	TemplateUtils = function() {
	};
	
	/**
	 * Replace chars &, >, <, ", ' with html entities.
	 * To disable function set settings: filter_data=false, filter_params=false
	 * @param {string} string
	 * @return {string}
	 * @static
	 * @memberOf TemplateUtils
	 */
	TemplateUtils.escapeHTML = function(txt) {
		return txt.replace(/&/g,'&amp;').replace(/>/g,'&gt;').replace(/</g,'&lt;').replace(/"/g,'&quot;').replace(/'/g,'&#39;');
	};

	/**
	 * Make a copy od data 'd'. It also filters data (depend on 'filter').
	 * @param {object} d input data
	 * @param {object} filter a filters
	 * @config {boolean} [escapeData] Use escapeHTML on every string.
	 * @config {boolean} [noFunc] Do not allow to use function (throws exception).
	 * @param {Function} f_escapeString function using to filter string (usually: TemplateUtils.escapeHTML)
	 * @return {object} output data (filtered)
	 * @static
	 * @memberOf TemplateUtils
	 */
	TemplateUtils.cloneData = function(d, filter, f_escapeString) {
		if(d == null) {
			return d;
		}
		switch(d.constructor) {
			case Object:
				var o = {};
				for(var i in d) {
					o[i] = TemplateUtils.cloneData(d[i], filter, f_escapeString);
				}
				if(!filter.noFunc) {
					if(d.hasOwnProperty("toString"))
						o.toString = d.toString;
				}
				return o;
			case Array:
				var o = [];
				for(var i=0,l=d.length; i<l; ++i) {
					o[i] = TemplateUtils.cloneData(d[i], filter, f_escapeString);
				}
				return o;
			case String:
				return (filter.escapeData) ? (f_escapeString(d)) : (d);
			case Function:
				if(filter.noFunc) {
					if(Template.DEBUG_MODE)
						throw new Error("jTemplates: Functions are not allowed.");
					else
						return undefined;
				}
				//no break
			default:
				return d;
		}
	};
	
	/**
	 * Convert text-based option string to Object
	 * @param {string} optionText text-based option string
	 * @return {Object}
	 * @static
	 * @memberOf TemplateUtils
	 */
	TemplateUtils.optionToObject = function(optionText) {
		if(optionText === null || optionText === undefined) {
			return {};
		}
		
		var o = optionText.split(/[= ]/);
		if(o[0] === '') {
			o.shift();
		}
		
		var obj = {};
		for(var i=0, l=o.length; i<l; i+=2) {
			obj[o[i]] = o[i+1];
		}
		
		return obj;
	};
	
	
	/**
	 * Create a new text node.
	 * @name TextNode
	 * @class All text (block {..}) between control's block "{#..}".
	 * @param {string} val text string
	 * @param {boolean} literalMode When enable (true) template does not process blocks {..}.
	 * @param {Template} Template object
	 * @augments BaseNode
	 */
	var TextNode = function(val, literalMode, template) {
		this._value = val;
		this._literalMode = literalMode;
		this._template = template;
	};
	
	/**
	 * Get the html string for a text node.
	 * @param {object} d data
	 * @param {object} param parameters
	 * @param {Element} element a HTML element
	 * @param {Number} deep
	 * @return {String}
	 */
	TextNode.prototype.get = function(d, param, element, deep) {
		var __t = this._value;
		if(!this._literalMode) {
			var __template = this._template;
			var $T = d;
			var $P = param;
			var $Q = element;
			__t = __t.replace(/\{(.*?)\}/g, function(__0, __1) {
				try {
					var __tmp = eval(__1);
					if(typeof __tmp == 'function') {
						if(__template.settings.disallow_functions || !__template.settings.runnable_functions) {
							return '';
						} else {
							__tmp = __tmp($T, $P, $Q);
						}
					}
					return (__tmp === undefined) ? ("") : (String(__tmp));
				} catch(e) {
					if(Template.DEBUG_MODE) {
						if(e instanceof JTException)
							e.type = "subtemplate";
						throw e;
					}
					return "";
				}
			});
		}
		return __t;
	};
	
	/**
	 * Create a new conditional node.
	 * @name opIF
	 * @class A class represent: {#if}.
	 * @param {string} oper content of operator {#..}
	 * @param {object} par parent node
	 * @augments BaseArray
	 */
	var opIF = function(oper, par) {
		this._parent = par;
		oper.match(/\{#(?:else)*if (.*?)\}/);
		this._cond = RegExp.$1;
		this._onTrue = [];
		this._onFalse = [];
		this._currentState = this._onTrue;
	};
	
	/**
	 * Add node 'e' to array.
	 * @param {BaseNode} e a node
	 */
	opIF.prototype.push = function(e) {
		this._currentState.push(e);
	};
	
	/**
	 * Get a parent node.
	 * @return {BaseNode}
	 */
	opIF.prototype.getParent = function() {
		return this._parent;
	};
	
	/**
	 * Switch from collection onTrue to onFalse.
	 */
	opIF.prototype.switchToElse = function() {
		this._currentState = this._onFalse;
	};
	
	/**
	 * Process node depend on conditional and get the html string.
	 * @param {object} d data
	 * @param {object} param parameters
	 * @param {Element} element a HTML element
	 * @param {Number} deep
	 * @return {String}
	 */
	opIF.prototype.get = function(d, param, element, deep) {
		var $T = d;
		var $P = param;
		var $Q = element;
		var ret = '';
		
		try {
			var tab = (eval(this._cond)) ? (this._onTrue) : (this._onFalse);
			for(var i=0, l=tab.length; i<l; ++i) {
				ret += tab[i].get(d, param, element, deep);
			}
		} catch(e) {
			if(Template.DEBUG_MODE || (e instanceof JTException))
				throw e;
		}
		return ret;
	};
	
	/**
	 * Handler for a tag 'FOR'. Create new and return relative opFOREACH object.
	 * @name opFORFactory
	 * @class Handler for a tag 'FOR'. Create new and return relative opFOREACH object.
	 * @param {string} oper content of operator {#..}
	 * @param {object} par parent node
	 * @param {Template} template a pointer to Template object
	 * @return {opFOREACH}
	 */
	opFORFactory = function(oper, par, template) {
		if(oper.match(/\{#for (\w+?) *= *(\S+?) +to +(\S+?) *(?:step=(\S+?))*\}/)) {
			oper = '{#foreach opFORFactory.funcIterator as ' + RegExp.$1 + ' begin=' + (RegExp.$2 || 0) + ' end=' + (RegExp.$3 || -1) + ' step=' + (RegExp.$4 || 1) + ' extData=$T}';
			return new opFOREACH(oper, par, template);
		} else {
			throw new Error('jTemplates: Operator failed "find": ' + oper);
		}
	};
	
	/**
	 * Function returns inputs data (using internal with opFORFactory)
	 * @param {object} i any data
	 * @return {object} any data (equal parameter 'i')
	 * @private
	 * @static
	 */
	opFORFactory.funcIterator = function(i) {
		return i;
	};
	
	/**
	 * Create a new loop node.
	 * @name opFOREACH
	 * @class A class represent: {#foreach}.
	 * @param {string} oper content of operator {#..}
	 * @param {object} par parent node
	 * @param {Template} template a pointer to Template object
	 * @augments BaseArray
	 */
	var opFOREACH = function(oper, par, template) {
		this._parent = par;
		this._template = template;
		oper.match(/\{#foreach (.+?) as (\w+?)( .+)*\}/);
		this._arg = RegExp.$1;
		this._name = RegExp.$2;
		this._option = RegExp.$3 || null;
		this._option = TemplateUtils.optionToObject(this._option);
		
		this._onTrue = [];
		this._onFalse = [];
		this._currentState = this._onTrue;
	};
	
	/**
	 * Add node 'e' to array.
	 * @param {BaseNode} e
	 */
	opFOREACH.prototype.push = function(e) {
		this._currentState.push(e);
	};
	
	/**
	 * Get a parent node.
	 * @return {BaseNode}
	 */
	opFOREACH.prototype.getParent = function() {
		return this._parent;
	};
	
	/**
	 * Switch from collection onTrue to onFalse.
	 */
	opFOREACH.prototype.switchToElse = function() {
		this._currentState = this._onFalse;
	};
	
	/**
	 * Process loop and get the html string.
	 * @param {object} d data
	 * @param {object} param parameters
	 * @param {Element} element a HTML element
	 * @param {Number} deep
	 * @return {String}
	 */
	opFOREACH.prototype.get = function(d, param, element, deep) {
		try {
			var $T = d;
			var $P = param;
			var $Q = element;
			var fcount = eval(this._arg);	//array of elements in foreach
			var key = [];	//only for objects
			var mode = typeof fcount;
			if(mode == 'object') {
				var arr = [];
				jQuery.each(fcount, function(k, v) {
					key.push(k);
					arr.push(v);
				});
				fcount = arr;
			}
			var extData = (this._option.extData !== undefined) ? (eval(this._option.extData)) : (($T != null) ? ($T) : ({}));
			var s = Number(eval(this._option.begin) || 0), e;	//start, end
			var step = Number(eval(this._option.step) || 1);
			if(mode != 'function') {
				e = fcount.length;
			} else {
				if(this._option.end === undefined || this._option.end === null) {
					e = Number.MAX_VALUE;
				} else {
					e = Number(eval(this._option.end)) + ((step>0) ? (1) : (-1));
				}
			}
			var ret = '';	//returned string
			var i,l;	//iterators
			
			if(this._option.count) {
				var tmp = s + Number(eval(this._option.count));
				e = (tmp > e) ? (e) : (tmp);
			}
			if((e>s && step>0) || (e<s && step<0)) {
				var iteration = 0;
				var _total = (mode != 'function') ? (Math.ceil((e-s)/step)) : undefined;
				var ckey, cval;	//current key, current value
				for(; ((step>0) ? (s<e) : (s>e)); s+=step, ++iteration) {
					ckey = key[s];
					if(mode != 'function') {
						cval = fcount[s];
					} else {
						cval = fcount(s);
						if(cval === undefined || cval === null) {
							break;
						}
					}
					if((typeof cval == 'function') && (this._template.settings.disallow_functions || !this._template.settings.runnable_functions)) {
						continue;
					}
					if((mode == 'object') && (ckey in Object)) {
						continue;
					}
					var prevValue = extData[this._name];
					extData[this._name] = cval;
					extData[this._name + '$index'] = s;
					extData[this._name + '$iteration'] = iteration;
					extData[this._name + '$first'] = (iteration==0);
					extData[this._name + '$last'] = (s+step>=e);
					extData[this._name + '$total'] = _total;
					extData[this._name + '$key'] = (ckey !== undefined && ckey.constructor == String) ? (this._template.f_escapeString(ckey)) : (ckey);
					extData[this._name + '$typeof'] = typeof cval;
					for(i=0, l=this._onTrue.length; i<l; ++i) {
						try {
							ret += this._onTrue[i].get(extData, param, element, deep);
						} catch(ex) {
							if(ex instanceof JTException) {
								switch(ex.type) {
									case 'continue':
										i = l;
										break;
									case 'break':
										i = l;
										s = e;
										break;
									default:
										throw e;
								}
							} else {
							  throw e;
							}
						}
					}
					delete extData[this._name + '$index'];
					delete extData[this._name + '$iteration'];
					delete extData[this._name + '$first'];
					delete extData[this._name + '$last'];
					delete extData[this._name + '$total'];
					delete extData[this._name + '$key'];
					delete extData[this._name + '$typeof'];
					delete extData[this._name];
					extData[this._name] = prevValue;
				}
			} else {
				for(i=0, l=this._onFalse.length; i<l; ++i) {
					ret += this._onFalse[i].get($T, param, element, deep);
				}
			}
			return ret;
		} catch(e) {
			if(Template.DEBUG_MODE || (e instanceof JTException))
				throw e;
			return "";
		}
	};
	
	/**
	 * Template-control exceptions
	 * @name JTException
	 * @class A class used internals for a template-control exceptions
	 * @param type {string} Type of exception
	 * @augments Error
	 * @augments BaseNode
	 */
	var JTException = function(type) {
		this.type = type;
	};
	JTException.prototype = Error;
	
	/**
	 * Throw a template-control exception
	 * @throws It throws itself
	 */
	JTException.prototype.get = function(d) {
		throw this;
	};
	
	/**
	 * Create a new entry for included template.
	 * @name Include
	 * @class A class represent: {#include}.
	 * @param {string} oper content of operator {#..}
	 * @param {array} includes
	 * @augments BaseNode
	 */
	var Include = function(oper, includes) {
		oper.match(/\{#include (.*?)(?: root=(.*?))?\}/);
		this._template = includes[RegExp.$1];
		if(this._template == undefined) {
			if(Template.DEBUG_MODE)
				throw new Error('jTemplates: Cannot find include: ' + RegExp.$1);
		}
		this._root = RegExp.$2;
	};
	
	/**
	 * Run method get on included template.
	 * @param {object} d data
	 * @param {object} param parameters
	 * @param {Element} element a HTML element
	 * @param {Number} deep
	 * @return {String}
	 */
	Include.prototype.get = function(d, param, element, deep) {
		var $T = d;
		var $P = param;
		try {
			return this._template.get(eval(this._root), param, element, deep);
		} catch(e) {
			if(Template.DEBUG_MODE || (e instanceof JTException))
				throw e;
		}
		return '';
	};
	
	/**
	 * Create new node for {#param}.
	 * @name UserParam
	 * @class A class represent: {#param}.
	 * @param {string} oper content of operator {#..}
	 * @augments BaseNode
	 */
	var UserParam = function(oper) {
		oper.match(/\{#param name=(\w*?) value=(.*?)\}/);
		this._name = RegExp.$1;
		this._value = RegExp.$2;
	};
	
	/**
	 * Return value of selected parameter.
	 * @param {object} d data
	 * @param {object} param parameters
	 * @param {Element} element a HTML element
	 * @param {Number} deep
	 * @return {String} empty string
	 */
	UserParam.prototype.get = function(d, param, element, deep) {
		var $T = d;
		var $P = param;
		var $Q = element;
		
		try {
			param[this._name] = eval(this._value);
		} catch(e) {
			if(Template.DEBUG_MODE || (e instanceof JTException))
				throw e;
			param[this._name] = undefined;
		}
		return '';
	};
	
	/**
	 * Create a new cycle node.
	 * @name Cycle
	 * @class A class represent: {#cycle}.
	 * @param {string} oper content of operator {#..}
	 * @augments BaseNode
	 */
	var Cycle = function(oper) {
		oper.match(/\{#cycle values=(.*?)\}/);
		this._values = eval(RegExp.$1);
		this._length = this._values.length;
		if(this._length <= 0) {
			throw new Error('jTemplates: cycle has no elements');
		}
		this._index = 0;
		this._lastSessionID = -1;
	};

	/**
	 * Do a step on cycle and return value.
	 * @param {object} d data
	 * @param {object} param parameters
	 * @param {Element} element a HTML element
	 * @param {Number} deep
	 * @return {String}
	 */
	Cycle.prototype.get = function(d, param, element, deep) {
		var sid = jQuery.data(element, 'jTemplateSID');
		if(sid != this._lastSessionID) {
			this._lastSessionID = sid;
			this._index = 0;
		}
		var i = this._index++ % this._length;
		return this._values[i];
	};
	
	
	/**
	 * Add a Template to HTML Elements.
	 * @param {Template/string} s a Template or a template string
	 * @param {array} [includes] Array of included templates.
	 * @param {object} [settings] Settings (see Template)
	 * @return {jQuery} chainable jQuery class
	 * @memberOf jQuery.fn
	 */
	jQuery.fn.setTemplate = function(s, includes, settings) {
		if(s.constructor === Template) {
			return jQuery(this).each(function() {
				jQuery.data(this, 'jTemplate', s);
				jQuery.data(this, 'jTemplateSID', 0);
			});
		} else {
			return jQuery(this).each(function() {
				jQuery.data(this, 'jTemplate', new Template(s, includes, settings));
				jQuery.data(this, 'jTemplateSID', 0);
			});
		}
	};
	
	/**
	 * Add a Template (from URL) to HTML Elements.
	 * @param {string} url_ URL to template
	 * @param {array} [includes] Array of included templates.
	 * @param {object} [settings] Settings (see Template)
	 * @return {jQuery} chainable jQuery class
	 * @memberOf jQuery.fn
	 */
	jQuery.fn.setTemplateURL = function(url_, includes, settings) {
		var s = jQuery.ajax({
			url: url_,
			async: false
		}).responseText;
		
		return jQuery(this).setTemplate(s, includes, settings);
	};
	
	/**
	 * Create a Template from element's content.
	 * @param {string} elementName an ID of element
	 * @param {array} [includes] Array of included templates.
	 * @param {object} [settings] Settings (see Template)
	 * @return {jQuery} chainable jQuery class
	 * @memberOf jQuery.fn
	 */
	jQuery.fn.setTemplateElement = function(elementName, includes, settings) {
		var s = jQuery('#' + elementName).val();
		if(s == null) {
			s = jQuery('#' + elementName).html();
			s = s.replace(/&lt;/g, "<").replace(/&gt;/g, ">");
		}
		
		s = jQuery.trim(s);
		s = s.replace(/^<\!\[CDATA\[([\s\S]*)\]\]>$/im, '$1');
		s = s.replace(/^<\!--([\s\S]*)-->$/im, '$1');
		
		return jQuery(this).setTemplate(s, includes, settings);
	};
	
	/**
	 * Check it HTML Elements have a template. Return count of templates.
	 * @return {number} Number of templates.
	 * @memberOf jQuery.fn
	 */
	jQuery.fn.hasTemplate = function() {
		var count = 0;
		jQuery(this).each(function() {
			if(jQuery.getTemplate(this)) {
				++count;
			}
		});
		return count;
	};
	
	/**
	 * Remote Template from HTML Element(s)
	 * @return {jQuery} chainable jQuery class
	 */
	jQuery.fn.removeTemplate = function() {
		jQuery(this).processTemplateStop();
		return jQuery(this).each(function() {
			jQuery.removeData(this, 'jTemplate');
		});
	};
	
	/**
	 * Set to parameter 'name' value 'value'.
	 * @param {string} name
	 * @param {object} value
	 * @return {jQuery} chainable jQuery class
	 * @memberOf jQuery.fn
	 */
	jQuery.fn.setParam = function(name, value) {
		return jQuery(this).each(function() {
			var t = jQuery.getTemplate(this);
			if(t === undefined) {
				if(Template.DEBUG_MODE)
					throw new Error('jTemplates: Template is not defined.');
				else
					return;
			}
			t.setParam(name, value); 
		});
	};
	
	/**
	 * Process template using data 'd' and parameters 'param'. Update HTML code.
	 * @param {object} d data 
	 * @param {object} [param] parameters
	 * @return {jQuery} chainable jQuery class
	 * @memberOf jQuery.fn
	 */
	jQuery.fn.processTemplate = function(d, param) {
		return jQuery(this).each(function() {
			var t = jQuery.getTemplate(this);
			if(t === undefined) {
				if(Template.DEBUG_MODE)
					throw new Error('jTemplates: Template is not defined.');
				else
					return;
			}
			jQuery.data(this, 'jTemplateSID', jQuery.data(this, 'jTemplateSID') + 1);
			jQuery(this).html(t.get(d, param, this, 0));
		});
	};
	
	/**
	 * Process template using data from URL 'url_' (only format JSON) and parameters 'param'. Update HTML code.
	 * @param {string} url_ URL to data (in JSON)
	 * @param {object} [param] parameters
	 * @param {object} options options and callbacks
	 * @return {jQuery} chainable jQuery class
	 * @memberOf jQuery.fn
	 */
	jQuery.fn.processTemplateURL = function(url_, param, options) {
		var that = this;
		
		options = jQuery.extend({
			type: 'GET',
			async: true,
			cache: false
		}, options);
		
		jQuery.ajax({
			url: url_,
			type: options.type,
			data: options.data,
			dataFilter: options.dataFilter,
			async: options.async,
			cache: options.cache,
			timeout: options.timeout,
			dataType: 'json',
			success: function(d) {
				var r = jQuery(that).processTemplate(d, param);
				if(options.on_success) {
					options.on_success(r);
				}
			},
			error: options.on_error,
			complete: options.on_complete
		});
		return this;
	};

//#####>UPDATER
	/**
	 * Create new Updater.
	 * @name Updater
	 * @class This class is used for 'Live Refresh!'.
	 * @param {string} url A destination URL
	 * @param {object} param Parameters (for template)
	 * @param {number} interval Time refresh interval
	 * @param {object} args Additional URL parameters (in URL alter ?) as assoc array.
	 * @param {array} objs An array of HTMLElement which will be modified by Updater.
	 * @param {object} options options and callbacks
	 */
	var Updater = function(url, param, interval, args, objs, options) {
		this._url = url;
		this._param = param;
		this._interval = interval;
		this._args = args;
		this.objs = objs;
		this.timer = null;
		this._options = options || {};
		
		var that = this;
		jQuery(objs).each(function() {
			jQuery.data(this, 'jTemplateUpdater', that);
		});
		this.run();
	};
	
	/**
	 * Create new HTTP request to server, get data (as JSON) and send it to templates. Also check does HTMLElements still exists in Document.
	 */
	Updater.prototype.run = function() {
		this.detectDeletedNodes();
		if(this.objs.length == 0) {
			return;
		}
		var that = this;
		jQuery.getJSON(this._url, this._args, function(d) {
		  var r = jQuery(that.objs).processTemplate(d, that._param);
			if(that._options.on_success) {
				that._options.on_success(r);
			}
		});
		this.timer = setTimeout(function(){that.run();}, this._interval);
	};
	
	/**
	 * Check does HTMLElements still exists in HTML Document.
	 * If not exist, delete it from property 'objs'.
	 */
	Updater.prototype.detectDeletedNodes = function() {
		this.objs = jQuery.grep(this.objs, function(o) {
			if(jQuery.browser.msie) {
				var n = o.parentNode;
				while(n && n != document) {
					n = n.parentNode;
				}
				return n != null;
			} else {
				return o.parentNode != null;
			}
		});
	};
	
	/**
	 * Start 'Live Refresh!'.
	 * @param {string} url A destination URL
	 * @param {object} param Parameters (for template)
	 * @param {number} interval Time refresh interval
	 * @param {object} args Additional URL parameters (in URL alter ?) as assoc array.
	 * @param {object} options options and callbacks
	 * @return {Updater} an Updater object
	 * @memberOf jQuery.fn
	 */
	jQuery.fn.processTemplateStart = function(url, param, interval, args, options) {
		return new Updater(url, param, interval, args, this, options);
	};
	
	/**
	 * Stop 'Live Refresh!'.
	 * @return {jQuery} chainable jQuery class
	 * @memberOf jQuery.fn
	 */
	jQuery.fn.processTemplateStop = function() {
		return jQuery(this).each(function() {
			var updater = jQuery.data(this, 'jTemplateUpdater');
			if(updater == null) {
				return;
			}
			var that = this;
			updater.objs = jQuery.grep(updater.objs, function(o) {
				return o != that;
			});
			jQuery.removeData(this, 'jTemplateUpdater');
		});
	};
//#####<UPDATER
	
	jQuery.extend(/** @scope jQuery.prototype */{
		/**
		 * Create new Template.
		 * @param {string} s A template string (like: "Text: {$T.txt}.").
		 * @param {array} includes Array of included templates.
		 * @param {object} settings Settings. (see Template)
		 * @return {Template}
		 */
		createTemplate: function(s, includes, settings) {
			return new Template(s, includes, settings);
		},
		
		/**
		 * Create new Template from URL.
		 * @param {string} url_ URL to template
		 * @param {array} includes Array of included templates.
		 * @param {object} settings Settings. (see Template)
		 * @return {Template}
		 */
		createTemplateURL: function(url_, includes, settings) {
			var s = jQuery.ajax({
				url: url_,
				async: false
			}).responseText;
			
			return new Template(s, includes, settings);
		},
		
		/**
		 * Get a Template for HTML node
		 * @param {Element} HTML node
		 * @return {Template} a Template or "undefined"
		 */
		getTemplate: function(element) {
			return jQuery.data(element, 'jTemplate');
		},
		
		/**
		 * Process template and return text content.
		 * @param {Template} template A Template
		 * @param {object} data data
		 * @param {object} param parameters
		 * @return {string} Content of template
		 */
		processTemplateToText: function(template, data, parameter) {
			return template.get(data, parameter, undefined, 0);
		},
		
		/**
		 * Set Debug Mode
		 * @param {Boolean} value
		 */
		jTemplatesDebugMode: function(value) {
			Template.DEBUG_MODE = value;
		}
	});
	
})(jQuery);}

