/*
 * Copyright (c) 2010-2018 "Neo Technology,"
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
(function(){var s=this;
var q=s._;
var b={};
var i=Array.prototype,A=Object.prototype;
var r=i.slice,v=i.unshift,u=A.toString,n=A.hasOwnProperty;
var l=i.forEach,h=i.map,y=i.reduce,e=i.reduceRight,k=i.filter,a=i.every,x=i.some,t=i.indexOf,f=i.lastIndexOf,c=Array.isArray,z=Object.keys;
var B=function(C){return new g(C)
};
if(typeof module!=="undefined"&&module.exports){module.exports=B;
B._=B
}else{s._=B
}B.VERSION="1.1.4";
var d=B.each=B.forEach=function(I,G,F){var H;
if(I==null){return
}if(l&&I.forEach===l){I.forEach(G,F)
}else{if(B.isNumber(I.length)){for(var E=0,C=I.length;
E<C;
E++){if(G.call(F,I[E],E,I)===b){return
}}}else{for(var D in I){if(n.call(I,D)){if(G.call(F,I[D],D,I)===b){return
}}}}}};
B.map=function(F,E,D){var C=[];
if(F==null){return C
}if(h&&F.map===h){return F.map(E,D)
}d(F,function(I,G,H){C[C.length]=E.call(D,I,G,H)
});
return C
};
B.reduce=B.foldl=B.inject=function(G,F,C,E){var D=C!==void 0;
if(G==null){G=[]
}if(y&&G.reduce===y){if(E){F=B.bind(F,E)
}return D?G.reduce(F,C):G.reduce(F)
}d(G,function(J,H,I){if(!D&&H===0){C=J;
D=true
}else{C=F.call(E,C,J,H,I)
}});
if(!D){throw new TypeError("Reduce of empty array with no initial value")
}return C
};
B.reduceRight=B.foldr=function(F,E,C,D){if(F==null){F=[]
}if(e&&F.reduceRight===e){if(D){E=B.bind(E,D)
}return C!==void 0?F.reduceRight(E,C):F.reduceRight(E)
}var G=(B.isArray(F)?F.slice():B.toArray(F)).reverse();
return B.reduce(G,E,C,D)
};
B.find=B.detect=function(F,E,D){var C;
o(F,function(I,G,H){if(E.call(D,I,G,H)){C=I;
return true
}});
return C
};
B.filter=B.select=function(F,E,D){var C=[];
if(F==null){return C
}if(k&&F.filter===k){return F.filter(E,D)
}d(F,function(I,G,H){if(E.call(D,I,G,H)){C[C.length]=I
}});
return C
};
B.reject=function(F,E,D){var C=[];
if(F==null){return C
}d(F,function(I,G,H){if(!E.call(D,I,G,H)){C[C.length]=I
}});
return C
};
B.every=B.all=function(F,E,D){E=E||B.identity;
var C=true;
if(F==null){return C
}if(a&&F.every===a){return F.every(E,D)
}d(F,function(I,G,H){if(!(C=C&&E.call(D,I,G,H))){return b
}});
return C
};
var o=B.some=B.any=function(F,E,D){E=E||B.identity;
var C=false;
if(F==null){return C
}if(x&&F.some===x){return F.some(E,D)
}d(F,function(I,G,H){if(C=E.call(D,I,G,H)){return b
}});
return C
};
B.include=B.contains=function(E,D){var C=false;
if(E==null){return C
}if(t&&E.indexOf===t){return E.indexOf(D)!=-1
}o(E,function(F){if(C=F===D){return true
}});
return C
};
B.invoke=function(D,E){var C=r.call(arguments,2);
return B.map(D,function(F){return(E?F[E]:F).apply(F,C)
})
};
B.pluck=function(D,C){return B.map(D,function(E){return E[C]
})
};
B.max=function(F,E,D){if(!E&&B.isArray(F)){return Math.max.apply(Math,F)
}var C={computed:-Infinity};
d(F,function(J,G,I){var H=E?E.call(D,J,G,I):J;
H>=C.computed&&(C={value:J,computed:H})
});
return C.value
};
B.min=function(F,E,D){if(!E&&B.isArray(F)){return Math.min.apply(Math,F)
}var C={computed:Infinity};
d(F,function(J,G,I){var H=E?E.call(D,J,G,I):J;
H<C.computed&&(C={value:J,computed:H})
});
return C.value
};
B.sortBy=function(E,D,C){return B.pluck(B.map(E,function(H,F,G){return{value:H,criteria:D.call(C,H,F,G)}
}).sort(function(I,H){var G=I.criteria,F=H.criteria;
return G<F?-1:G>F?1:0
}),"value")
};
B.sortedIndex=function(H,G,E){E=E||B.identity;
var C=0,F=H.length;
while(C<F){var D=(C+F)>>1;
E(H[D])<E(G)?C=D+1:F=D
}return C
};
B.toArray=function(C){if(!C){return[]
}if(C.toArray){return C.toArray()
}if(B.isArray(C)){return C
}if(B.isArguments(C)){return r.call(C)
}return B.values(C)
};
B.size=function(C){return B.toArray(C).length
};
B.first=B.head=function(E,D,C){return D&&!C?r.call(E,0,D):E[0]
};
B.rest=B.tail=function(E,C,D){return r.call(E,B.isUndefined(C)||D?1:C)
};
B.last=function(C){return C[C.length-1]
};
B.compact=function(C){return B.filter(C,function(D){return !!D
})
};
B.flatten=function(C){return B.reduce(C,function(D,E){if(B.isArray(E)){return D.concat(B.flatten(E))
}D[D.length]=E;
return D
},[])
};
B.without=function(D){var C=r.call(arguments,1);
return B.filter(D,function(E){return !B.include(C,E)
})
};
B.uniq=B.unique=function(C){return B.reduce(C,function(D,F,E){if(0==E||!B.include(D,F)){D[D.length]=F
}return D
},[])
};
B.intersect=function(D){var C=r.call(arguments,1);
return B.filter(B.uniq(D),function(E){return B.every(C,function(F){return B.indexOf(F,E)>=0
})
})
};
B.zip=function(){var C=r.call(arguments);
var F=B.max(B.pluck(C,"length"));
var E=new Array(F);
for(var D=0;
D<F;
D++){E[D]=B.pluck(C,""+D)
}return E
};
B.indexOf=function(G,E,F){if(G==null){return -1
}if(F){var D=B.sortedIndex(G,E);
return G[D]===E?D:-1
}if(t&&G.indexOf===t){return G.indexOf(E)
}for(var D=0,C=G.length;
D<C;
D++){if(G[D]===E){return D
}}return -1
};
B.lastIndexOf=function(E,D){if(E==null){return -1
}if(f&&E.lastIndexOf===f){return E.lastIndexOf(D)
}var C=E.length;
while(C--){if(E[C]===D){return C
}}return -1
};
B.range=function(J,G,H){var F=r.call(arguments),I=F.length<=1,J=I?0:F[0],G=I?F[0]:F[1],H=F[2]||1,D=Math.max(Math.ceil((G-J)/H),0),C=0,E=new Array(D);
while(C<D){E[C++]=J;
J+=H
}return E
};
B.bind=function(D,E){var C=r.call(arguments,2);
return function(){return D.apply(E||{},C.concat(r.call(arguments)))
}
};
B.bindAll=function(D){var C=r.call(arguments,1);
if(C.length==0){C=B.functions(D)
}d(C,function(E){D[E]=B.bind(D[E],D)
});
return D
};
B.memoize=function(E,D){var C={};
D=D||B.identity;
return function(){var F=D.apply(this,arguments);
return F in C?C[F]:(C[F]=E.apply(this,arguments))
}
};
B.delay=function(D,E){var C=r.call(arguments,2);
return setTimeout(function(){return D.apply(D,C)
},E)
};
B.defer=function(C){return B.delay.apply(B,[C,1].concat(r.call(arguments,1)))
};
var w=function(D,F,C){var E;
return function(){var H=this,G=arguments;
var I=function(){E=null;
D.apply(H,G)
};
if(C){clearTimeout(E)
}if(C||!E){E=setTimeout(I,F)
}}
};
B.throttle=function(C,D){return w(C,D,false)
};
B.debounce=function(C,D){return w(C,D,true)
};
B.wrap=function(C,D){return function(){var E=[C].concat(r.call(arguments));
return D.apply(this,E)
}
};
B.compose=function(){var C=r.call(arguments);
return function(){var D=r.call(arguments);
for(var E=C.length-1;
E>=0;
E--){D=[C[E].apply(this,D)]
}return D[0]
}
};
B.keys=z||function(E){if(B.isArray(E)){return B.range(0,E.length)
}var D=[];
for(var C in E){if(n.call(E,C)){D[D.length]=C
}}return D
};
B.values=function(C){return B.map(C,B.identity)
};
B.functions=B.methods=function(C){return B.filter(B.keys(C),function(D){return B.isFunction(C[D])
}).sort()
};
B.extend=function(C){d(r.call(arguments,1),function(D){for(var E in D){C[E]=D[E]
}});
return C
};
B.clone=function(C){return B.isArray(C)?C.slice():B.extend({},C)
};
B.tap=function(D,C){C(D);
return D
};
B.isEqual=function(D,C){if(D===C){return true
}var G=typeof(D),I=typeof(C);
if(G!=I){return false
}if(D==C){return true
}if((!D&&C)||(D&&!C)){return false
}if(D._chain){D=D._wrapped
}if(C._chain){C=C._wrapped
}if(D.isEqual){return D.isEqual(C)
}if(B.isDate(D)&&B.isDate(C)){return D.getTime()===C.getTime()
}if(B.isNaN(D)&&B.isNaN(C)){return false
}if(B.isRegExp(D)&&B.isRegExp(C)){return D.source===C.source&&D.global===C.global&&D.ignoreCase===C.ignoreCase&&D.multiline===C.multiline
}if(G!=="object"){return false
}if(D.length&&(D.length!==C.length)){return false
}var E=B.keys(D),H=B.keys(C);
if(E.length!=H.length){return false
}for(var F in D){if(!(F in C)||!B.isEqual(D[F],C[F])){return false
}}return true
};
B.isEmpty=function(D){if(B.isArray(D)||B.isString(D)){return D.length===0
}for(var C in D){if(n.call(D,C)){return false
}}return true
};
B.isElement=function(C){return !!(C&&C.nodeType==1)
};
B.isArray=c||function(C){return u.call(C)==="[object Array]"
};
B.isArguments=function(C){return !!(C&&n.call(C,"callee"))
};
B.isFunction=function(C){return !!(C&&C.constructor&&C.call&&C.apply)
};
B.isString=function(C){return !!(C===""||(C&&C.charCodeAt&&C.substr))
};
B.isNumber=function(C){return !!(C===0||(C&&C.toExponential&&C.toFixed))
};
B.isNaN=function(C){return C!==C
};
B.isBoolean=function(C){return C===true||C===false
};
B.isDate=function(C){return !!(C&&C.getTimezoneOffset&&C.setUTCFullYear)
};
B.isRegExp=function(C){return !!(C&&C.test&&C.exec&&(C.ignoreCase||C.ignoreCase===false))
};
B.isNull=function(C){return C===null
};
B.isUndefined=function(C){return C===void 0
};
B.noConflict=function(){s._=q;
return this
};
B.identity=function(C){return C
};
B.times=function(F,E,D){for(var C=0;
C<F;
C++){E.call(D,C)
}};
B.mixin=function(C){d(B.functions(C),function(D){p(D,B[D]=C[D])
})
};
var j=0;
B.uniqueId=function(C){var D=j++;
return C?C+D:D
};
B.templateSettings={evaluate:/<%([\s\S]+?)%>/g,interpolate:/<%=([\s\S]+?)%>/g};
B.template=function(F,E){var G=B.templateSettings;
var C="var __p=[],print=function(){__p.push.apply(__p,arguments);};with(obj||{}){__p.push('"+F.replace(/\\/g,"\\\\").replace(/'/g,"\\'").replace(G.interpolate,function(H,I){return"',"+I.replace(/\\'/g,"'")+",'"
}).replace(G.evaluate||null,function(H,I){return"');"+I.replace(/\\'/g,"'").replace(/[\r\n\t]/g," ")+"__p.push('"
}).replace(/\r/g,"\\r").replace(/\n/g,"\\n").replace(/\t/g,"\\t")+"');}return __p.join('');";
var D=new Function("obj",C);
return E?D(E):D
};
var g=function(C){this._wrapped=C
};
B.prototype=g.prototype;
var m=function(D,C){return C?B(D).chain():D
};
var p=function(C,D){g.prototype[C]=function(){var E=r.call(arguments);
v.call(E,this._wrapped);
return m(D.apply(B,E),this._chain)
}
};
B.mixin(B);
d(["pop","push","reverse","shift","sort","splice","unshift"],function(C){var D=i[C];
g.prototype[C]=function(){D.apply(this._wrapped,arguments);
return m(this._wrapped,this._chain)
}
});
d(["concat","join","slice"],function(C){var D=i[C];
g.prototype[C]=function(){return m(D.apply(this._wrapped,arguments),this._chain)
}
});
g.prototype.chain=function(){this._chain=true;
return this
};
g.prototype.value=function(){return this._wrapped
}
})();
var neo4j=neo4j||{};
neo4j.services=neo4j.services||{};
neo4j.exceptions=neo4j.exceptions||{};
neo4j.exceptions.HttpException=function(a,d,c,b){var b=b||"A server error or a network error occurred. Status code: "+a+".";
this.status=a;
this.data=d||{};
this.req=c||{};
Error.call(this,b)
};
neo4j.exceptions.HttpException.prototype=new Error();
neo4j.exceptions.HttpException.RESPONSE_CODES={Conflict:409,NotFound:404};
(function(){var b=neo4j.exceptions.HttpException.prototype,a=neo4j.exceptions.HttpException.RESPONSE_CODES;
_.each(_.keys(a),function(c){b["is"+c]=function(){return this.status===a[c]
}
})
})();
neo4j.exceptions.ConnectionLostException=function(){neo4j.exceptions.HttpException.call(this,-1,null,null,"The server connection was lost.")
};
neo4j.exceptions.HttpException.prototype=new neo4j.exceptions.HttpException();
neo4j.exceptions.NotFoundException=function(a){Error.call(this,"The object at url "+a+" does not exist.");
this.url=a
};
neo4j.exceptions.NotFoundException.prototype=new Error();
neo4j.exceptions.InvalidDataException=function(){Error.call(this,"Unable to create relationship or node from the provided data. This may be because you tried to get a node or relationship from an invalid url.")
};
neo4j.exceptions.InvalidDataException.prototype=new Error();
neo4j.exceptions.StartNodeSameAsEndNodeException=function(a){Error.call(this,"You cannot create a relationship with the same start and end node.");
this.url=a
};
neo4j.exceptions.StartNodeSameAsEndNodeException.prototype=new Error();
_.extend(neo4j,{setTimeout:function(b,a){if(typeof(setTimeout)!="undefined"){return setTimeout(b,a)
}else{if(a===0){b()
}else{neo4j.log("No timeout implementation found, unable to do timed tasks.")
}}},clearTimeout:function(a){if(typeof(clearTimeout)!="undefined"){clearTimeout(intervalId)
}else{neo4j.log("No timeout implementation found, unable to do timed tasks.")
}},_intervals:{}});
_.extend(neo4j,{setInterval:function(c,a){if(typeof(setInterval)!="undefined"){return setInterval(c,a)
}else{if(typeof(setTimeout)!="undefined"){var d=(new Date()).getTime();
function b(){c();
neo4j._intervals[d]=setTimeout(b,a)
}neo4j._intervals[d]=setTimeout(b,a);
return d
}else{neo4j.log("No timeout or interval implementation found, unable to do timed tasks.")
}}},clearInterval:function(a){if(typeof(clearInterval)!="undefined"){clearInterval(a)
}else{if(typeof(clearTimeout)!="undefined"){clearTimeout(neo4j._intervals[a])
}else{neo4j.log("No timeout or interval implementation found, unable to do timed tasks.")
}}},_intervals:{}});
neo4j.Promise=function(a){_.bindAll(this,"then","fulfill","fail","addHandlers","addFulfilledHandler","addFailedHandler","_callHandlers","_callHandler","_addHandler");
this._handlers=[];
if(typeof(a)==="function"){a(this.fulfill,this.fail)
}};
neo4j.Promise.wrap=function(a){if(a instanceof neo4j.Promise){return a
}else{return neo4j.Promise.fulfilled(a)
}};
neo4j.Promise.fulfilled=function(a){return new neo4j.Promise(function(b){b(a)
})
};
neo4j.Promise.join=function(){var a=_.toArray(arguments);
if(a.length==1){return a[0]
}else{return new neo4j.Promise(function(g,d){var f=[];
function c(h){if(h.length>0){h.shift().addFulfilledHandler(function(i){f.push(i);
c(h)
})
}else{g(f)
}}for(var e=0,b=a.length;
e<b;
e++){a[e].addFailedHandler(d)
}c(a)
})
}};
_.extend(neo4j.Promise.prototype,{then:function(c,a){var b=this;
return new neo4j.Promise(function(e,d){b.addHandlers(function(f){if(c){c(f,e,d)
}else{e(f)
}},function(f){if(typeof(a)==="function"){a(f,e,d)
}else{d(f)
}})
})
},chain:function(b){var a=this;
this.chainedPromise=b;
b.then(null,function(c){a.fail(c)
})
},fulfill:function(a){if(this.chainedPromise){var b=this;
this.chainedPromise.then(function(){b._fulfill(a)
})
}else{this._fulfill(a)
}},fail:function(a){if(!this._complete){this._failedResult=a;
this._fulfilled=false;
this._complete=true;
this._callHandlers()
}},_fulfill:function(a){if(!this._complete){this._fulfilledResult=a;
this._fulfilled=true;
this._complete=true;
this._callHandlers()
}},_callHandlers:function(){_.each(this._handlers,this._callHandler)
},_callHandler:function(a){if(this._fulfilled&&typeof(a.fulfilled)==="function"){a.fulfilled(this._fulfilledResult)
}else{if(typeof(a.failed)==="function"){a.failed(this._failedResult)
}}},addHandlers:function(b,a){b=b||function(){};
a=a||function(){};
this._addHandler({fulfilled:b,failed:a})
},addFulfilledHandler:function(a){this.addHandlers(a)
},addFailedHandler:function(a){this.addHandlers(null,a)
},_addHandler:function(a){if(this._complete){this._callHandler(a)
}else{this._handlers.push(a)
}}});
neo4j.cachedFunction=function(f,h,g){var c=null,b=null,a=false,g=g||false,e=[];
return function d(){var i=arguments[h];
if(a){i.apply(b,c)
}else{e.push(i);
if(e.length===1){arguments[h]=function(){b=this;
c=arguments;
a=true;
for(var j in e){e[j].apply(b,c)
}e=[];
if(g){setTimeout(function(){a=false
},g)
}};
f.apply(this,arguments)
}}}
};
neo4j.log=function(){if(typeof(console)!="undefined"&&typeof(console.log)==="function"){console.log.apply(this,arguments)
}};
neo4j.proxy=function(b,a){return _.bind(b,a)
};
neo4j.Events=function(a){this.uniqueNamespaceCount=0;
this.handlers={};
this.context=a||{}
};
neo4j.Events.prototype.createUniqueNamespace=function(){return"uniq#"+(this.uniqueNamespaceCount++)
};
neo4j.Events.prototype.bind=function(a,b){if(typeof(this.handlers[a])==="undefined"){this.handlers[a]=[]
}this.handlers[a].push(b)
};
neo4j.Events.prototype.trigger=function(b,d){if(typeof(this.handlers[b])!=="undefined"){var d=d||{};
var e=this.handlers[b];
var c=_.extend({key:b,data:d},this.context);
for(var a=0,f=e.length;
a<f;
a++){neo4j.setTimeout((function(g){return function(){try{g(c)
}catch(h){neo4j.log("Event handler for event "+b+" threw exception.",h)
}}
})(e[a]),0)
}}};
neo4j.events=new neo4j.Events();
neo4j.jqueryWebProvider={ajax:function(h){var i=h.timeout||6*60*60*1000,a=h.method,c=h.url,e=h.data,k=h.success,d=h.failure,b=a==="GET";
function g(m,l,n){if(n.status===0){f(n)
}else{k.apply(this,arguments)
}}function f(m){try{if(m.status===200){return k(null)
}}catch(n){}try{if(m.status===0){d(new neo4j.exceptions.ConnectionLostException())
}else{var l=JSON.parse(m.responseText);
d(new neo4j.exceptions.HttpException(m.status,l,m))
}}catch(n){d(new neo4j.exceptions.HttpException(-1,{},m))
}}var j=this.isCrossDomain;
(function(p,m,n,o,l){if(n===null||n==="null"){n=""
}else{if(!b){n=JSON.stringify(n)
}}if(j(m)&&window.XDomainRequest){if(typeof(l)==="function"){l(new neo4j.exceptions.HttpException(-1,null,null,"Cross-domain requests are available in IE, but are not yet implemented in neo4js."))
}}else{$.ajax({url:m,type:p,data:n,timeout:i,cache:false,processData:b,success:g,contentType:"application/json",error:f,dataType:"json"})
}})(a,c,e,k,d)
},isCrossDomain:function(b){if(b){var a=b.indexOf("://");
if(a===-1||a>7){return false
}else{return b.substring(a+3).split("/",1)[0]!==window.location.host
}}else{return false
}}};
neo4j.Web=function(b,a){this.webProvider=b||neo4j.jqueryWebProvider;
this.events=a||neo4j.events
};
_.extend(neo4j.Web.prototype,{get:function(b,c,d,a){return this.ajax("GET",b,c,d,a)
},post:function(b,c,d,a){return this.ajax("POST",b,c,d,a)
},put:function(b,c,d,a){return this.ajax("PUT",b,c,d,a)
},del:function(b,c,d,a){return this.ajax("DELETE",b,c,d,a)
},ajax:function(){var b=this._processAjaxArguments(arguments),a=this;
b.userFail=this.wrapFailureCallback(b.failure);
b.userSuccess=b.success;
return new neo4j.Promise(function(d,c){b.failure=function(){c.call(this,{error:arguments[0],args:arguments});
b.userFail.apply(this,arguments)
};
b.success=function(){d.call(this,{data:arguments[0],args:arguments});
b.userSuccess.apply(this,arguments)
};
try{a.webProvider.ajax(b)
}catch(f){b.failure(f)
}})
},looksLikeUrl:function(a){var b=/(ftp|http|https):\/\/(\w+:{0,1}\w*@)?(\S+)(:[0-9]+)?(\/|\/([\w#!:.?+=&%@!\-\/]))?/;
return b.test(a)
},setWebProvider:function(a){this.webProvider=a
},replace:function(c,b){var a={url:c};
_.each(_.keys(b),function(d){a.url=a.url.replace("{"+d+"}",b[d])
});
return a.url
},wrapFailureCallback:function(a){var b=this.events;
return function(c){if(typeof(c)!="undefined"&&c instanceof neo4j.exceptions.ConnectionLostException){b.trigger("web.connection_lost",_.toArray(arguments));
b.trigger("web.connection.failed",_.toArray(arguments))
}a.apply(this,arguments)
}
},_processAjaxArguments:function(c){var f,b,d,e,a,c=_.toArray(c);
f=c.shift();
b=c.shift();
d=c.length>0&&!_.isFunction(c[0])?c.shift():null;
e=c.length>0?c.shift():null;
a=c.length>0?c.shift():null;
e=_.isFunction(e)?e:function(){};
a=_.isFunction(a)?a:function(){};
return{method:f,url:b,data:d,success:e,failure:a}
}});
neo4j.Service=function(a){this.callsWaiting=[];
this.loadServiceDefinition=neo4j.cachedFunction(this.loadServiceDefinition,0);
this.events=new neo4j.Events();
this.bind=neo4j.proxy(this.events.bind,this.events);
this.trigger=neo4j.proxy(this.events.trigger,this.events);
this.db=a;
this.db.bind("services.loaded",neo4j.proxy(function(){if(!this.initialized){this.setNotAvailable()
}},this))
};
neo4j.Service.resourceFactory=function(d){var f=d.urlArgs||[];
var c=f.length;
var g=d.after?d.after:function(h,i){i(h)
};
var e=d.before?d.before:function(i,h){i.apply(this,h)
};
var b=d.errorHandler?d.errorHandler:function(i,h){i({message:"An error occurred, please see attached error object.",error:h})
};
var a=function(){var h=neo4j.proxy(g,this);
var m=neo4j.proxy(b,this);
if(c>0){var l={};
for(var k=0;
k<c;
k++){l[f[k]]=arguments[k]
}var j=this.db.web.replace(this.resources[d.resource],l)
}else{var j=this.resources[d.resource]
}var n=null;
var o=function(){};
if(arguments.length>c){if(typeof(arguments[arguments.length-1])==="function"){o=arguments[arguments.length-1]
}if((arguments.length-1)>c){n=arguments[arguments.length-2]
}}if(n!==null){this.db.web.ajax(d.method,j,n,function(i){h(i,o)
},function(i){m(o,i)
})
}else{this.db.web.ajax(d.method,j,function(i){h(i,o)
},function(i){m(o,i)
})
}};
return function(){this.serviceMethodPreflight(function(){e.call(this,neo4j.proxy(a,this),arguments)
},arguments)
}
};
_.extend(neo4j.Service.prototype,{initialized:false,available:null,resources:null,handleWaitingCalls:function(){for(var b=0,a=this.callsWaiting.length;
b<a;
b++){try{this.serviceMethodPreflight(this.callsWaiting[b].method,this.callsWaiting[b].args)
}catch(c){neo4j.log(c)
}}},loadServiceDefinition:function(a){this.get("/",neo4j.proxy(function(b){this.resources=b.resources;
this.trigger("service.definition.loaded",b);
a(b)
},this))
},makeAvailable:function(a){this.initialized=true;
this.available=true;
this.url=a;
this.handleWaitingCalls()
},setNotAvailable:function(){this.initialized=true;
this.available=false;
this.handleWaitingCalls()
},get:function(c,b,d,a){this.db.web.get(this.url+c,b,d,a)
},del:function(c,b,d,a){this.db.web.del(this.url+c,b,d,a)
},post:function(c,b,d,a){this.db.web.post(this.url+c,b,d,a)
},put:function(c,b,d,a){this.db.web.put(this.url+c,b,d,a)
},serviceMethodPreflight:function(b,a){if(this.available===false){throw new Error("The service you are accessing is not available for this server.")
}else{if(!this.initialized){this.callsWaiting.push({method:b,args:a});
return
}}a=a||[];
if(this.resources!==null){b.apply(this,a)
}else{this.loadServiceDefinition(neo4j.proxy(function(){b.apply(this,a)
},this))
}}});
neo4j.GraphDatabaseHeartbeat=function(a){this.db=a;
this.monitor=a.manage.monitor;
this.listeners={};
this.idCounter=0;
this.listenerCounter=0;
this.timespan={year:60*60*24*365,month:60*60*24*31,week:60*60*24*7,day:60*60*24,hours:60*60*6,minutes:60*35};
this.startTimestamp=Math.round((new Date()).getTime()/1000)-this.timespan.year;
this.endTimestamp=this.startTimestamp+1;
this.timestamps=[];
this.data={};
this.isPolling=false;
this.processMonitorData=neo4j.proxy(this.processMonitorData,this);
this.beat=neo4j.proxy(this.beat,this);
this.waitForPulse=neo4j.proxy(this.waitForPulse,this);
neo4j.setInterval(this.beat,2000)
};
neo4j.GraphDatabaseHeartbeat.prototype.addListener=function(a){this.listenerCounter++;
this.listeners[this.idCounter++]=a;
return this.idCounter
};
neo4j.GraphDatabaseHeartbeat.prototype.removeListener=function(b){var c=false;
if(typeof(b)==="function"){for(var a in this.listeners){if(this.listeners[a]===b){delete this.listeners[a];
c;
break
}}}else{if(this.listeners[b]){delete this.listeners[b];
c=true
}}if(c){this.listenerCounter--
}};
neo4j.GraphDatabaseHeartbeat.prototype.getCachedData=function(){return{timestamps:this.timestamps,data:this.data,endTimestamp:this.endTimestamp,startTimestamp:this.startTimestamp}
};
neo4j.GraphDatabaseHeartbeat.prototype.beat=function(){if(this.listenerCounter>0&&!this.isPolling&&this.monitor.available){this.isPolling=true;
this.monitor.getDataFrom(this.endTimestamp,this.processMonitorData)
}};
neo4j.GraphDatabaseHeartbeat.prototype.processMonitorData=function(d){this.isPolling=false;
if(d&&!d.error){var a=this.findDataBoundaries(d);
if(a.dataEnd>=0){this.endTimestamp=d.timestamps[a.dataEnd];
var e=d.timestamps.splice(a.dataStart,a.dataEnd-a.dataStart);
this.timestamps=this.timestamps.concat(e);
var c={};
for(var b in d.data){c[b]=d.data[b].splice(a.dataStart,a.dataEnd-a.dataStart);
if(typeof(this.data[b])==="undefined"){this.data[b]=[]
}this.data[b]=this.data[b].concat(c[b])
}var f={server:this.server,newData:{data:c,timestamps:e,end_time:this.endTimestamp,start_time:d.start_time},allData:this.getCachedData()};
this.callListeners(f)
}else{this.adjustRequestedTimespan()
}}};
neo4j.GraphDatabaseHeartbeat.prototype.waitForPulse=function(c){if(!this.pulsePromise){var b=this,a=this.db.get;
this.pulsePromise=new neo4j.Promise(function(e){var d={interval:null};
d.interval=neo4j.setInterval(function(){a("",function(f){if(f!==null){neo4j.clearInterval(d.interval);
b.pulsePromise=null;
e(true)
}})
},4000)
})
}this.pulsePromise.addFulfilledHandler(c);
return this.pulsePromise
};
neo4j.GraphDatabaseHeartbeat.prototype.adjustRequestedTimespan=function(b){var a=Math.round((new Date()).getTime()/1000);
var c=a-this.endTimestamp;
if(c>=this.timespan.year){this.endTimestamp=a-this.timespan.month;
this.beat()
}else{if(c>=this.timespan.month){this.endTimestamp=a-this.timespan.week;
this.beat()
}else{if(c>=this.timespan.week){this.endTimestamp=a-this.timespan.day;
this.beat()
}else{if(c>=this.timespan.day){this.endTimestamp=a-this.timespan.hours;
this.beat()
}else{if(c>=this.timespan.day){this.endTimestamp=a-this.timespan.minutes;
this.beat()
}}}}}};
neo4j.GraphDatabaseHeartbeat.prototype.findDataBoundaries=function(d){var c=this.getFirstKey(d);
var a=-1,b=-1;
if(c){for(a=d.timestamps.length-1;
a>=0;
a--){if(typeof(d.data[c][a])==="number"){break
}}for(b=0;
b<=a;
b++){if(typeof(d.data[c][b])==="number"){break
}}}return{dataStart:b,dataEnd:a}
};
neo4j.GraphDatabaseHeartbeat.prototype.callListeners=function(b){for(var a in this.listeners){setTimeout(function(c){return function(){c(b)
}
}(this.listeners[a]),0)
}};
neo4j.GraphDatabaseHeartbeat.prototype.getFirstKey=function(a){if(typeof(a)==="object"){for(var b in a.data){break
}}return b?b:null
};
neo4j.models=neo4j.models||{};
neo4j.models.JMXBean=function(a){this.parse(a)
};
neo4j.models.JMXBean.prototype.parse=function(a){var b=this.parseName(a.name);
this.domain=b.domain;
delete (b.domain);
this.properties=b;
this.attributes=a.attributes;
this.description=a.description;
this.jmxName=a.name
};
neo4j.models.JMXBean.prototype.getName=function(a){if(this.properties.name){return this.properties.name
}else{for(var b in this.properties){return this.properties[b]
}}return this.jmxName
};
neo4j.models.JMXBean.prototype.parseName=function(d){var g=d.split(":"),c,f,b={};
f=g[0];
g=g[1].split(",");
for(var e=0,a=g.length;
e<a;
e++){c=g[e].split("=");
b[c[0]]=c[1]
}b.domain=f;
return b
};
neo4j.models.JMXBean.prototype.getAttribute=function(b){var d=b.toLowerCase();
for(var c=0,a=this.attributes.length;
c<a;
c++){if(this.attributes[c].name.toLowerCase()===d){return this.attributes[c]
}}return null
};
neo4j.models.PropertyContainer=function(){_.bindAll(this,"getSelf","exists","getProperty","setProperty","getProperties","setProperties");
this._data=this._data||{}
};
_.extend(neo4j.models.PropertyContainer.prototype,{getSelf:function(){return typeof(this._self)!="undefined"?this._self:null
},getId:function(){var a=this.getSelf();
return a==null?null:a.substr(a.lastIndexOf("/")+1)
},exists:function(){return this.getSelf()!==null
},hasProperty:function(a){return a in this._data
},getProperty:function(a){return this._data[a]||null
},setProperty:function(a,b){this._data[a]=b
},getProperties:function(){return this._data
},setProperties:function(a){this._data=_.extend(this._data,a)
},removeProperty:function(a){delete (this._data[a])
}});
neo4j.models.Node=function(a,b){neo4j.models.PropertyContainer.call(this);
this.db=b;
this._init(a);
_.bindAll(this,"save","fetch","getRelationships","_init")
};
neo4j.models.Node.IN="in";
neo4j.models.Node.OUT="out";
neo4j.models.Node.ALL="all";
neo4j.traverse={};
neo4j.traverse.RETURN_NODES="node";
neo4j.traverse.RETURN_RELATIONSHIPS="relationship";
neo4j.traverse.RETURN_PATHS="path";
_.extend(neo4j.models.Node.prototype,neo4j.models.PropertyContainer.prototype,{save:function(){var b=this,a=this.db.web;
if(!this.exists()){return new neo4j.Promise(function(d,c){b.db.getServiceDefinition().then(function(e){a.post(e.node,b._data).then(function(f){b._init(f.data);
d(b)
},c)
},c)
})
}else{return new neo4j.Promise(function(d,c){a.put(b._urls.properties,b.getProperties(),function(){d(b)
},c)
})
}},fetch:function(){var b=this,a=this.db.web;
return new neo4j.Promise(function(d,c){a.get(b._self).then(function(e){if(e.data&&e.data.self){b._init(e.data);
d(b)
}else{c(new neo4j.exceptions.InvalidDataException())
}},function(e){c(new neo4j.exceptions.NotFoundException(b._self))
})
})
},remove:function(){var e=this,b=this.db.web,a=false,d=this.db,c=e.getSelf();
return new neo4j.Promise(function(g,f){b.del(e.getSelf()).then(function(){d.getReferenceNodeUrl().then(function(h){if(h==c){d.forceRediscovery()
}g(true)
},f)
},function(h){if(h.error.isConflict()&&!a){e.getRelationships().then(function(i){_.each(i,function(j){j.remove()
});
a=true;
e.remove().then(function(){g(true)
},f)
},f)
}})
})
},getCreateRelationshipUrl:function(){if(this.exists()){return this._urls.create_relationship
}else{throw new Error("You can't get the create relationship url until you have saved the node!")
}},traverse:function(b,d){d=d||neo4j.traverse.RETURN_NODES;
var c=this.db.web.replace(this._urls.traverse,{returnType:d}),e=this,a;
switch(d){case neo4j.traverse.RETURN_RELATIONSHIPS:a=neo4j.models.Relationship;
break;
case neo4j.traverse.RETURN_PATHS:a=neo4j.models.Path;
break;
default:a=neo4j.models.Node;
break
}return new neo4j.Promise(function(g,f){e.db.web.post(c,b).then(function(h){var i=_.map(h.data,function(j){return new a(j,e.db)
});
g(i)
},f)
})
},getRelationships:function(c,d){var c=c||neo4j.models.Node.ALL,d=d||null,e=this,b;
var a=d?true:false;
if(_.isArray(d)){d=d.join("&")
}switch(c){case neo4j.models.Node.IN:b=a?this._urls.incoming_typed_relationships:this._urls.incoming_relationships;
break;
case neo4j.models.Node.OUT:b=a?this._urls.outgoing_typed_relationships:this._urls.outgoing_relationships;
break;
default:b=a?this._urls.all_typed_relationships:this._urls.all_relationships;
break
}if(a){b=this.db.web.replace(b,{"-list|&|types":d})
}return new neo4j.Promise(function(g,f){e.db.web.get(b).then(function(h){var i=_.map(h.data,function(j){return new neo4j.models.Relationship(j,e.db)
});
g(i)
},f)
})
},_init:function(a){this._self=a.self||null;
this._data=a.data||{};
this._urls={properties:a.properties||"",traverse:a.traverse||"",create_relationship:a.create_relationship||"",all_relationships:a.all_relationships||"",all_typed_relationships:a.all_typed_relationships||"",incoming_relationships:a.incoming_relationships||"",incoming_typed_relationships:a.incoming_typed_relationships||"",outgoing_relationships:a.outgoing_relationships||"",outgoing_typed_relationships:a.outgoing_typed_relationships||""}
}});
neo4j.models.Relationship=function(a,b){neo4j.models.PropertyContainer.call(this);
this.db=b;
this._init(a);
_.bindAll(this,"save","fetch","_init")
};
_.extend(neo4j.models.Relationship.prototype,neo4j.models.PropertyContainer.prototype,{save:function(){var a=this,b=this.db.web;
if(!this.exists()){return this.getStartNode().then(function(f,d,c){var e=b.post(f.getCreateRelationshipUrl(),{to:a._endUrl,type:a.getType(),data:a.getProperties()});
e.then(function(g){a._init(g.data);
d(a)
},function(g){if(g.error&&g.error.data&&g.error.data.exception){var h=g.error.data.exception;
if(h.indexOf("EndNodeNotFoundException")>-1||(h.indexOf("BadInputException")>-1&&h.indexOf(a._endUrl)>-1)){return c(new neo4j.exceptions.NotFoundException(a._endUrl))
}else{if(h.indexOf("StartNodeSameAsEndNodeException")>-1){return c(new neo4j.exceptions.StartNodeSameAsEndNodeException(a._endUrl))
}}}c(g)
})
})
}else{return new neo4j.Promise(function(d,c){b.put(a._urls.properties,a.getProperties()).then(function(){d(a)
},c)
})
}},fetch:function(){var a=this,b=this.db.web;
return new neo4j.Promise(function(d,c){b.get(a._self).then(function(e){if(e.data&&e.data.self&&e.data.start&&e.data.end){a._init(e.data);
d(a)
}else{c(new neo4j.exceptions.InvalidDataException())
}},c)
})
},remove:function(){var a=this,b=this.db.web;
return new neo4j.Promise(function(d,c){b.del(a.getSelf()).then(function(){d(true)
},c)
})
},getType:function(){return this._type||null
},getStartNode:function(){return this._getNode("_startNode","_startUrl")
},getStartNodeUrl:function(){return this._startUrl
},isStartNode:function(a){if(a instanceof neo4j.models.Node){return this._startUrl===a.getSelf()
}else{return this._startUrl===a
}},getEndNode:function(){return this._getNode("_endNode","_endUrl")
},getEndNodeUrl:function(){return this._endUrl
},isEndNode:function(a){if(a instanceof neo4j.models.Node){return this._endUrl===a.getSelf()
}else{return this._endUrl===a
}},getOtherNode:function(a){if(this.isStartNode(a)){return this.getEndNode()
}else{return this.getStartNode()
}},getOtherNodeUrl:function(a){if(this.isStartNode(a)){return this.getEndNodeUrl()
}else{return this.getStartNodeUrl()
}},_getNode:function(b,c){if(typeof(this[b])!="undefined"){return neo4j.Promise.fulfilled(this[b])
}else{var a=this;
return this.db.node(this[c]).then(function(e,d){a[b]=e;
d(e)
})
}},_init:function(a){this._self=a.self||null;
this._data=a.data||{};
this._type=a.type||null;
this._urls={properties:a.properties||""};
if(typeof(a.start)!="undefined"){if(a.start instanceof neo4j.models.Node){this._startNode=a.start;
this._startUrl=a.start.getSelf()
}else{this._startUrl=a.start
}}if(typeof(a.end)!="undefined"){if(a.end instanceof neo4j.models.Node){this._endNode=a.end;
this._endUrl=a.end.getSelf()
}else{this._endUrl=a.end
}}}});
neo4j.models.Path=function(a,b){this.db=b;
this._init(a);
_.bindAll(this,"_init")
};
_.extend(neo4j.models.Path.prototype,{_init:function(a){this._start=a.start;
this._end=a.end;
this._length=a.length;
this._nodeUrls=a.nodes;
this._relationshipUrls=a.relationships
}});
neo4j.cypher=neo4j.cypher||{};
neo4j.cypher.ExecutionEngine=function(a){this.db=a
};
_.extend(neo4j.cypher.ExecutionEngine.prototype,{execute:function(b){var a=this;
return this.db.getServiceDefinition().then(function(e,d,c){a.db.web.post(e.cypher,{query:b},function(f){d(new neo4j.cypher.QueryResult(a.db,f))
},c)
})
}});
neo4j.cypher.ResultRow=function(a,c,b){this.db=a;
this.row=c;
this.columnMap=b;
this.pointer=0
};
_.extend(neo4j.cypher.ResultRow.prototype,{size:function(){return this.row.length
},getByIndex:function(a){return this._convertValue(this.row[a])
},get:function(a){return this.getByIndex(this.columnMap[a])
},next:function(){return this.getByIndex(this.pointer++)
},hasNext:function(){return this.pointer<this.size()
},reset:function(){this.pointer=0
},_convertValue:function(a){if(a===null){return null
}else{if(typeof(a.data)!=="undefined"){if(typeof(a.type)!=="undefined"){return new neo4j.models.Relationship(a,this.db)
}else{if(typeof(a.length)!=="undefined"){return JSON.stringify(a)
}else{return new neo4j.models.Node(a,this.db)
}}}else{return a
}}}});
neo4j.cypher.QueryResult=function(b,a){this.db=b;
this.data=a.data;
this.columns=a.columns;
this.pointer=0;
this.columnMap={};
for(var c=0;
c<this.columns.length;
c++){this.columnMap[this.columns[c]]=c
}};
_.extend(neo4j.cypher.QueryResult.prototype,{size:function(){return this.data.length
},next:function(){return new neo4j.cypher.ResultRow(this.db,this.data[this.pointer++],this.columnMap)
},hasNext:function(){return this.pointer<this.size()
},reset:function(){this.pointer=0
}});
neo4j.services.BackupService=function(a){neo4j.Service.call(this,a)
};
_.extend(neo4j.services.BackupService.prototype,neo4j.Service.prototype);
neo4j.services.BackupService.prototype.triggerManual=neo4j.Service.resourceFactory({resource:"trigger_manual",method:"POST",errorHandler:function(b,a){if(a.exception=="NoBackupFoundationException"){b(false)
}}});
neo4j.services.BackupService.prototype.triggerManualFoundation=neo4j.Service.resourceFactory({resource:"trigger_manual_foundation",method:"POST"});
neo4j.services.BackupService.prototype.getJobs=neo4j.Service.resourceFactory({resource:"jobs",method:"GET"});
neo4j.services.BackupService.prototype.getJob=function(b,a){this.getJobs(function(c){for(var d in c.jobList){if(c.jobList[d].id==b){a(c.jobList[d]);
return
}}a(null)
})
};
neo4j.services.BackupService.prototype.deleteJob=neo4j.Service.resourceFactory({resource:"job",method:"DELETE",urlArgs:["id"]});
neo4j.services.BackupService.prototype.triggerJobFoundation=neo4j.Service.resourceFactory({resource:"trigger_job_foundation",method:"POST",urlArgs:["id"]});
neo4j.services.BackupService.prototype.setJob=neo4j.Service.resourceFactory({resource:"jobs",method:"PUT"});
neo4j.services.ConfigService=function(a){neo4j.Service.call(this,a)
};
_.extend(neo4j.services.ConfigService.prototype,neo4j.Service.prototype);
neo4j.services.ConfigService.prototype.getProperties=neo4j.Service.resourceFactory({resource:"properties",method:"GET",before:function(c,a){var b=a[0];
c(function(f){var e={};
for(var d in f){e[f[d].key]=f[d]
}b(e)
})
}});
neo4j.services.ConfigService.prototype.getProperty=function(a,b){this.getProperties(function(c){for(var d in c){if(d===a){b(c[d]);
return
}}b(null)
})
};
neo4j.services.ConfigService.prototype.setProperties=neo4j.Service.resourceFactory({resource:"properties",method:"POST",before:function(e,a){var c=[];
var d;
for(var b in a[0]){d={key:b,value:a[0][b]};
c.push(d);
this.db.trigger("config.property.set",d)
}e(c,a[1])
}});
neo4j.services.ConfigService.prototype.setProperty=function(a,c,d){var b={};
b[a]=c;
this.setProperties(b,d)
};
neo4j.services.ImportService=function(a){neo4j.Service.call(this,a)
};
_.extend(neo4j.services.ImportService.prototype,neo4j.Service.prototype);
neo4j.services.ImportService.prototype.fromUrl=neo4j.Service.resourceFactory({resource:"import_from_url",method:"POST",before:function(b,a){b({url:a[0]},a[1])
}});
neo4j.services.ImportService.prototype.getUploadUrl=function(a){this.serviceMethodPreflight(function(b){b(this.resources.import_from_file)
},arguments)
};
neo4j.services.ExportService=function(a){neo4j.Service.call(this,a)
};
_.extend(neo4j.services.ExportService.prototype,neo4j.Service.prototype);
neo4j.services.ExportService.prototype.all=neo4j.Service.resourceFactory({resource:"export_all",method:"POST"});
neo4j.services.ConsoleService=function(a){neo4j.Service.call(this,a)
};
_.extend(neo4j.services.ConsoleService.prototype,neo4j.Service.prototype);
neo4j.services.ConsoleService.prototype.exec=neo4j.Service.resourceFactory({resource:"exec",method:"POST",before:function(b,a){b({command:a[0],engine:a[1]},a[2])
}});
neo4j.services.ConsoleService.prototype.availableEngines=function(a){this.serviceMethodPreflight(function(){function c(d){a(d.engines?d.engines:[])
}function b(d){a(d)
}this.get("/",null,c,b)
})
};
neo4j.services.JmxService=function(a){neo4j.Service.call(this,a);
this.kernelInstance=neo4j.cachedFunction(this.kernelInstance,0,2000)
};
_.extend(neo4j.services.JmxService.prototype,neo4j.Service.prototype);
neo4j.services.JmxService.prototype.getDomains=neo4j.Service.resourceFactory({resource:"domains",method:"GET"});
neo4j.services.JmxService.prototype.getDomain=neo4j.Service.resourceFactory({resource:"domain",method:"GET",urlArgs:["domain"],after:function(d,e){var c=[];
for(var b=0,a=d.beans;
b<a;
b++){c.push(new neo4j.models.JMXBean(d.beans[b]))
}d.beans=c;
e(d)
}});
neo4j.services.JmxService.prototype.getBean=neo4j.Service.resourceFactory({resource:"bean",method:"GET",urlArgs:["domain","objectName"],before:function(e,b){if(b[0]==="neo4j"){var c=this,a=b[1],d=b[2];
this.kernelInstance(function(f){var g=["org.neo4j",escape(f+",name="+a),d];
e.apply(this,g)
})
}else{b[0]=escape(b[0]);
b[1]=escape(b[1]);
e.apply(this,b)
}},after:function(a,b){if(a.length>0){b(new neo4j.models.JMXBean(a[0]))
}else{b(null)
}}});
neo4j.services.JmxService.prototype.query=neo4j.Service.resourceFactory({resource:"query",method:"POST",after:function(d,e){var c=[];
for(var b=0,a=d.length;
b<a;
b++){c.push(new neo4j.models.JMXBean(d[b]))
}e(c)
}});
neo4j.services.JmxService.prototype.kernelInstance=function(b){var a=this.db.web;
this.serviceMethodPreflight(function(d){var c=this.resources.kernelquery;
a.get(c,function(f){var e=f?f.split(":")[1].split(",")[0]:null;
d(e)
})
},[b])
};
neo4j.services.LifecycleService=function(a){neo4j.Service.call(this,a)
};
_.extend(neo4j.services.LifecycleService.prototype,neo4j.Service.prototype);
neo4j.services.LifecycleService.prototype.getStatus=neo4j.Service.resourceFactory({resource:"status",method:"GET"});
neo4j.services.LifecycleService.prototype.start=neo4j.Service.resourceFactory({resource:"start",method:"POST"});
neo4j.services.LifecycleService.prototype.stop=neo4j.Service.resourceFactory({resource:"stop",method:"POST"});
neo4j.services.LifecycleService.prototype.restart=neo4j.Service.resourceFactory({resource:"restart",method:"POST"});
neo4j.services.MonitorService=function(a){neo4j.Service.call(this,a)
};
_.extend(neo4j.services.MonitorService.prototype,neo4j.Service.prototype);
neo4j.services.MonitorService.prototype.getData=neo4j.Service.resourceFactory({resource:"latest_data",method:"GET"});
neo4j.services.MonitorService.prototype.getDataFrom=neo4j.Service.resourceFactory({resource:"data_from",method:"GET",urlArgs:["start"]});
neo4j.services.MonitorService.prototype.getDataBetween=neo4j.Service.resourceFactory({resource:"data_period",method:"GET",urlArgs:["start","stop"]});
neo4j.index=neo4j.index||{};
neo4j.index.Index=function(a,b){this.db=a;
this.name=b;
this.config=null;
this.provider="N/A";
_.bindAll(this,"query","exactQuery","index","unindex")
};
_.extend(neo4j.index.Index.prototype,{getUriFor:function(a){return""
},getObjectFor:function(a){return""
},getType:function(){return""
},createObjectFromDefinition:function(a){},getIdFor:function(a){return a.then(function(c,b){b(c.getId())
})
},setConfig:function(a){this.config=a
},configAvailable:function(){return this.config!==null
},getConfig:function(){return this.config
},query:function(b){var a=this;
return this.db.getServiceDefinition().then(function(e,d,c){a.db.web.get(e[a.getType()]+"/"+a.name,{query:b},function(f){var h=[];
for(var j=0,g=f.length;
j<g;
j++){h.push(a.createObjectFromDefinition(f[j]))
}d(h)
},c)
})
},exactQuery:function(b,c){var a=this;
return this.db.getServiceDefinition().then(function(f,e,d){a.db.web.get(f[a.getType()]+"/"+a.name+"/"+b+"/"+c,function(g){var j=[];
for(var k=0,h=g.length;
k<h;
k++){j.push(a.createObjectFromDefinition(g[k]))
}e(j)
},d)
})
},index:function(i,g,f){var j=neo4j.Promise.wrap(i),e=neo4j.Promise.wrap(g),c=this.getUriFor(j),h=this.db.getServiceDefinition(),b=this;
if(typeof(f)==="undefined"){var d=this.getObjectFor(j).then(function(l,k){k(l.getProperty(g))
})
}else{var d=neo4j.Promise.wrap(f)
}var a=neo4j.Promise.join.apply(this,[c,e,d,h]);
return a.then(function(l,o,k){var m=l[0],n=l[1],p=l[2],q=l[3];
b.db.web.post(q[b.getType()]+"/"+b.name+"/"+n+"/"+p,m,function(r){o(true)
},k)
})
},unindex:function(e,d,f){var a=neo4j.Promise.wrap(e),g=this.getIdFor(a),h=this.db.getServiceDefinition(),c=this;
var b=neo4j.Promise.join.apply(this,[g,h]);
return b.then(function(j,l,i){var n=j[0],m=j[1];
var k=m[c.getType()]+"/"+c.name;
if(d){k+="/"+d
}if(f){k+="/"+f
}k+="/"+n;
c.db.web.del(k,function(o){l(true)
},i)
})
}});
neo4j.index.NodeIndex=function(a,b){neo4j.index.Index.call(this,a,b)
};
_.extend(neo4j.index.NodeIndex.prototype,neo4j.index.Index.prototype,{getType:function(){return"node_index"
},getUriFor:function(a){var b=this.db;
return a.then(function(d,c){b.nodeUri(d).then(c)
})
},getObjectFor:function(b){var a=this.db;
return b.then(function(d,c){if(typeof(d.getSelf)!="undefined"){c(d)
}else{a.node(d).then(function(e){c(e)
})
}})
},createObjectFromDefinition:function(a){return new neo4j.models.Node(a,this.db)
}});
neo4j.index.RelationshipIndex=function(a,b){neo4j.index.Index.call(this,a,b)
};
_.extend(neo4j.index.RelationshipIndex.prototype,neo4j.index.Index.prototype,{getType:function(){return"relationship_index"
},getUriFor:function(a){var b=this.db;
return a.then(function(d,c){b.relUri(d).then(c)
})
},getObjectFor:function(b){var a=this.db;
return b.then(function(d,c){if(typeof(d.getSelf)!="undefined"){c(d)
}else{a.rel(d).then(function(e){c(e)
})
}})
},createObjectFromDefinition:function(a){return new neo4j.models.Relationship(a,this.db)
}});
neo4j.index.Indexes=function(a){this.db=a;
this._cache={};
_.bindAll(this,"getNodeIndex","getRelationshipIndex","createNodeIndex","createRelationshipIndex","removeNodeIndex","removeRelationshipIndex")
};
_.extend(neo4j.index.Indexes.prototype,{getAllNodeIndexes:function(){return this._listAllIndexes("node_index")
},getAllRelationshipIndexes:function(){return this._listAllIndexes("relationship_index")
},getNodeIndex:function(a){return this._getOrCreateLocalIndexObject("node_index",a)
},getRelationshipIndex:function(a){return this._getOrCreateLocalIndexObject("relationship_index",a)
},createNodeIndex:function(b,a){return this._createIndex("node_index",b,a)
},createRelationshipIndex:function(b,a){return this._createIndex("relationship_index",b,a)
},removeNodeIndex:function(a){return this._removeIndex("node_index",a)
},removeRelationshipIndex:function(a){return this._removeIndex("relationship_index",a)
},_listAllIndexes:function(c){var a=this.db,b=this;
return this.db.getServiceDefinition().then(function(f,e,d){a.web.get(f[c],function(m){var j=[],h=m===null?[]:_(m).keys();
for(var k=0,g=h.length;
k<g;
k++){j.push(b._getOrCreateLocalIndexObject(c,h[k],m[h[k]]))
}e(j)
},d)
})
},_createIndex:function(e,d,c){var c=c||{provider:"lucene",type:"exact"},a=this.db,b=this;
return this.db.getServiceDefinition().then(function(h,g,f){a.web.post(h[e],{name:d,config:c},function(i){g(b._getOrCreateLocalIndexObject(e,d,c))
},f)
})
},_removeIndex:function(c,b){var a=this.db;
return this.db.getServiceDefinition().then(function(f,e,d){a.web.del(f[c]+"/"+b,e,d)
})
},_getOrCreateLocalIndexObject:function(d,c,b){var b=b||null;
if(typeof(this._cache[d])=="undefined"){this._cache[d]={}
}if(typeof(this._cache[d][c])=="undefined"){if(d==="relationship_index"){var a=new neo4j.index.RelationshipIndex(this.db,c)
}else{var a=new neo4j.index.NodeIndex(this.db,c)
}this._cache[d][c]=a
}if(b!=null){if(b.provider){this._cache[d][c].provider=b.provider;
delete (b.provider)
}if(b.template){delete (b.template)
}this._cache[d][c].setConfig(b)
}return this._cache[d][c]
}});
neo4j.GraphDatabaseManager=function(a){_.bindAll(this,"discoverServices");
this.db=a;
this.backup=new neo4j.services.BackupService(a);
this.config=new neo4j.services.ConfigService(a);
this.importing=new neo4j.services.ImportService(a);
this.exporting=new neo4j.services.ExportService(a);
this.console=new neo4j.services.ConsoleService(a);
this.jmx=new neo4j.services.JmxService(a);
this.lifecycle=new neo4j.services.LifecycleService(a);
this.monitor=new neo4j.services.MonitorService(a);
this.db.getServiceDefinition().then(this.discoverServices)
};
_.extend(neo4j.GraphDatabaseManager.prototype,{servicesLoaded:function(){return(this.services)?true:false
},availableServices:function(){if(this.services){if(!this.serviceNames){this.serviceNames=[];
for(var a in this.services){this.serviceNames.push(a)
}}return this.serviceNames
}else{throw new Error("Service definition has not been loaded yet.")
}},discoverServices:function(){var a=this;
this.db.getDiscoveryDocument().then(function(b){a.db.web.get(b.management,neo4j.proxy(function(d){this.services=d.services;
for(var c in d.services){if(this[c]){this[c].makeAvailable(d.services[c])
}}this.db.trigger("services.loaded")
},a),neo4j.proxy(function(c){neo4j.log("Unable to fetch service descriptions for server "+this.url+". Server management will be unavailable.")
},this))
})
}});
neo4j.GraphDatabase=function(b,a){this.url=b;
this.events=new neo4j.Events({db:this});
this.bind=neo4j.proxy(this.events.bind,this.events);
this.web=a||new neo4j.Web(null,this.events);
this.trigger=neo4j.proxy(this.events.trigger,this.events);
this.index=new neo4j.index.Indexes(this);
this.cypher=new neo4j.cypher.ExecutionEngine(this);
this.manage=new neo4j.GraphDatabaseManager(this);
this.heartbeat=new neo4j.GraphDatabaseHeartbeat(this);
this.rel=this.relationship;
this.referenceNode=this.getReferenceNode;
_.bindAll(this,"getServiceDefinition","getReferenceNode","node","relationship","getReferenceNodeUrl","getAvailableRelationshipTypes","get","put","post","del","forceRediscovery")
};
_.extend(neo4j.GraphDatabase.prototype,{node:function(a){var b=this,c=neo4j.Promise.wrap(a);
return c.then(function(d,f,e){if(typeof(d)==="object"){var h=new neo4j.models.Node({data:d},b);
h.save().then(function(i){f(i)
},e)
}else{var g=b.promiseNodeOrNodeUrl(d);
g.then(function(i){var j=new neo4j.models.Node({self:i},b);
j.fetch().then(function(k){f(k)
},function(){e(new neo4j.exceptions.NotFoundException(i))
})
})
}})
},relationship:function(i,h,e,c){var k=this;
if(typeof(h)=="undefined"){var g=this.promiseRelationshipOrRelationshipUrl(i);
return g.then(function(m,n,l){var o=new neo4j.models.Relationship({self:m},k);
o.fetch().then(function(p){n(p)
},function(){l(new neo4j.exceptions.NotFoundException(m))
})
})
}else{var a=neo4j.Promise.wrap(c||{}),d=neo4j.Promise.wrap(h),b=this.promiseNodeOrNodeUrl(i),f=this.promiseNodeOrNodeUrl(e);
var j=neo4j.Promise.join(b,f,d,a);
return j.then(function(n,m,l){var o=new neo4j.models.Relationship({start:n[0],end:n[1],type:n[2],data:n[3]},k);
o.save().then(function(p){m(p)
},l)
})
}},query:function(a){return this.cypher.execute(a)
},getNodeOrRelationship:function(b){var a=this;
return this.isNodeUrl(b).then(function(e,d,c){if(e){a.node(b).then(function(f){d(f)
},c)
}else{a.rel(b).then(function(f){d(f)
},c)
}})
},getReferenceNode:function(){return this.node(this.getReferenceNodeUrl())
},getAvailableRelationshipTypes:function(){var a=this;
return this.getServiceDefinition().then(function(b,d,c){a.web.get(b.relationship_types,function(e){d(e)
},c)
})
},getReferenceNodeUrl:function(){return this.getServiceDefinition().then(function(c,b,a){if(typeof(c.reference_node)!=="undefined"){b(c.reference_node)
}else{a()
}})
},nodeUri:function(a){if(typeof(a.getSelf)!="undefined"){return neo4j.Promise.wrap(a.getSelf())
}return this.getServiceDefinition().then(function(c,b){if(/^[0-9]+$/i.test(a)){b(c.node+"/"+a)
}else{b(a)
}})
},relUri:function(a){if(typeof(a.getSelf)!="undefined"){return neo4j.Promise.wrap(a.getSelf())
}return this.getDiscoveryDocument().then(function(c,b){if(/^[0-9]+$/i.test(a)){b(c.data+"relationship/"+a)
}else{b(a)
}})
},getServiceDefinition:function(){if(typeof(this._serviceDefinitionPromise)==="undefined"){var a=this;
this._serviceDefinitionPromise=this.getDiscoveryDocument().then(function(c,d,b){a.web.get(c.data,d,b)
})
}return this._serviceDefinitionPromise
},getDiscoveryDocument:function(){if(typeof(this._discoveryDocumentPromise)==="undefined"){var a=this;
this._discoveryDocumentPromise=new neo4j.Promise(function(c,b){a.web.get(a.url,c,b)
})
}return this._discoveryDocumentPromise
},get:function(c,b,d,a){this.web.get(this.url+c,b,d,a)
},del:function(c,b,d,a){this.web.del(this.url+c,b,d,a)
},post:function(c,b,d,a){this.web.post(this.url+c,b,d,a)
},put:function(c,b,d,a){this.web.put(this.url+c,b,d,a)
},stripUrlBase:function(a){if(typeof(a)==="undefined"||a.indexOf("://")==-1){return a
}if(a.indexOf(this.url)===0){return a.substring(this.url.length)
}else{if(a.indexOf(this.manageUrl)===0){return a.substring(this.manageUrl.length)
}else{return a.substring(a.indexOf("/",8))
}}},isNodeUrl:function(a){return this.getServiceDefinition().then(function(c,b){b(a.indexOf(c.node)===0)
})
},promiseNodeOrNodeUrl:function(a){if(typeof(a)==="object"||this.isUrl(a)){return neo4j.Promise.wrap(a)
}else{return this.nodeUri(a)
}},promiseRelationshipOrRelationshipUrl:function(a){if(typeof(a)==="object"||this.isUrl(a)){return neo4j.Promise.wrap(a)
}else{return this.relUri(a)
}},isUrl:function(a){if(typeof(a)==="object"){return false
}a+="";
return a.indexOf("://")!==-1
},toJSONString:function(){return{url:this.url}
},forceRediscovery:function(){delete this._discoveryDocumentPromise;
delete this._serviceDefinitionPromise
}});
