//
//  arbor-tween.js
//  smooth transitions with a realtime clock
//
//  Copyright (c) 2011 Samizdat Drafting Co.
// 
//  Permission is hereby granted, free of charge, to any person
//  obtaining a copy of this software and associated documentation
//  files (the "Software"), to deal in the Software without
//  restriction, including without limitation the rights to use,
//  copy, modify, merge, publish, distribute, sublicense, and/or sell
//  copies of the Software, and to permit persons to whom the
//  Software is furnished to do so, subject to the following
//  conditions:
// 
//  The above copyright notice and this permission notice shall be
//  included in all copies or substantial portions of the Software.
// 
//  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
//  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
//  OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
//  NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
//  HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
//  WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
//  FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
//  OTHER DEALINGS IN THE SOFTWARE.
//

//  Easing Equations in easing.js:
//  Copyright © 2001 Robert Penner. All rights reserved.
//  
//  Open source under the BSD License. Redistribution and use in source
//  and binary forms, with or without modification, are permitted
//  provided that the following conditions are met:
//  
//  Redistributions of source code must retain the above copyright
//  notice, this list of conditions and the following disclaimer.
//  Redistributions in binary form must reproduce the above copyright
//  notice, this list of conditions and the following disclaimer in the
//  documentation and/or other materials provided with the distribution.
//  
//  Neither the name of the author nor the names of contributors may be
//  used to endorse or promote products derived from this software
//  without specific prior written permission.
//  
//  THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
//  "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
//  LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
//  A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
//  OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
//  SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
//  LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
//  DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
//  THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
//  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
//  OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE. 


(function($){

  /*    etc.js */  var trace=function(msg){if(typeof(window)=="undefined"||!window.console){return}var len=arguments.length;var args=[];for(var i=0;i<len;i++){args.push("arguments["+i+"]")}eval("console.log("+args.join(",")+")")};var dirname=function(a){var b=a.replace(/^\/?(.*?)\/?$/,"$1").split("/");b.pop();return"/"+b.join("/")};var basename=function(b){var c=b.replace(/^\/?(.*?)\/?$/,"$1").split("/");var a=c.pop();if(a==""){return null}else{return a}};var _ordinalize_re=/(\d)(?=(\d\d\d)+(?!\d))/g;var ordinalize=function(a){var b=""+a;if(a<11000){b=(""+a).replace(_ordinalize_re,"$1,")}else{if(a<1000000){b=Math.floor(a/1000)+"k"}else{if(a<1000000000){b=(""+Math.floor(a/1000)).replace(_ordinalize_re,"$1,")+"m"}}}return b};var nano=function(a,b){return a.replace(/\{([\w\-\.]*)}/g,function(f,c){var d=c.split("."),e=b[d.shift()];$.each(d,function(){if(e.hasOwnProperty(this)){e=e[this]}else{e=f}});return e})};var objcopy=function(a){if(a===undefined){return undefined}if(a===null){return null}if(a.parentNode){return a}switch(typeof a){case"string":return a.substring(0);break;case"number":return a+0;break;case"boolean":return a===true;break}var b=($.isArray(a))?[]:{};$.each(a,function(d,c){b[d]=objcopy(c)});return b};var objmerge=function(d,b){d=d||{};b=b||{};var c=objcopy(d);for(var a in b){c[a]=b[a]}return c};var objcmp=function(e,c,d){if(!e||!c){return e===c}if(typeof e!=typeof c){return false}if(typeof e!="object"){return e===c}else{if($.isArray(e)){if(!($.isArray(c))){return false}if(e.length!=c.length){return false}}else{var h=[];for(var f in e){if(e.hasOwnProperty(f)){h.push(f)}}var g=[];for(var f in c){if(c.hasOwnProperty(f)){g.push(f)}}if(!d){h.sort();g.sort()}if(h.join(",")!==g.join(",")){return false}}var i=true;$.each(e,function(a){var b=objcmp(e[a],c[a]);i=i&&b;if(!i){return false}});return i}};var objkeys=function(b){var a=[];$.each(b,function(d,c){if(b.hasOwnProperty(d)){a.push(d)}});return a};var objcontains=function(c){if(!c||typeof c!="object"){return false}for(var b=1,a=arguments.length;b<a;b++){if(c.hasOwnProperty(arguments[b])){return true}}return false};var uniq=function(b){var a=b.length;var d={};for(var c=0;c<a;c++){d[b[c]]=true}return objkeys(d)};var arbor_path=function(){var a=$("script").map(function(b){var c=$(this).attr("src");if(!c){return}if(c.match(/arbor[^\/\.]*.js|dev.js/)){return c.match(/.*\//)||"/"}});if(a.length>0){return a[0]}else{return null}};
  /* colors.js */  var Colors=(function(){var f=/#[0-9a-f]{6}/i;var b=/#(..)(..)(..)/;var c=function(h){var g=h.toString(16);return(g.length==2)?g:"0"+g};var a=function(g){return parseInt(g,16)};var d=function(g){if(!g||typeof g!="object"){return false}var h=objkeys(g).sort().join("");if(h=="abgr"){return true}};var e={CSS:{aliceblue:"#f0f8ff",antiquewhite:"#faebd7",aqua:"#00ffff",aquamarine:"#7fffd4",azure:"#f0ffff",beige:"#f5f5dc",bisque:"#ffe4c4",black:"#000000",blanchedalmond:"#ffebcd",blue:"#0000ff",blueviolet:"#8a2be2",brown:"#a52a2a",burlywood:"#deb887",cadetblue:"#5f9ea0",chartreuse:"#7fff00",chocolate:"#d2691e",coral:"#ff7f50",cornflowerblue:"#6495ed",cornsilk:"#fff8dc",crimson:"#dc143c",cyan:"#00ffff",darkblue:"#00008b",darkcyan:"#008b8b",darkgoldenrod:"#b8860b",darkgray:"#a9a9a9",darkgrey:"#a9a9a9",darkgreen:"#006400",darkkhaki:"#bdb76b",darkmagenta:"#8b008b",darkolivegreen:"#556b2f",darkorange:"#ff8c00",darkorchid:"#9932cc",darkred:"#8b0000",darksalmon:"#e9967a",darkseagreen:"#8fbc8f",darkslateblue:"#483d8b",darkslategray:"#2f4f4f",darkslategrey:"#2f4f4f",darkturquoise:"#00ced1",darkviolet:"#9400d3",deeppink:"#ff1493",deepskyblue:"#00bfff",dimgray:"#696969",dimgrey:"#696969",dodgerblue:"#1e90ff",firebrick:"#b22222",floralwhite:"#fffaf0",forestgreen:"#228b22",fuchsia:"#ff00ff",gainsboro:"#dcdcdc",ghostwhite:"#f8f8ff",gold:"#ffd700",goldenrod:"#daa520",gray:"#808080",grey:"#808080",green:"#008000",greenyellow:"#adff2f",honeydew:"#f0fff0",hotpink:"#ff69b4",indianred:"#cd5c5c",indigo:"#4b0082",ivory:"#fffff0",khaki:"#f0e68c",lavender:"#e6e6fa",lavenderblush:"#fff0f5",lawngreen:"#7cfc00",lemonchiffon:"#fffacd",lightblue:"#add8e6",lightcoral:"#f08080",lightcyan:"#e0ffff",lightgoldenrodyellow:"#fafad2",lightgray:"#d3d3d3",lightgrey:"#d3d3d3",lightgreen:"#90ee90",lightpink:"#ffb6c1",lightsalmon:"#ffa07a",lightseagreen:"#20b2aa",lightskyblue:"#87cefa",lightslategray:"#778899",lightslategrey:"#778899",lightsteelblue:"#b0c4de",lightyellow:"#ffffe0",lime:"#00ff00",limegreen:"#32cd32",linen:"#faf0e6",magenta:"#ff00ff",maroon:"#800000",mediumaquamarine:"#66cdaa",mediumblue:"#0000cd",mediumorchid:"#ba55d3",mediumpurple:"#9370d8",mediumseagreen:"#3cb371",mediumslateblue:"#7b68ee",mediumspringgreen:"#00fa9a",mediumturquoise:"#48d1cc",mediumvioletred:"#c71585",midnightblue:"#191970",mintcream:"#f5fffa",mistyrose:"#ffe4e1",moccasin:"#ffe4b5",navajowhite:"#ffdead",navy:"#000080",oldlace:"#fdf5e6",olive:"#808000",olivedrab:"#6b8e23",orange:"#ffa500",orangered:"#ff4500",orchid:"#da70d6",palegoldenrod:"#eee8aa",palegreen:"#98fb98",paleturquoise:"#afeeee",palevioletred:"#d87093",papayawhip:"#ffefd5",peachpuff:"#ffdab9",peru:"#cd853f",pink:"#ffc0cb",plum:"#dda0dd",powderblue:"#b0e0e6",purple:"#800080",red:"#ff0000",rosybrown:"#bc8f8f",royalblue:"#4169e1",saddlebrown:"#8b4513",salmon:"#fa8072",sandybrown:"#f4a460",seagreen:"#2e8b57",seashell:"#fff5ee",sienna:"#a0522d",silver:"#c0c0c0",skyblue:"#87ceeb",slateblue:"#6a5acd",slategray:"#708090",slategrey:"#708090",snow:"#fffafa",springgreen:"#00ff7f",steelblue:"#4682b4",tan:"#d2b48c",teal:"#008080",thistle:"#d8bfd8",tomato:"#ff6347",turquoise:"#40e0d0",violet:"#ee82ee",wheat:"#f5deb3",white:"#ffffff",whitesmoke:"#f5f5f5",yellow:"#ffff00",yellowgreen:"#9acd32"},decode:function(h){var g=arguments.length;for(var l=g-1;l>=0;l--){if(arguments[l]===undefined){g--}}var k=arguments;if(!h){return null}if(g==1&&d(h)){return h}var j=null;if(typeof h=="string"){var o=1;if(g==2){o=k[1]}var n=e.CSS[h.toLowerCase()];if(n!==undefined){h=n}var m=h.match(f);if(m){vals=h.match(b);if(!vals||!vals.length||vals.length!=4){return null}j={r:a(vals[1]),g:a(vals[2]),b:a(vals[3]),a:o}}}else{if(typeof h=="number"){if(g>=3){j={r:k[0],g:k[1],b:k[2],a:1};if(g>=4){j.a*=k[3]}}else{if(g>=1){j={r:k[0],g:k[0],b:k[0],a:1};if(g==2){j.a*=k[1]}}}}}return j},validate:function(g){if(!g||typeof g!="string"){return false}if(e.CSS[g.toLowerCase()]!==undefined){return true}if(g.match(f)){return true}return false},mix:function(h,g,k){var j=e.decode(h);var i=e.decode(g)},blend:function(g,j){j=(j!==undefined)?Math.max(0,Math.min(1,j)):1;var h=e.decode(g);if(!h){return null}if(j==1){return g}var h=g;if(typeof g=="string"){h=e.decode(g)}var i=objcopy(h);i.a*=j;return nano("rgba({r},{g},{b},{a})",i)},encode:function(g){if(!d(g)){g=e.decode(g);if(!d(g)){return null}}if(g.a==1){return nano("#{r}{g}{b}",{r:c(g.r),g:c(g.g),b:c(g.b)})}else{return nano("rgba({r},{g},{b},{a})",g)}}};return e})();
  /* easing.js */  var Easing=(function(){var a={linear:function(f,e,h,g){return h*(f/g)+e},quadin:function(f,e,h,g){return h*(f/=g)*f+e},quadout:function(f,e,h,g){return -h*(f/=g)*(f-2)+e},quadinout:function(f,e,h,g){if((f/=g/2)<1){return h/2*f*f+e}return -h/2*((--f)*(f-2)-1)+e},cubicin:function(f,e,h,g){return h*(f/=g)*f*f+e},cubicout:function(f,e,h,g){return h*((f=f/g-1)*f*f+1)+e},cubicinout:function(f,e,h,g){if((f/=g/2)<1){return h/2*f*f*f+e}return h/2*((f-=2)*f*f+2)+e},quartin:function(f,e,h,g){return h*(f/=g)*f*f*f+e},quartout:function(f,e,h,g){return -h*((f=f/g-1)*f*f*f-1)+e},quartinout:function(f,e,h,g){if((f/=g/2)<1){return h/2*f*f*f*f+e}return -h/2*((f-=2)*f*f*f-2)+e},quintin:function(f,e,h,g){return h*(f/=g)*f*f*f*f+e},quintout:function(f,e,h,g){return h*((f=f/g-1)*f*f*f*f+1)+e},quintinout:function(f,e,h,g){if((f/=g/2)<1){return h/2*f*f*f*f*f+e}return h/2*((f-=2)*f*f*f*f+2)+e},sinein:function(f,e,h,g){return -h*Math.cos(f/g*(Math.PI/2))+h+e},sineout:function(f,e,h,g){return h*Math.sin(f/g*(Math.PI/2))+e},sineinout:function(f,e,h,g){return -h/2*(Math.cos(Math.PI*f/g)-1)+e},expoin:function(f,e,h,g){return(f==0)?e:h*Math.pow(2,10*(f/g-1))+e},expoout:function(f,e,h,g){return(f==g)?e+h:h*(-Math.pow(2,-10*f/g)+1)+e},expoinout:function(f,e,h,g){if(f==0){return e}if(f==g){return e+h}if((f/=g/2)<1){return h/2*Math.pow(2,10*(f-1))+e}return h/2*(-Math.pow(2,-10*--f)+2)+e},circin:function(f,e,h,g){return -h*(Math.sqrt(1-(f/=g)*f)-1)+e},circout:function(f,e,h,g){return h*Math.sqrt(1-(f=f/g-1)*f)+e},circinout:function(f,e,h,g){if((f/=g/2)<1){return -h/2*(Math.sqrt(1-f*f)-1)+e}return h/2*(Math.sqrt(1-(f-=2)*f)+1)+e},elasticin:function(g,e,k,j){var h=1.70158;var i=0;var f=k;if(g==0){return e}if((g/=j)==1){return e+k}if(!i){i=j*0.3}if(f<Math.abs(k)){f=k;var h=i/4}else{var h=i/(2*Math.PI)*Math.asin(k/f)}return -(f*Math.pow(2,10*(g-=1))*Math.sin((g*j-h)*(2*Math.PI)/i))+e},elasticout:function(g,e,k,j){var h=1.70158;var i=0;var f=k;if(g==0){return e}if((g/=j)==1){return e+k}if(!i){i=j*0.3}if(f<Math.abs(k)){f=k;var h=i/4}else{var h=i/(2*Math.PI)*Math.asin(k/f)}return f*Math.pow(2,-10*g)*Math.sin((g*j-h)*(2*Math.PI)/i)+k+e},elasticinout:function(g,e,k,j){var h=1.70158;var i=0;var f=k;if(g==0){return e}if((g/=j/2)==2){return e+k}if(!i){i=j*(0.3*1.5)}if(f<Math.abs(k)){f=k;var h=i/4}else{var h=i/(2*Math.PI)*Math.asin(k/f)}if(g<1){return -0.5*(f*Math.pow(2,10*(g-=1))*Math.sin((g*j-h)*(2*Math.PI)/i))+e}return f*Math.pow(2,-10*(g-=1))*Math.sin((g*j-h)*(2*Math.PI)/i)*0.5+k+e},backin:function(f,e,i,h,g){if(g==undefined){g=1.70158}return i*(f/=h)*f*((g+1)*f-g)+e},backout:function(f,e,i,h,g){if(g==undefined){g=1.70158}return i*((f=f/h-1)*f*((g+1)*f+g)+1)+e},backinout:function(f,e,i,h,g){if(g==undefined){g=1.70158}if((f/=h/2)<1){return i/2*(f*f*(((g*=(1.525))+1)*f-g))+e}return i/2*((f-=2)*f*(((g*=(1.525))+1)*f+g)+2)+e},bouncein:function(f,e,h,g){return h-a.bounceOut(g-f,0,h,g)+e},bounceout:function(f,e,h,g){if((f/=g)<(1/2.75)){return h*(7.5625*f*f)+e}else{if(f<(2/2.75)){return h*(7.5625*(f-=(1.5/2.75))*f+0.75)+e}else{if(f<(2.5/2.75)){return h*(7.5625*(f-=(2.25/2.75))*f+0.9375)+e}else{return h*(7.5625*(f-=(2.625/2.75))*f+0.984375)+e}}}},bounceinout:function(f,e,h,g){if(f<g/2){return a.bounceIn(f*2,0,h,g)*0.5+e}return a.bounceOut(f*2-g,0,h,g)*0.5+h*0.5+e}};return a})();
  /*  tween.js */  var Tween=function(){var a={};var c=true;var b={init:function(){return b},busy:function(){var e=false;for(var d in a){e=true;break}return e},to:function(g,e,p){var f=new Date().valueOf();var d={};var q={from:{},to:{},colors:{},node:g,t0:f,t1:f+e*1000,dur:e*1000};var o="linear";for(var j in p){if(j=="easing"){var h=p[j].toLowerCase();if(h in Easing){o=h}continue}else{if(j=="delay"){var m=(p[j]||0)*1000;q.t0+=m;q.t1+=m;continue}}if(Colors.validate(p[j])){q.colors[j]=[Colors.decode(g.data[j]),Colors.decode(p[j]),p[j]];d[j]=true}else{q.from[j]=(g.data[j]!=undefined)?g.data[j]:p[j];q.to[j]=p[j];d[j]=true}}q.ease=Easing[o];if(a[g._id]===undefined){a[g._id]=[]}a[g._id].push(q);if(a.length>1){for(var l=a.length-2;l>=0;l++){var n=a[l];for(var j in n.to){if(j in d){delete n.to[j]}else{d[j]=true}}for(var j in n.colors){if(j in d){delete n.colors[j]}else{d[j]=true}}if($.isEmptyObject(n.colors)&&$.isEmptyObject(n.to)){a.splice(l,1)}}}c=false},interpolate:function(e,h,i,g){g=(g||"").toLowerCase();var d=Easing.linear;if(g in Easing){d=Easing[g]}var f=d(e,0,1,1);if(Colors.validate(h)&&Colors.validate(i)){return lerpRGB(f,h,i)}else{if(!isNaN(h)){return lerpNumber(f,h,i)}else{if(typeof h=="string"){return(f<0.5)?h:i}}}},tick:function(){var f=true;for(var d in a){f=false;break}if(f){return}var e=new Date().valueOf();$.each(a,function(i,h){var g=false;$.each(h,function(p,t){var o=t.ease((e-t.t0),0,1,t.dur);o=Math.min(1,o);var r=t.from;var s=t.to;var j=t.colors;var l=t.node.data;var m=(o==1);for(var n in s){switch(typeof s[n]){case"number":l[n]=lerpNumber(o,r[n],s[n]);if(n=="alpha"){l[n]=Math.max(0,Math.min(1,l[n]))}break;case"string":if(m){l[n]=s[n]}break}}for(var n in j){if(m){l[n]=j[n][2]}else{var q=lerpRGB(o,j[n][0],j[n][1]);l[n]=Colors.encode(q)}}if(m){t.completed=true;g=true}});if(g){a[i]=$.map(h,function(j){if(!j.completed){return j}});if(a[i].length==0){delete a[i]}}});c=$.isEmptyObject(a);return c}};return b.init()};var lerpNumber=function(a,c,b){return c+a*(b-c)};var lerpRGB=function(b,d,c){b=Math.max(Math.min(b,1),0);var a={};$.each("rgba".split(""),function(e,f){a[f]=Math.round(d[f]+b*(c[f]-d[f]))});return a};

  arbor = (typeof(arbor)!=='undefined') ? arbor : {}
  $.extend(arbor, {
    // not really user-serviceable; use the ParticleSystem’s .tween* methods instead
    Tween:Tween,
    
    // immutable object with useful methods
    colors:{
      CSS:Colors.CSS,           // dictionary: {colorname:#fef2e2,...}
      validate:Colors.validate, // ƒ(str) -> t/f
      decode:Colors.decode,     // ƒ(hexString_or_cssColor) -> {r,g,b,a}
      encode:Colors.encode,     // ƒ({r,g,b,a}) -> hexOrRgbaString
      blend:Colors.blend        // ƒ(color, opacity) -> rgbaString
    }
  })
  
})(this.jQuery)




 
