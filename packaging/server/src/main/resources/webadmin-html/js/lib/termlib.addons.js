/*

	termlib-invaders
	a termlib.js application
	(c) mass:werk (N. Landsteiner) 2008
	based on JS/UIX invaders (c) mass:werk (N. Landsteiner) 2005
	all rights reserved
	v1.11
	
	termlib-invaders is a simple text-mode invaders game.
	requires termlib.js 1.4 or better <http://www.masswerk.at/termlib/>.
	requires a terminal set to at least 68 cols x 20 rows.
	uses namespace 'invaders' in object 'env' of the termlib.js Terminal instance.
	the entire code is contained inside the single global object 'TermlibInvaders'.
	(the massive 'apply'-syntax is a bit awkward, but this is, which does it.)
	
	example call from inside a termlib.js Terminal instance's handler:
	
	  if ( TermlibInvaders.start(this) ) {
	    return;
	  }
	  else {
	  	// oops, terminal doesn't meet the requirements
	  	this.write('Sorry, invaders failed.');
	  }
	
	call with a max game screen of 80 cols x 25 rows:
	
	  TermlibInvaders.start(this, 80, 25);

*/


TermlibInvaders = {
	version: '1.11 (original)',
	start: function( termref, maxcols, maxrows ) {
		if (!Terminal || !termref || parseFloat(termref.version)<1.4) {
			// color support required
			return false;
		}
		if (termref.conf.cols<68 || termref.conf.rows<20) {
			// required min. dimensions: 68 x 20
			return false;
		}
		if (parseFloat(termref.version)>=1.5) {
			// backup the screen
			termref.backupScreen();
		}
		var gc=TermlibInvaders.getStyleColorFromHexString;
		termref.env.invaders= {
			termref: termref,
			maxCols: maxcols || 0,
			maxRows: maxrows || 0,
			charMode: termref.charMode,
			paused: false,
			moveAll: true,
			// setup values
			rows: 3,
			cols: 5,
			maxBombs: 3,
			bombRate: 0.005,
			timer: null,
			delay: 50,
			newWaveDelay: 1500,
			textColor: gc(TermlibInvaders.textColor),
			invaderColor: gc(TermlibInvaders.invaderColor),
			invaderHitColor: gc(TermlibInvaders.invaderHitColor),
			bombColor: gc(TermlibInvaders.bombColor),
			blockColor: gc(TermlibInvaders.blockColor),
			statusColor: gc(TermlibInvaders.statusColor),
			shotColor: gc(TermlibInvaders.shotColor),
			shipColor: gc(TermlibInvaders.shipColor),
			shipHitColor: gc(TermlibInvaders.shipHitColor),
			alertColor: gc(TermlibInvaders.alertColor),
			frameColor: gc(TermlibInvaders.frameColor)
		};
		TermlibInvaders.init.apply(termref);
		return true;
	},
	// color definitions (colors will match nearest webcolor)
	textColor: '#00cc00',
	invaderColor: '#00cc00',
	invaderHitColor: '#66aa66',
	bombColor: '#cccc00',
	blockColor: '#bbbb00',
	statusColor: '#00bb00',
	shotColor: '#aacc00',
	shipColor: '#aacc00',
	shipHitColor: '#aaaaaa',
	alertColor: '#ff9900',
	// frame definitions
	// the frame is only drawn, if the terminal is bigger
	// than the game's max dimensions. if you do not want
	// to draw any frames leave 'frameChar' empty ('').
	frameChar: '*',
	frameColor: '#777777',
	// global assets
	sprites: [
	'       ',' (^o^) ',' (^-^) ','  ( ) ',' (   )',
	' (=^=) ',' ((.)) ',' ( . ) ','( (.) )',
	' ( . ) ','(  .  )','   .   ','       '
	],
	splashScreen: [
		'%c(#0c0)%+i** T E R M L I B - I N V A D E R S **%-i',
		'',
		'',
		'%c(#0c0)Instructions:',
		'',
		'%c(#0c0)  use cursor LEFT and RIGHT to move',
		'%c(#0c0)  (or use vi movements alternatively)',
		'%c(#0c0)  press space to fire',
		'%c(#0c0)',
		'%c(#0c0)  press "q" or "esc" to quit,',
		'%c(#0c0)  "p" to pause the game.',
		'',
		'',
		'%c(#0c0)%+r   press any key to start the game   %-r',
		'',
		'',
		'%c(#0c0)(c) mass:werk N.Landsteiner 2005-2008',
		'%c(#0c0)based on JS/UIX-Invaders by mass:werk'
	],
	splashScreenWidth: 40, // width of splash-screen in chars
	gameOverScreen: [
		'                          ',
		'%c(#f90)    G A M E  O V E R !    ',
		'                          ',
		'%c(#0c0) press any key to restart,',
		'%c(#0c0) "q" or "esc" for quit.   ',
		'                          '
	],
	gameOverScreenWidth: 26,
	invObject: function(y,x) {
		this.x=x;
		this.y=y;
		this.status=1;
	},
	// handlers:
	//   'this' refers to ther termlib.js Terminal instcance
	//   'inv' refers to the TermlibInvaders instance
	init: function() {
		var inv=this.env.invaders;
		// back up the terminal state
		inv.termHandler=this.handler;
		if (this.maxLines != this.conf.rows) {
			inv.charBuf=new Array();
			inv.styleBuf=new Array();
			for (var r=this.conf.rows-1; r>=this.maxLines; r--) {
				var cb=new Array();
				var sb=new Array();
				var tcb=this.charBuf[r];
				var tsb=this.styleBuf[r];
				for (var c=0; c=this.conf.cols; c++) {
					cb[c]=tcb[c];
					sb[c]=tsb[c];
				}
				inv.charBuf.push(cb);
				inv.styleBuf.push(sb);
			}
			this.maxLines = this.conf.rows;
		}
		if (this.maxCols!=this.conf.cols) {
			inv.termMaxCols=this.maxCols;
			this.maxCols=this.conf.cols;
		}
		else {
			inv.termMaxCols=-1;
		}
		inv.keyRepeatDelay1=this.keyRepeatDelay1;
		inv.keyRepeatDelay2=this.keyRepeatDelay2;
		this.keyRepeatDelay1=this.keyRepeatDelay2=inv.delay-1;
		// output init-screen
		this.clear();
		TermlibInvaders.writeToCenter.apply(this, [TermlibInvaders.splashScreen, TermlibInvaders.splashScreenWidth]);
		this.charMode=true;
		this.lock=false;
		this.handler=TermlibInvaders.splashScreenHandler;
	},
	splashScreenHandler: function() {
		var key = this.inputChar;
		if (key==this.termKey.ESC || key==113) {
			TermlibInvaders.exit.apply(this);
			return;
		}
		// setup the game
		var inv=this.env.invaders;
		TermlibInvaders.buildScreen.apply(this);
		inv.maxRight=inv.width-7;
		inv.wave=0;
		inv.score=0;
		var d=Math.floor(inv.width/5);
		var d1=Math.floor((inv.width-3*d)/2);
		inv.blockpos=new Array();
		for (var i=0; i<4; i++) {
			var x=d1+i*d;
			inv.blockpos.push(x-1);
			inv.blockpos.push(x);
			inv.blockpos.push(x+1);
		}
		TermlibInvaders.newWave.apply(this);
	},
	newWave: function() {
		this.clear();
		var inv=this.env.invaders;
		inv.wave++;
		var s='W A V E  # '+inv.wave;
		var c=Math.floor((this.conf.cols-s.length)/2);
		var r=Math.floor((this.conf.rows-3)/2)-4;
		this.typeAt(r, c, s, 4 | inv.textColor);
		this.typeAt(r+2, c, 'Get ready ...', inv.textColor);
		inv.timer=setTimeout(function() { TermlibInvaders.waveStart.apply(inv.termref); }, inv.newWaveDelay);
		this.lock=true;
	},
	waveStart: function() {
		var inv=this.env.invaders;
		clearTimeout(inv.timer);
		this.clear();
		TermlibInvaders.drawFrame.apply(this);
		inv.smove=0;
		inv.phase=1;
		inv.dir=1;
		inv.population=0;
		inv.shot= inv.shotX= 0
		inv.over=false;
		inv.bombs=0;
		inv.invrows=(inv.wave==2)? inv.rows+1:inv.rows;
		inv.invcols=(inv.wave<=2)? inv.cols:inv.cols+1;
		var changed=inv.changed=new Array();
		inv.inv=new Array();
		for (var r=0; r<inv.invrows; r++) {
			var ir=inv.inv[r]=new Array();
			for (var c=0; c<inv.invcols; c++) {
				ir[c]=new TermlibInvaders.invObject(r*2+1,c*8);
				inv.population++;
			}
		}
		inv.block=this.getRowArray(inv.width, false);
		for (var i=0; i<inv.blockpos.length; i++) {
			var x=inv.blockpos[i];
			inv.block[x]=true;
			TermlibInvaders.drawSprite(this, inv.blockY, x, 'H', inv.blockColor);
		}
		inv.bomb=new Array();
		inv.shipX=inv.shipCenter;
		TermlibInvaders.drawScoreBG.apply(this);
		TermlibInvaders.displayScore.apply(this);
		TermlibInvaders.drawSprite(this, inv.shipY, inv.shipX, TermlibInvaders.sprites[5], inv.shipColor);
		for (var i=0; i<this.maxLines; i++) {
			this.redraw(i);
			changed[i]=false;
		}
		inv.moveAll=true;
		TermlibInvaders.invStep(inv);
		inv.timer=setTimeout(function() { TermlibInvaders.mainLoop.apply(inv.termref); }, inv.delay);
		this.lock=false;
		this.handler=TermlibInvaders.keyHandler;
	},
	mainLoop: function() {
		var inv=this.env.invaders;
		clearTimeout(inv.timer);
		var now=new Date();
		var enterTime=now.getTime();
		if (inv.smove) {
			inv.shipX+=inv.smove;
			inv.smove=0;
			TermlibInvaders.drawSprite(this, inv.shipY, inv.shipX, TermlibInvaders.sprites[5], inv.shipColor);
		}
		var s=inv.sore;
		TermlibInvaders.invStep(inv);
		var changed=inv.changed;
		for (var i=0; i<this.maxLines; i++) {
			if (changed[i]) {
				this.redraw(i);
				changed[i]=false;
			}
		}
		inv.moveAll=!inv.moveAll;
		if (s!=inv.score) TermlibInvaders.displayScore.apply(this);
		if (inv.population==0) {
			this.lock=true;
			inv.phase=-1;
			inv.timer=setTimeout(function() { TermlibInvaders.waveEnd.apply(inv.termref); }, inv.delay*2);
		}
		else if (inv.invbottom==inv.shipY || inv.over) {
			this.lock=true;
			inv.phase=(inv.over)? 6:5;
			TermlibInvaders.gameOver.apply(this);
		}
		else {
			now=new Date();
			var delay=Math.max(1, inv.delay-(now.getTime()-enterTime));
			inv.timer=setTimeout(function() { TermlibInvaders.mainLoop.apply(inv.termref); }, delay);
		}
	},
	invStep: function(inv) {
		var termref=inv.termref;
		var br=0, bl=inv.right, bb=0, dir=inv.dir;
		var linestep= ((inv.invleft==0) || (inv.invright==inv.maxRight));
		var shot= (inv.shot>0), shotx=inv.shotX, shoty=inv.shipY-inv.shot;
		var bomb= inv.bomb,block=inv.block, blocky=inv.blockY, isblockrow=false;
		var sprites=TermlibInvaders.sprites, invclr=inv.invaderColor;
		var moveAll=inv.moveAll;
		if (shot && inv.shot>1) TermlibInvaders.drawSprite(termref, shoty+1,shotx,' ',0);
		for (var r=0; r<inv.invrows; r++) {
			var ir=inv.inv[r];
			for (var c=0; c<inv.invcols; c++) {
				var i=ir[c];
				if (i.status==1) {
					if (moveAll && linestep) {
						TermlibInvaders.drawSprite(termref, i.y,i.x, sprites[0], invclr);
						i.y++;
					}
					if (shot && shoty==i.y && shotx>i.x && shotx<(i.x+6)) {
						i.status=2;
						inv.population--;
						inv.score+=50;
						inv.shot=shot=0;
						TermlibInvaders.drawSprite(termref, i.y,i.x, sprites[3], inv.invaderHitColor);
					}
					else if (moveAll) {
						TermlibInvaders.drawSprite(termref, i.y,i.x, sprites[inv.phase], invclr );
						if (i.y<inv.bombMaxY && inv.bombs<inv.maxBombs && Math.random()<inv.bombRate) {
							for (var n=0; n<inv.maxBombs; n++) {
								if (bomb[n]==null) {
									bomb[n]=new TermlibInvaders.invObject(i.y+1,i.x+3);
									inv.bombs++;
									break;
								}
							}
						}
						if (i.y==blocky) isblockrow=true;
						bb=Math.max(i.y,bb);
					}
					else {
						i.x+=dir;
						br=Math.max(i.x,br);
						bl=Math.min(i.x,bl);
					}
				}
				else if (i.status==2) {
					TermlibInvaders.drawSprite(termref, i.y,i.x, sprites[4], inv.invaderHitColor);
					i.status=3
				}
				else if (i.status==3) {
					TermlibInvaders.drawSprite(termref, i.y,i.x, sprites[0], invclr);
					i.status=0;
				}
			}
		}
		for (var n=0; n<inv.maxBombs; n++) {
			var b=bomb[n];
			if (b!=null) {
				var _br=inv.top+b.y-1;
				var _bc=inv.left+b.x;
				if (termref.charBuf[_br][_bc]==86) TermlibInvaders.drawSprite(termref, b.y-1,b.x, ' ', 0);
				if (b.y==blocky && block[b.x]) {
					block[b.x]=false;
					TermlibInvaders.drawSprite(termref, blocky,b.x, ' ', 0);
					b=bomb[n]=null;
					inv.bombs--;
				}
				else if (b.y==inv.shipY) {
					if (b.x>inv.shipX && b.x<(inv.shipX+6)) {
						inv.over=true;
					}
					else {
						b=bomb[n]=null;
						inv.bombs--;
					}
				}
				else if (shot) {
					if ((b.y==shoty || b.y==shoty+1) && Math.abs(b.x-shotx)<2) {
						b=bomb[n]=null;
						inv.bombs--;
						inv.score+=5;
						inv.shot=shot=0
					}
				}
				if (b) {
					TermlibInvaders.drawSprite(termref, b.y,b.x, 'V', inv.bombColor);
					b.y++;
				}
			}
		}
		if (shot) {
			if (shoty>0) {
				if (shoty==blocky && inv.block[shotx]) {
					inv.block[shotx]=false;
					TermlibInvaders.drawSprite(termref, blocky,shotx, ' ', 0);
					inv.shot=0;
					
				}
				else {
					TermlibInvaders.drawSprite(termref, shoty,shotx, '|', inv.shotColor);
					inv.shot++;
				}
			}
			else {
				inv.shot=0;
			}
		}
		if (moveAll) {
			inv.invbottom=bb;
		}
		else {
			inv.invleft=bl;
			inv.invright=br;
			if (dir==-1 && bl==0) {
				inv.dir=1;
			}
			else if (dir==1 && br==inv.maxRight) {
				inv.dir=-1;
			}
			inv.phase=(inv.phase==1)? 2:1;
		}
		// restore any overwritten blocks
		if (isblockrow) {
			var blockpos=inv.blockpos;
			for (var i=0; i<inv.blockpos.length; i++) {
				var x=blockpos[i];
				if (block[x] && termref.charBuf[inv.top+blocky][inv.left+x]<=32) {
					TermlibInvaders.drawSprite(termref, blocky,x, 'H', inv.blockColor);
				}
			}
		}
	},
	waveEnd: function() {
		var inv=this.env.invaders;
		clearTimeout(inv.timer);
		var drawblocks=false;
		var r;
		if (inv.phase==0) {
			this.clear();
			TermlibInvaders.drawFrame.apply(this);
			TermlibInvaders.drawScoreBG.apply(this);
			TermlibInvaders.displayScore.apply(this);
			if (inv.width+1<this.maxCols || inv.height+1<this.maxLines) {
				for (r=0; r<this.maxLines; r++) this.redraw(r);
			}
			drawblocks=true;
		}
		else {
			r=inv.shipY-inv.phase;
			TermlibInvaders.drawSprite(this, r, inv.shipX, TermlibInvaders.sprites[0], inv.shipColor);
			this.redraw(inv.top+r);
			if (inv.shipY-inv.phase==inv.blockY) drawblocks=true;
		}
		if (inv.phase==inv.shipY) {
			inv.timer=setTimeout(function() { TermlibInvaders.newWave.apply(inv.termref); }, inv.delay);
		}
		else {
			inv.phase++;
			r=inv.shipY-inv.phase;
			TermlibInvaders.drawSprite(this, r, inv.shipX, TermlibInvaders.sprites[5], inv.shipColor);
			this.redraw(inv.top+r);
			if (r==inv.blockY) drawblocks=true;
			if (drawblocks) {
				var block=inv.block;
				var blockpos=inv.blockpos;
				r=inv.blockY;
				for (var i=0; i<inv.blockpos.length; i++) {
					var x=blockpos[i];
					if (block[x] && inv.termref.charBuf[inv.top+r][inv.left+x]<=32) {
						TermlibInvaders.drawSprite(inv.termref, r,x, 'H', inv.blockColor);
					}
				}
				this.redraw(inv.top+r)
			}
			inv.timer=setTimeout(function() { TermlibInvaders.waveEnd.apply(inv.termref); }, Math.max(10, inv.delay*2-inv.phase*2));
		}
	},
	gameOver: function() {
		var inv=this.env.invaders;
		clearTimeout(inv.timer);
		if (inv.phase>=TermlibInvaders.sprites.length) {
			TermlibInvaders.writeToCenter.apply(this, [TermlibInvaders.gameOverScreen, TermlibInvaders.gameOverScreenWidth]);
			this.lock=false;
			this.handler=TermlibInvaders.splashScreenHandler;
		}
		else {
			TermlibInvaders.drawSprite(this, inv.shipY,inv.shipX, TermlibInvaders.sprites[inv.phase++], inv.shipHitColor);
			this.redraw(inv.top+inv.shipY);
			inv.timer=setTimeout(function() { TermlibInvaders.gameOver.apply(inv.termref); }, inv.delay*3);
		}
	},
	keyHandler: function() {
		var inv=this.env.invaders;
		var key=this.inputChar;
		if (key==this.termKey.ESC || key==113) {
			// esc or q
			TermlibInvaders.exit.apply(this);
		}
		else if (key==112 || inv.paused) {
			// p or paused
			TermlibInvaders.pause.apply(this);
		}
		// cursor movements
		else if (key==this.termKey.LEFT || key==104) {
			// left
			if (inv.shipX>0) inv.smove=-1;
			return;
		}
		else if (key==this.termKey.RIGHT || key==108) {
			// right
			if (inv.shipX<inv.maxRight) inv.smove=1;
			return;
		}
		else if (key==32) {
			// space
			if (inv.shot==0) {
				inv.shot=1;
				inv.shotX=inv.shipX+3;
			}
		}
	},
	pause: function() {
		var inv=this.env.invaders;
		clearTimeout(inv.timer);
		inv.paused=!inv.paused;
		var text=(inv.paused)? ' *** P A U S E D *** ' :'                     ';
		this.typeAt(Math.floor(this.maxLines/2)-2, Math.floor((this.maxCols-text.length)/2), text, inv.alertColor);
		if (!inv.paused) TermlibInvaders.mainLoop.apply(this);
	},
	drawSprite: function(termref, r,c,t,s) {
		var inv=termref.env.invaders;
		r+=inv.top;
		c+=inv.left;
		var cb=termref.charBuf[r];
		var sb=termref.styleBuf[r];
		for (var i=0; i<t.length; i++, c++) {
			cb[c]=t.charCodeAt(i);
			sb[c]=s;
		}
		inv.changed[r]=true;
	},
	drawScoreBG: function() {
		var inv=this.env.invaders;
		var srs=this.styleBuf[inv.statusRow];
		var src=this.charBuf[inv.statusRow];
		var clr=inv.statusColor | 1;
		for (var c=inv.left; c<inv.right; c++) {
			srs[c]=clr;
			src[c]=0;
		}
	},
	displayScore: function() {
		var inv=this.env.invaders;
		var text='Invaders | "q","esc": quit "p": pause |  Wave: '+inv.wave+'  Score: '+inv.score;
		var x=inv.left+Math.floor((inv.width-text.length)/2);
		var b=this.charBuf[inv.statusRow];
		for (var i=0; i<text.length; i++) b[x+i]=text.charCodeAt(i);
		this.redraw(inv.statusRow);
	},
	writeToCenter: function(buffer, bufferWidth) {
		var sx = Math.max(0, Math.floor((this.maxCols-bufferWidth)/2));
		var sy = Math.max(0, Math.floor((this.maxLines-buffer.length)/2));
		for (var i=0; i<buffer.length; i++) {
			this.cursorSet(sy+i, sx);
			this.write(buffer[i]);
		}
	},
	buildScreen: function() {
		// (re)build a screen on max dimensions
		this.clear();
		var inv=this.env.invaders;
		if (inv.maxCols>0 && this.maxCols>inv.maxCols) {
			inv.width = inv.maxCols;
			inv.left= Math.floor((this.maxCols-inv.maxCols)/2);
			inv.right= inv.left+inv.width;
		}
		else {
			inv.width= inv.right= this.maxCols;
			inv.left=0;
		}
		if (inv.maxRows>0 && this.maxLines>inv.maxRows) {
			inv.height = inv.maxRows;
			inv.top= Math.floor((this.maxLines-inv.maxRows)/2);
			inv.bottom=inv.top+inv.height;
		}
		else {
			inv.height= inv.bottom= this.maxLines;
			inv.top=0;
		}
		inv.shipCenter=Math.floor((inv.width-3)/2);
		inv.statusRow=inv.bottom-1;
		inv.maxRight=inv.width-7;
		inv.shipY=inv.height-3;
		inv.bombMaxY=inv.height-7;
		inv.blockY=inv.height-5;
	},
	drawFrame: function() {
		var inv=this.env.invaders;
		if (TermlibInvaders.frameChar) {
			var r0, r1, i;
			var c = TermlibInvaders.frameChar.charCodeAt(0);
			var cc= inv.frameColor;
			if (inv.height+1<this.maxLines) {
				r0=Math.max(inv.left-1, 0);
				r1=Math.min(inv.right+1, this.maxCols);
				var cb1=this.charBuf[inv.top-1];
				var sb1=this.styleBuf[inv.top-1];
				var cb2=this.charBuf[inv.bottom];
				var sb2=this.styleBuf[inv.bottom];
				for (i=r0; i<r1; i++) {
					cb1[i]=cb2[i]=c;
					sb1[i]=sb2[i]=cc;
				}
			}
			if (inv.width+1<this.maxCols) {
				r0=Math.max(inv.top-1, 0);
				r1=Math.min(inv.bottom+1, this.maxLines);
				var p1=inv.left-1;
				var p2=inv.right;
				for (i=r0; i<r1; i++) {
					var b=this.charBuf[i];
					b[p1]=b[p2]=c;
					b=this.styleBuf[i];
					b[p1]=b[p2]=cc;
				}
			}
		}
	},
	exit: function() {
		var inv=this.env.invaders;
		if (inv.timer) clearTimeout(inv.timer);
		if (parseFloat(this.version)>=1.5) {
			// backup the screen
			this.restoreScreen();
		}
		else {
			// reset the terminal "manually"
			this.clear();
			this.handler=inv.termHandler;
			if (inv.charBuf) {
				for (var r=0; r<inv.charBuff.length; r++) {
					var tr=this.maxLines-1;
					this.charBuf[tr]=inv.charBuf[r];
					this.styleBuf[tr]=inv.styleBuf[r];
					this.redraw(tr);
					this.maxLines--;
				}
			}
			if (inv.termMaxCols>=0) this.maxCols=inv.termMaxCols;
			this.charMode=inv.charMode;
		}
		this.keyRepeatDelay1=inv.keyRepeatDelay1;
		this.keyRepeatDelay2=inv.keyRepeatDelay2;
		delete inv.termref;
		this.lock=false;
		// delete instance and leave with a prompt
		delete this.env.invaders;
		this.prompt();
	},
	getStyleColorFromHexString: function(clr) {
		// returns a stylevector for the given color-string
		var cc=Terminal.prototype.globals.webifyColor(clr.replace(/^#/,''));
		if (cc) {
			return Terminal.prototype.globals.webColors[cc]*0x10000;
		}
		return 0;
	}
};

// eof