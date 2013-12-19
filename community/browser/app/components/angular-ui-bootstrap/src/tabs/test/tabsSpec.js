describe('tabs', function() {
  beforeEach(module('ui.bootstrap.tabs', 'template/tabs/tabset.html', 'template/tabs/tab.html', 'template/tabs/tabset-titles.html'));

  var elm, scope;
  function titles() {
    return elm.find('ul.nav-tabs li');
  }
  function contents() {
    return elm.find('div.tab-content div.tab-pane');
  }

  function expectTitles(titlesArray) {
    var t = titles();
    expect(t.length).toEqual(titlesArray.length);
    for (var i=0; i<t.length; i++) {
      expect(t.eq(i).text().trim()).toEqual(titlesArray[i]);
    }
  }
  function expectContents(contentsArray) {
    var c = contents();
    expect(c.length).toEqual(contentsArray.length);
    for (var i=0; i<c.length; i++) {
      expect(c.eq(i).text().trim()).toEqual(contentsArray[i]);
    }
  }


  describe('basics', function() {

    beforeEach(inject(function($compile, $rootScope) {
      scope = $rootScope.$new();
      scope.first = '1';
      scope.second = '2';
      scope.actives = {};
      scope.selectFirst = jasmine.createSpy();
      scope.selectSecond = jasmine.createSpy();
      scope.deselectFirst = jasmine.createSpy();
      scope.deselectSecond = jasmine.createSpy();
      elm = $compile([
        '<div>',
        '  <tabset class="hello" data-pizza="pepperoni">',
        '    <tab heading="First Tab {{first}}" active="actives.one" select="selectFirst()" deselect="deselectFirst()">',
        '      first content is {{first}}',
        '    </tab>',
        '    <tab active="actives.two" select="selectSecond()" deselect="deselectSecond()">',
        '      <tab-heading><b>Second</b> Tab {{second}}</tab-heading>',
        '      second content is {{second}}',
        '    </tab>',
        '  </tabs>',
        '</div>'
      ].join('\n'))(scope);
      scope.$apply();
      return elm;
    }));

    it('should pass class and other attributes on to tab template', function() {
      var tabbable = elm.find('.tabbable');
      expect(tabbable).toHaveClass('hello');
      expect(tabbable.attr('data-pizza')).toBe('pepperoni');
    });

    it('should create clickable titles', function() {
      var t = titles();
      expect(t.length).toBe(2);
      expect(t.find('a').eq(0).text()).toBe('First Tab 1');
      //It should put the tab-heading element into the 'a' title
      expect(t.find('a').eq(1).children().is('tab-heading')).toBe(true);
      expect(t.find('a').eq(1).children().html()).toBe('<b>Second</b> Tab 2');
    });

    it('should bind tabs content and set first tab active', function() {
      expectContents(['first content is 1', 'second content is 2']);
      expect(titles().eq(0)).toHaveClass('active');
      expect(titles().eq(1)).not.toHaveClass('active');
      expect(scope.actives.one).toBe(true);
      expect(scope.actives.two).toBe(false);
    });

    it('should change active on click', function() {
      titles().eq(1).find('a').click();
      expect(contents().eq(1)).toHaveClass('active');
      expect(titles().eq(0)).not.toHaveClass('active');
      expect(titles().eq(1)).toHaveClass('active');
      expect(scope.actives.one).toBe(false);
      expect(scope.actives.two).toBe(true);
    });

    it('should call select callback on select', function() {
      titles().eq(1).find('a').click();
      expect(scope.selectSecond).toHaveBeenCalled();
      titles().eq(0).find('a').click();
      expect(scope.selectFirst).toHaveBeenCalled();
    });

    it('should call deselect callback on deselect', function() {
      titles().eq(1).find('a').click();
      titles().eq(0).find('a').click();
      expect(scope.deselectSecond).toHaveBeenCalled();
      titles().eq(1).find('a').click();
      expect(scope.deselectFirst).toHaveBeenCalled();
    });

  });

  describe('ng-repeat', function() {

    beforeEach(inject(function($compile, $rootScope) {
      scope = $rootScope.$new();

      function makeTab(active) {
        return {
          active: !!active,
          select: jasmine.createSpy()
        };
      }
      scope.tabs = [
        makeTab(), makeTab(), makeTab(true), makeTab()
      ];
      elm = $compile([
        '<tabset>',
        '  <tab ng-repeat="t in tabs" active="t.active" select="t.select()">',
        '    <tab-heading><b>heading</b> {{index}}</tab-heading>',
        '    content {{$index}}',
        '  </tab>',
        '</tabset>'
      ].join('\n'))(scope);
      scope.$apply();
    }));

    function titles() {
      return elm.find('ul.nav-tabs li');
    }
    function contents() {
      return elm.find('div.tab-content div.tab-pane');
    }

    function expectTabActive(activeTab) {
      var _titles = titles();
      angular.forEach(scope.tabs, function(tab, i) {
        if (activeTab === tab) {
          expect(tab.active).toBe(true);
          //It should only call select ONCE for each select
          expect(tab.select).toHaveBeenCalled();
          expect(_titles.eq(i)).toHaveClass('active');
          expect(contents().eq(i).text().trim()).toBe('content ' + i);
          expect(contents().eq(i)).toHaveClass('active');
        } else {
          expect(tab.active).toBe(false);
          expect(_titles.eq(i)).not.toHaveClass('active');
        }
      });
    }

    it('should make tab titles and set active tab active', function() {
      expect(titles().length).toBe(scope.tabs.length);
      expectTabActive(scope.tabs[2]);
    });

    it('should switch active when clicking', function() {
      titles().eq(3).find('a').click();
      expectTabActive(scope.tabs[3]);
    });

    it('should switch active when setting active=true', function() {
      scope.$apply('tabs[2].active = true');
      expectTabActive(scope.tabs[2]);
    });

    it('should deselect all when no tabs are active', function() {
      angular.forEach(scope.tabs, function(t) { t.active = false; });
      scope.$apply();
      expectTabActive(null);
      expect(contents().filter('.active').length).toBe(0);

      scope.tabs[2].active = true;
      scope.$apply();
      expectTabActive(scope.tabs[2]);
    });
  });

  describe('advanced tab-heading element', function() {
    beforeEach(inject(function($compile, $rootScope) {
      scope = $rootScope.$new();
      scope.myHtml = "<b>hello</b>, there!";
      scope.value = true;
      elm = $compile([
        '<tabset>',
        '  <tab>',
        '    <tab-heading ng-bind-html-unsafe="myHtml" ng-show="value">',
        '    </tab-heading>',
        '  </tab>',
        '  <tab><data-tab-heading>1</data-tab-heading></tab>',
        '  <tab><div data-tab-heading>2</div></tab>',
        '  <tab><div tab-heading>3</div></tab>',
        '</tabset>'
      ].join('\n'))(scope);
      scope.$apply();
    }));

    function heading() {
      return elm.find('ul li a').children();
    }

    it('should create a heading bound to myHtml', function() {
      expect(heading().eq(0).html()).toBe("<b>hello</b>, there!");
    });

    it('should hide and show the heading depending on value', function() {
      expect(heading().eq(0).css('display')).not.toBe('none');
      scope.$apply('value = false');
      expect(heading().eq(0).css('display')).toBe('none');
      scope.$apply('value = true');
      expect(heading().eq(0).css('display')).not.toBe('none');
    });

    it('should have a tab-heading no matter what syntax was used', function() {
      expect(heading().eq(1).text()).toBe('1');
      expect(heading().eq(2).text()).toBe('2');
      expect(heading().eq(3).text()).toBe('3');
    });

  });

  //Tests that http://git.io/lG6I9Q is fixed
  describe('tab ordering', function() {

    beforeEach(inject(function($compile, $rootScope) {
      scope = $rootScope.$new();
      scope.tabs = [
        { title:"Title 1", available:true },
        { title:"Title 2", available:true },
        { title:"Title 3", available:true }
      ];
      elm = $compile([
        '<tabset>',
        '  <!-- a comment -->',
        '  <div>div that makes troubles</div>',
        '  <tab heading="first">First Static</tab>',
        '  <div>another div that may do evil</div>',
        '  <tab ng-repeat="tab in tabs | filter:tabIsAvailable" active="tab.active" heading="{{tab.title}}">some content</tab>',
        '  <!-- another comment -->',
        '  <tab heading="mid">Mid Static</tab>',
        '  a text node',
        '  <!-- another comment -->',
        '  <span>yet another span that may do evil</span>',
        '  <tab ng-repeat="tab in tabs | filter:tabIsAvailable" active="tab.active" heading="Second {{tab.title}}">some content</tab>',
        '  a text node',
        '  <span>yet another span that may do evil</span>',
        '  <!-- another comment -->',
        '  <tab heading="last">Last Static</tab>',
        '  a text node',
        '  <span>yet another span that may do evil</span>',
        '  <!-- another comment -->',
        '</tabset>'
      ].join('\n'))(scope);

      scope.tabIsAvailable = function(tab) {
        return tab.available;
      };
    }));

    it('should preserve correct ordering', function() {
      function titles() {
        return elm.find('ul.nav-tabs li a');
      }
      scope.$apply();
      expect(titles().length).toBe(9);
      scope.$apply('tabs[1].available=false');
      scope.$digest();
      expect(titles().length).toBe(7);
      scope.$apply('tabs[0].available=false');
      scope.$digest();
      expect(titles().length).toBe(5);
      scope.$apply('tabs[2].available=false');
      scope.$digest();
      expect(titles().length).toBe(3);
      scope.$apply('tabs[0].available=true');
      scope.$digest();
      expect(titles().length).toBe(5);
      scope.$apply('tabs[1].available=true');
      scope.$apply('tabs[2].available=true');
      scope.$digest();
      expect(titles().length).toBe(9);
      expect(titles().eq(0).text().trim()).toBe("first");
      expect(titles().eq(1).text().trim()).toBe("Title 1");
      expect(titles().eq(2).text().trim()).toBe("Title 2");
      expect(titles().eq(3).text().trim()).toBe("Title 3");
      expect(titles().eq(4).text().trim()).toBe("mid");
      expect(titles().eq(5).text().trim()).toBe("Second Title 1");
      expect(titles().eq(6).text().trim()).toBe("Second Title 2");
      expect(titles().eq(7).text().trim()).toBe("Second Title 3");
      expect(titles().eq(8).text().trim()).toBe("last");
    });
  });

  describe('tabset controller', function() {
    function mockTab(isActive) {
      return { active: !!isActive };
    }

    var ctrl;
    beforeEach(inject(function($controller, $rootScope) {
      scope = $rootScope;
      //instantiate the controller stand-alone, without the directive
      ctrl = $controller('TabsetController', {$scope: scope, $element: null});
    }));


    describe('select', function() {

      it('should mark given tab selected', function() {
        var tab = mockTab();

        ctrl.select(tab);
        expect(tab.active).toBe(true);
      });


      it('should deselect other tabs', function() {
        var tab1 = mockTab(), tab2 = mockTab(), tab3 = mockTab();

        ctrl.addTab(tab1);
        ctrl.addTab(tab2);
        ctrl.addTab(tab3);

        ctrl.select(tab1);
        expect(tab1.active).toBe(true);
        expect(tab2.active).toBe(false);
        expect(tab3.active).toBe(false);

        ctrl.select(tab2);
        expect(tab1.active).toBe(false);
        expect(tab2.active).toBe(true);
        expect(tab3.active).toBe(false);

        ctrl.select(tab3);
        expect(tab1.active).toBe(false);
        expect(tab2.active).toBe(false);
        expect(tab3.active).toBe(true);
      });
    });


    describe('addTab', function() {

      it('should append tab', function() {
        var tab1 = mockTab(), tab2 = mockTab();

        expect(ctrl.tabs).toEqual([]);

        ctrl.addTab(tab1);
        expect(ctrl.tabs).toEqual([tab1]);

        ctrl.addTab(tab2);
        expect(ctrl.tabs).toEqual([tab1, tab2]);
      });


      it('should select the first one', function() {
        var tab1 = mockTab(), tab2 = mockTab();

        ctrl.addTab(tab1);
        expect(tab1.active).toBe(true);

        ctrl.addTab(tab2);
        expect(tab1.active).toBe(true);
      });

      it('should select a tab added that\'s already active', function() {
        var tab1 = mockTab(), tab2 = mockTab(true);
        ctrl.addTab(tab1);
        expect(tab1.active).toBe(true);

        ctrl.addTab(tab2);
        expect(tab1.active).toBe(false);
        expect(tab2.active).toBe(true);
      });
    });
  });

  describe('remove', function() {

    it('should remove title tabs when elements are destroyed and change selection', inject(function($controller, $compile, $rootScope) {
      scope = $rootScope.$new();
      elm = $compile("<tabset><tab heading='1'>Hello</tab><tab ng-repeat='i in list' heading='tab {{i}}'>content {{i}}</tab></tabset>")(scope);
      scope.$apply();

      expectTitles(['1']);
      expectContents(['Hello']);

      scope.$apply('list = [1,2,3]');
      expectTitles(['1', 'tab 1', 'tab 2', 'tab 3']);
      expectContents(['Hello', 'content 1', 'content 2', 'content 3']);

      titles().find('a').eq(3).click();
      expect(contents().eq(3)).toHaveClass('active');
      expect(titles().eq(3)).toHaveClass('active');

      scope.$apply('list = [1,2]');
      expectTitles(['1', 'tab 1', 'tab 2']);
      expectContents(['Hello', 'content 1', 'content 2']);

      expect(titles().eq(2)).toHaveClass('active');
      expect(contents().eq(2)).toHaveClass('active');
    }));
  });

  describe('disabled', function() {
    beforeEach(inject(function($compile, $rootScope) {
      scope = $rootScope.$new();

      function makeTab(disabled) {
        return {
          active: false,
          select: jasmine.createSpy(),
          disabled: disabled
        };
      }
      scope.tabs = [
        makeTab(false), makeTab(true), makeTab(false), makeTab(true)
      ];
      elm = $compile([
        '<tabset>',
        '  <tab ng-repeat="t in tabs" active="t.active" select="t.select()" disabled="t.disabled">',
        '    <tab-heading><b>heading</b> {{index}}</tab-heading>',
        '    content {{$index}}',
        '  </tab>',
        '</tabset>'
      ].join('\n'))(scope);
      scope.$apply();
    }));

    function expectTabActive(activeTab) {
      var _titles = titles();
      angular.forEach(scope.tabs, function(tab, i) {
        if (activeTab === tab) {
          expect(tab.active).toBe(true);
          expect(tab.select.callCount).toBe( (tab.disabled) ? 0 : 1 );
          expect(_titles.eq(i)).toHaveClass('active');
          expect(contents().eq(i).text().trim()).toBe('content ' + i);
          expect(contents().eq(i)).toHaveClass('active');
        } else {
          expect(tab.active).toBe(false);
          expect(_titles.eq(i)).not.toHaveClass('active');
        }
      });
    }

    it('should not switch active when clicking on title', function() {
      titles().eq(2).find('a').click();
      expectTabActive(scope.tabs[2]);

      titles().eq(3).find('a').click();
      expectTabActive(scope.tabs[2]);
    });

    it('should toggle between states', function() {
      expect(titles().eq(3)).toHaveClass('disabled');
      scope.$apply('tabs[3].disabled = false');
      expect(titles().eq(3)).not.toHaveClass('disabled');

      expect(titles().eq(2)).not.toHaveClass('disabled');
      scope.$apply('tabs[2].disabled = true');
      expect(titles().eq(2)).toHaveClass('disabled');
    });
  });

  describe('vertical', function() {
    beforeEach(inject(function($compile, $rootScope) {
      scope = $rootScope.$new();

      elm = $compile('<tabset vertical="true"></tabset>')(scope);
      scope.$apply();
    }));

    it('to stack tabs', function() {
      expect(elm.find('ul.nav-tabs')).toHaveClass('nav-stacked');
    });
  });

  describe('type', function() {
    beforeEach(inject(function($compile, $rootScope) {
      scope = $rootScope.$new();
      scope.navType = 'pills';

      elm = $compile('<tabset type="navType"></tabset>')(scope);
      scope.$apply();
    }));

    it('to show pills', function() {
      expect(elm.find('ul')).toHaveClass('nav-pills');
      expect(elm.find('ul')).not.toHaveClass('nav-tabs');
    });
  });

  describe('direction', function() {
    it('should not have `tab-left`, `tab-right` nor `tabs-below` classes if the direction is undefined', inject(function($compile, $rootScope) {
      scope = $rootScope.$new();
      scope.direction = undefined;

      elm = $compile('<tabset direction="direction"></tabset>')(scope);
      scope.$apply();
      expect(elm).not.toHaveClass('tabs-left');
      expect(elm).not.toHaveClass('tabs-right');
      expect(elm).not.toHaveClass('tabs-below');
      expect(elm.find('.nav + .tab-content').length).toBe(1);
    }));

    it('should only have the `tab-left` direction class if the direction is "left"', inject(function($compile, $rootScope) {
      scope = $rootScope.$new();
      scope.direction = 'left';

      elm = $compile('<tabset direction="direction"></tabset>')(scope);
      scope.$apply();
      expect(elm).toHaveClass('tabs-left');
      expect(elm).not.toHaveClass('tabs-right');
      expect(elm).not.toHaveClass('tabs-below');
      expect(elm.find('.nav + .tab-content').length).toBe(1);
    }));

    it('should only have the `tab-right direction class if the direction is "right"', inject(function($compile, $rootScope) {
      scope = $rootScope.$new();
      scope.direction = 'right';

      elm = $compile('<tabset direction="direction"></tabset>')(scope);
      scope.$apply();
      expect(elm).not.toHaveClass('tabs-left');
      expect(elm).toHaveClass('tabs-right');
      expect(elm).not.toHaveClass('tabs-below');
      expect(elm.find('.nav + .tab-content').length).toBe(1);
    }));

    it('should only have the `tab-below direction class if the direction is "below"', inject(function($compile, $rootScope) {
      scope = $rootScope.$new();
      scope.direction = 'below';

      elm = $compile('<tabset direction="direction"></tabset>')(scope);
      scope.$apply();
      expect(elm).not.toHaveClass('tabs-left');
      expect(elm).not.toHaveClass('tabs-right');
      expect(elm).toHaveClass('tabs-below');
      expect(elm.find('.tab-content + .nav').length).toBe(1);
    }));
  });

  //https://github.com/angular-ui/bootstrap/issues/524
  describe('child compilation', function() {

    var elm;
    beforeEach(inject(function($compile, $rootScope) {
      elm = $compile('<tabset><tab><div></div></tab></tabset></div>')($rootScope.$new());
      $rootScope.$apply();
    }));

    it('should hookup the tab\'s children to the tab with $compile', function() {
      var tabChild = $('.tab-pane', elm).children().first();
      expect(tabChild.inheritedData('$tabsetController')).toBeTruthy();
    });
  });

  //https://github.com/angular-ui/bootstrap/issues/631
  describe('ng-options in content', function() {
    var elm;
    it('should render correct amount of options', inject(function($compile, $rootScope) {
      var scope = $rootScope.$new();
      elm = $compile('<tabset><tab><select ng-model="foo" ng-options="i for i in [1,2,3]"></tab>')(scope);
      scope.$apply();

      var select = elm.find('select');
      scope.$apply();
      expect(select.children().length).toBe(4);
    }));
  });

  //https://github.com/angular-ui/bootstrap/issues/599
  describe('ng-repeat in content', function() {
    var elm;
    it('should render ng-repeat', inject(function($compile, $rootScope) {
      var scope = $rootScope.$new();
      scope.tabs = [
        {title:'a', array:[1,2,3]},
        {title:'b', array:[2,3,4]},
        {title:'c', array:[3,4,5]}
      ];
      elm = $compile('<div><tabset>' +
        '<tab ng-repeat="tab in tabs" heading="{{tab.title}}">' +
          '<tab-heading>{{$index}}</tab-heading>' +
          '<span ng-repeat="a in tab.array">{{a}},</span>' +
        '</tab>' +
      '</tabset></div>')(scope);
      scope.$apply();

      var contents = elm.find('.tab-pane');
      expect(contents.eq(0).text().trim()).toEqual('1,2,3,');
      expect(contents.eq(1).text().trim()).toEqual('2,3,4,');
      expect(contents.eq(2).text().trim()).toEqual('3,4,5,');
    }));
  });
});
