describe('accordion', function () {
  var $scope;

  beforeEach(module('ui.bootstrap.accordion'));
  beforeEach(module('template/accordion/accordion.html'));
  beforeEach(module('template/accordion/accordion-group.html'));

  beforeEach(inject(function ($rootScope) {
    $scope = $rootScope;
  }));

  describe('controller', function () {

    var ctrl, $element, $attrs;
    beforeEach(inject(function($controller) {
      $attrs = {}; $element = {};
      ctrl = $controller('AccordionController', { $scope: $scope, $element: $element, $attrs: $attrs });
    }));

    describe('addGroup', function() {
      it('adds a the specified panel to the collection', function() {
        var group1, group2;
        ctrl.addGroup(group1 = $scope.$new());
        ctrl.addGroup(group2 = $scope.$new());
        expect(ctrl.groups.length).toBe(2);
        expect(ctrl.groups[0]).toBe(group1);
        expect(ctrl.groups[1]).toBe(group2);
      });
    });

    describe('closeOthers', function() {
      var group1, group2, group3;
      beforeEach(function() {
        ctrl.addGroup(group1 = { isOpen: true, $on : angular.noop });
        ctrl.addGroup(group2 = { isOpen: true, $on : angular.noop });
        ctrl.addGroup(group3 = { isOpen: true, $on : angular.noop });
      });
      it('should close other panels if close-others attribute is not defined', function() {
        delete $attrs.closeOthers;
        ctrl.closeOthers(group2);
        expect(group1.isOpen).toBe(false);
        expect(group2.isOpen).toBe(true);
        expect(group3.isOpen).toBe(false);
      });

      it('should close other panels if close-others attribute is true', function() {
        $attrs.closeOthers = 'true';
        ctrl.closeOthers(group3);
        expect(group1.isOpen).toBe(false);
        expect(group2.isOpen).toBe(false);
        expect(group3.isOpen).toBe(true);
      });

      it('should not close other panels if close-others attribute is false', function() {
        $attrs.closeOthers = 'false';
        ctrl.closeOthers(group2);
        expect(group1.isOpen).toBe(true);
        expect(group2.isOpen).toBe(true);
        expect(group3.isOpen).toBe(true);
      });

      describe('setting accordionConfig', function() {
        var originalCloseOthers;
        beforeEach(inject(function(accordionConfig) {
          originalCloseOthers = accordionConfig.closeOthers;
          accordionConfig.closeOthers = false;
        }));
        afterEach(inject(function(accordionConfig) {
          // return it to the original value
          accordionConfig.closeOthers = originalCloseOthers;
        }));

        it('should not close other panels if accordionConfig.closeOthers is false', function() {
          ctrl.closeOthers(group2);
          expect(group1.isOpen).toBe(true);
          expect(group2.isOpen).toBe(true);
          expect(group3.isOpen).toBe(true);
        });
      });
    });

    describe('removeGroup', function() {
      it('should remove the specified panel', function () {
        var group1, group2, group3;
        ctrl.addGroup(group1 = $scope.$new());
        ctrl.addGroup(group2 = $scope.$new());
        ctrl.addGroup(group3 = $scope.$new());
        ctrl.removeGroup(group2);
        expect(ctrl.groups.length).toBe(2);
        expect(ctrl.groups[0]).toBe(group1);
        expect(ctrl.groups[1]).toBe(group3);
      });
      it('should ignore remove of non-existing panel', function () {
        var group1, group2;
        ctrl.addGroup(group1 = $scope.$new());
        ctrl.addGroup(group2 = $scope.$new());
        expect(ctrl.groups.length).toBe(2);
        ctrl.removeGroup({});
        expect(ctrl.groups.length).toBe(2);
      });
    });
  });

  describe('accordion-group', function () {

    var scope, $compile;
    var element, groups;
    var findGroupLink = function (index) {
      return groups.eq(index).find('a').eq(0);
    };
    var findGroupBody = function (index) {
      return groups.eq(index).find('.panel-collapse').eq(0);
    };


    beforeEach(inject(function(_$rootScope_, _$compile_) {
      scope = _$rootScope_;
      $compile = _$compile_;
    }));

    afterEach(function () {
      element = groups = scope = $compile = undefined;
    });

    describe('with static panels', function () {
      beforeEach(function () {
        var tpl =
          '<accordion>' +
            '<accordion-group heading="title 1">Content 1</accordion-group>' +
            '<accordion-group heading="title 2">Content 2</accordion-group>' +
            '</accordion>';
        element = angular.element(tpl);
        $compile(element)(scope);
        scope.$digest();
        groups = element.find('.panel');
      });
      afterEach(function() {
        element.remove();
      });

      it('should create accordion panels with content', function () {
        expect(groups.length).toEqual(2);
        expect(findGroupLink(0).text()).toEqual('title 1');
        expect(findGroupBody(0).text().trim()).toEqual('Content 1');
        expect(findGroupLink(1).text()).toEqual('title 2');
        expect(findGroupBody(1).text().trim()).toEqual('Content 2');
      });

      it('should change selected element on click', function () {
        findGroupLink(0).click();
        scope.$digest();
        expect(findGroupBody(0).scope().isOpen).toBe(true);

        findGroupLink(1).click();
        scope.$digest();
        expect(findGroupBody(0).scope().isOpen).toBe(false);
        expect(findGroupBody(1).scope().isOpen).toBe(true);
      });

      it('should toggle element on click', function() {
        findGroupLink(0).click();
        scope.$digest();
        expect(findGroupBody(0).scope().isOpen).toBe(true);
        findGroupLink(0).click();
        scope.$digest();
        expect(findGroupBody(0).scope().isOpen).toBe(false);
      });
    });

    describe('with dynamic panels', function () {
      var model;
      beforeEach(function () {
        var tpl =
          '<accordion>' +
            '<accordion-group ng-repeat="group in groups" heading="{{group.name}}">{{group.content}}</accordion-group>' +
          '</accordion>';
        element = angular.element(tpl);
        model = [
          {name: 'title 1', content: 'Content 1'},
          {name: 'title 2', content: 'Content 2'}
        ];

        $compile(element)(scope);
        scope.$digest();
      });

      it('should have no panels initially', function () {
        groups = element.find('.panel');
        expect(groups.length).toEqual(0);
      });

      it('should have a panel for each model item', function() {
        scope.groups = model;
        scope.$digest();
        groups = element.find('.panel');
        expect(groups.length).toEqual(2);
        expect(findGroupLink(0).text()).toEqual('title 1');
        expect(findGroupBody(0).text().trim()).toEqual('Content 1');
        expect(findGroupLink(1).text()).toEqual('title 2');
        expect(findGroupBody(1).text().trim()).toEqual('Content 2');
      });

      it('should react properly on removing items from the model', function () {
        scope.groups = model;
        scope.$digest();
        groups = element.find('.panel');
        expect(groups.length).toEqual(2);

        scope.groups.splice(0,1);
        scope.$digest();
        groups = element.find('.panel');
        expect(groups.length).toEqual(1);
      });
    });

    describe('is-open attribute', function() {
      beforeEach(function () {
        var tpl =
          '<accordion>' +
            '<accordion-group heading="title 1" is-open="open.first">Content 1</accordion-group>' +
            '<accordion-group heading="title 2" is-open="open.second">Content 2</accordion-group>' +
            '</accordion>';
        element = angular.element(tpl);
        scope.open = { first: false, second: true };
        $compile(element)(scope);
        scope.$digest();
        groups = element.find('.panel');
      });

      it('should open the panel with isOpen set to true', function () {
        expect(findGroupBody(0).scope().isOpen).toBe(false);
        expect(findGroupBody(1).scope().isOpen).toBe(true);
      });

      it('should toggle variable on element click', function() {
        findGroupLink(0).click();
        scope.$digest();
        expect(scope.open.first).toBe(true);

        findGroupLink(0).click();
        scope.$digest();
        expect(scope.open.second).toBe(false);
      });
    });

    describe('is-open attribute with dynamic content', function() {
      beforeEach(function () {
        var tpl =
          '<accordion>' +
            '<accordion-group heading="title 1" is-open="open1"><div ng-repeat="item in items">{{item}}</div></accordion-group>' +
            '<accordion-group heading="title 2" is-open="open2">Static content</accordion-group>' +
            '</accordion>';
        element = angular.element(tpl);
        scope.items = ['Item 1', 'Item 2', 'Item 3'];
        scope.open1 = true;
        scope.open2 = false;
        angular.element(document.body).append(element);
        $compile(element)(scope);
        scope.$digest();
        groups = element.find('.panel');
      });

      afterEach(function() {
        element.remove();
      });

      it('should have visible panel body when the group with isOpen set to true', function () {
        expect(findGroupBody(0)[0].clientHeight).not.toBe(0);
        expect(findGroupBody(1)[0].clientHeight).toBe(0);
      });
    });

    describe('is-open attribute with dynamic groups', function () {
      beforeEach(function () {
        var tpl =
          '<accordion>' +
            '<accordion-group ng-repeat="group in groups" heading="{{group.name}}" is-open="group.open">{{group.content}}</accordion-group>' +
          '</accordion>';
        element = angular.element(tpl);
        scope.groups = [
          {name: 'title 1', content: 'Content 1', open: false},
          {name: 'title 2', content: 'Content 2', open: true}
        ];
        $compile(element)(scope);
        scope.$digest();

        groups = element.find('.panel');
      });

      it('should have visible group body when the group with isOpen set to true', function () {
        expect(findGroupBody(0).scope().isOpen).toBe(false);
        expect(findGroupBody(1).scope().isOpen).toBe(true);
      });

      it('should toggle element on click', function() {
        findGroupLink(0).click();
        scope.$digest();
        expect(findGroupBody(0).scope().isOpen).toBe(true);
        expect(scope.groups[0].open).toBe(true);

        findGroupLink(0).click();
        scope.$digest();
        expect(findGroupBody(0).scope().isOpen).toBe(false);
        expect(scope.groups[0].open).toBe(false);
      });
    });

    describe('`is-disabled` attribute', function() {
      var groupBody;
      beforeEach(function () {
        var tpl =
          '<accordion>' +
            '<accordion-group heading="title 1" is-disabled="disabled">Content 1</accordion-group>' +
            '</accordion>';
        element = angular.element(tpl);
        scope.disabled = true;
        $compile(element)(scope);
        scope.$digest();
        groups = element.find('.panel');
        groupBody = findGroupBody(0);
      });

      it('should open the panel with isOpen set to true', function () {
        expect(groupBody.scope().isOpen).toBeFalsy();
      });

      it('should not toggle if disabled', function() {
        findGroupLink(0).click();
        scope.$digest();
        expect(groupBody.scope().isOpen).toBeFalsy();
      });

      it('should toggle after enabling', function() {
        scope.disabled = false;
        scope.$digest();
        expect(groupBody.scope().isOpen).toBeFalsy();

        findGroupLink(0).click();
        scope.$digest();
        expect(groupBody.scope().isOpen).toBeTruthy();
      });
    });

    describe('accordion-heading element', function() {
      beforeEach(function() {
        var tpl =
          '<accordion ng-init="a = [1,2,3]">' +
            '<accordion-group heading="I get overridden">' +
              '<accordion-heading>Heading Element <span ng-repeat="x in a">{{x}}</span> </accordion-heading>' +
              'Body' +
            '</accordion-group>' +
          '</accordion>';
        element = $compile(tpl)(scope);
        scope.$digest();
        groups = element.find('.panel');
      });
      it('transcludes the <accordion-heading> content into the heading link', function() {
        expect(findGroupLink(0).text()).toBe('Heading Element 123 ');
      });
      it('attaches the same scope to the transcluded heading and body', function() {
        expect(findGroupLink(0).find('span').scope().$id).toBe(findGroupBody(0).find('span').scope().$id);
      });

    });

    describe('accordion-heading attribute', function() {
      beforeEach(function() {
        var tpl =
          '<accordion ng-init="a = [1,2,3]">' +
            '<accordion-group heading="I get overridden">' +
              '<div accordion-heading>Heading Element <span ng-repeat="x in a">{{x}}</span> </div>' +
              'Body' +
            '</accordion-group>' +
          '</accordion>';
        element = $compile(tpl)(scope);
        scope.$digest();
        groups = element.find('.panel');
      });
      it('transcludes the <accordion-heading> content into the heading link', function() {
        expect(findGroupLink(0).text()).toBe('Heading Element 123 ');
      });
      it('attaches the same scope to the transcluded heading and body', function() {
        expect(findGroupLink(0).find('span').scope().$id).toBe(findGroupBody(0).find('span').scope().$id);
      });

    });

    describe('accordion-heading, with repeating accordion-groups', function() {
      it('should clone the accordion-heading for each group', function() {
        element = $compile('<accordion><accordion-group ng-repeat="x in [1,2,3]"><accordion-heading>{{x}}</accordion-heading></accordion-group></accordion>')(scope);
        scope.$digest();
        groups = element.find('.panel');
        expect(groups.length).toBe(3);
        expect(findGroupLink(0).text()).toBe('1');
        expect(findGroupLink(1).text()).toBe('2');
        expect(findGroupLink(2).text()).toBe('3');
      });
    });


    describe('accordion-heading attribute, with repeating accordion-groups', function() {
      it('should clone the accordion-heading for each group', function() {
        element = $compile('<accordion><accordion-group ng-repeat="x in [1,2,3]"><div accordion-heading>{{x}}</div></accordion-group></accordion>')(scope);
        scope.$digest();
        groups = element.find('.panel');
        expect(groups.length).toBe(3);
        expect(findGroupLink(0).text()).toBe('1');
        expect(findGroupLink(1).text()).toBe('2');
        expect(findGroupLink(2).text()).toBe('3');
      });
    });

  });
});
