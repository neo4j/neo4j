describe('progressbar directive', function () {
  var $rootScope, $compile, element;
  beforeEach(module('ui.bootstrap.progressbar'));
  beforeEach(module('template/progressbar/progressbar.html', 'template/progressbar/progress.html', 'template/progressbar/bar.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.value = 22;
    element = $compile('<progressbar animate="false" value="value">{{value}} %</progressbar>')($rootScope);
    $rootScope.$digest();
  }));

  var BAR_CLASS = 'progress-bar';

  function getBar(i) {
    return element.children().eq(i);
  }

  it('has a "progress" css class', function() {
    expect(element).toHaveClass('progress');
  });

  it('contains one child element with "bar" css class', function() {
    expect(element.children().length).toBe(1);
    expect(getBar(0)).toHaveClass(BAR_CLASS);
  });

  it('has a "bar" element with expected width', function() {
    expect(getBar(0).css('width')).toBe('22%');
  });

  it('has the appropriate aria markup', function() {
    var bar = getBar(0);
    expect(bar.attr('role')).toBe('progressbar');
    expect(bar.attr('aria-valuemin')).toBe('0');
    expect(bar.attr('aria-valuemax')).toBe('100');
    expect(bar.attr('aria-valuenow')).toBe('22');
    expect(bar.attr('aria-valuetext')).toBe('22%');
  });

  it('transcludes "bar" text', function() {
    expect(getBar(0).text()).toBe('22 %');
  });

  it('it should be possible to add additional classes', function () {
    element = $compile('<progress class="progress-striped active" max="200"><bar class="pizza"></bar></progress>')($rootScope);
    $rootScope.$digest();

    expect(element).toHaveClass('progress-striped');
    expect(element).toHaveClass('active');

    expect(getBar(0)).toHaveClass('pizza');
  });

  it('adjusts the "bar" width and aria when value changes', function() {
      $rootScope.value = 60;
      $rootScope.$digest();

      var bar = getBar(0);
      expect(bar.css('width')).toBe('60%');

      expect(bar.attr('aria-valuemin')).toBe('0');
      expect(bar.attr('aria-valuemax')).toBe('100');
      expect(bar.attr('aria-valuenow')).toBe('60');
      expect(bar.attr('aria-valuetext')).toBe('60%');
    });

  it('allows fractional "bar" width values, rounded to two places', function () {
    $rootScope.value = 5.625;
    $rootScope.$digest();
    expect(getBar(0).css('width')).toBe('5.63%');

    $rootScope.value = 1.3;
    $rootScope.$digest();
    expect(getBar(0).css('width')).toBe('1.3%');
  });

  it('does not include decimals in aria values', function () {
    $rootScope.value = 50.34;
    $rootScope.$digest();

    var bar = getBar(0);
    expect(bar.css('width')).toBe('50.34%');
    expect(bar.attr('aria-valuetext')).toBe('50%');
  });

  describe('"max" attribute', function () {
    beforeEach(inject(function() {
      $rootScope.max = 200;
      element = $compile('<progressbar max="max" animate="false" value="value">{{value}}/{{max}}</progressbar>')($rootScope);
      $rootScope.$digest();
    }));

    it('has the appropriate aria markup', function() {
      expect(getBar(0).attr('aria-valuemax')).toBe('200');
    });

    it('adjusts the "bar" width', function() {
      expect(element.children().eq(0).css('width')).toBe('11%');
    });

    it('adjusts the "bar" width when value changes', function() {
      $rootScope.value = 60;
      $rootScope.$digest();
      expect(getBar(0).css('width')).toBe('30%');

      $rootScope.value += 12;
      $rootScope.$digest();
      expect(getBar(0).css('width')).toBe('36%');

      $rootScope.value = 0;
      $rootScope.$digest();
      expect(getBar(0).css('width')).toBe('0%');
    });

    it('transcludes "bar" text', function() {
      expect(getBar(0).text()).toBe('22/200');
    });
  });

  describe('"type" attribute', function () {
    beforeEach(inject(function() {
      $rootScope.type = 'success';
      element = $compile('<progressbar value="value" type="{{type}}"></progressbar>')($rootScope);
      $rootScope.$digest();
    }));

    it('should use correct classes', function() {
      expect(getBar(0)).toHaveClass(BAR_CLASS);
      expect(getBar(0)).toHaveClass(BAR_CLASS + '-success');
    });

    it('should change classes if type changed', function() {
      $rootScope.type = 'warning';
      $rootScope.value += 1;
      $rootScope.$digest();

      var barEl = getBar(0);
      expect(barEl).toHaveClass(BAR_CLASS);
      expect(barEl).not.toHaveClass(BAR_CLASS + '-success');
      expect(barEl).toHaveClass(BAR_CLASS + '-warning');
    });
  });

  describe('stacked', function () {
    beforeEach(inject(function() {
      $rootScope.objects = [
        { value: 10, type: 'success' },
        { value: 50, type: 'warning' },
        { value: 20 }
      ];
      element = $compile('<progress animate="false"><bar ng-repeat="o in objects" value="o.value" type="{{o.type}}">{{o.value}}</bar></progress>')($rootScope);
      $rootScope.$digest();
    }));

    it('contains the right number of bars', function() {
      expect(element.children().length).toBe(3);
      for (var i = 0; i < 3; i++) {
        expect(getBar(i)).toHaveClass(BAR_CLASS);
      }
    });

    it('renders each bar with the appropriate width', function() {
      expect(getBar(0).css('width')).toBe('10%');
      expect(getBar(1).css('width')).toBe('50%');
      expect(getBar(2).css('width')).toBe('20%');
    });

    it('uses correct classes', function() {
      expect(getBar(0)).toHaveClass(BAR_CLASS + '-success');
      expect(getBar(0)).not.toHaveClass(BAR_CLASS + '-warning');

      expect(getBar(1)).not.toHaveClass(BAR_CLASS + '-success');
      expect(getBar(1)).toHaveClass(BAR_CLASS + '-warning');

      expect(getBar(2)).not.toHaveClass(BAR_CLASS + '-success');
      expect(getBar(2)).not.toHaveClass(BAR_CLASS + '-warning');
    });

    it('should change classes if type changed', function() {
      $rootScope.objects = [
        { value: 20, type: 'warning' },
        { value: 50 },
        { value: 30, type: 'info' }
      ];
      $rootScope.$digest();

      expect(getBar(0)).not.toHaveClass(BAR_CLASS + '-success');
      expect(getBar(0)).toHaveClass(BAR_CLASS + '-warning');

      expect(getBar(1)).not.toHaveClass(BAR_CLASS + '-success');
      expect(getBar(1)).not.toHaveClass(BAR_CLASS + '-warning');

      expect(getBar(2)).toHaveClass(BAR_CLASS + '-info');
      expect(getBar(2)).not.toHaveClass(BAR_CLASS + '-success');
      expect(getBar(2)).not.toHaveClass(BAR_CLASS + '-warning');
    });

    it('should change classes if type changed', function() {
      $rootScope.objects = [
        { value: 70, type: 'info' }
      ];
      $rootScope.$digest();

      expect(element.children().length).toBe(1);

      expect(getBar(0)).toHaveClass(BAR_CLASS + '-info');
      expect(getBar(0)).not.toHaveClass(BAR_CLASS + '-success');
      expect(getBar(0)).not.toHaveClass(BAR_CLASS + '-warning');
    });
  });
});