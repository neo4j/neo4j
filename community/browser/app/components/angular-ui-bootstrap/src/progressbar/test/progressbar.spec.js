describe('progressbar directive with no binding', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.progressbar'));
  beforeEach(module('template/progressbar/progress.html', 'template/progressbar/bar.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    element = $compile('<progress percent="22" animate="false"></progress>')($rootScope);
    $rootScope.$digest();
  }));

  it('has a "progress" css class', function() {
    expect(element.hasClass('progress')).toBe(true);
  });

  it('contains one child element with "bar" css class', function() {
    expect(element.children().length).toBe(1);
    expect(element.children().eq(0).hasClass('bar')).toBe(true);
  });

  it('has a "bar" element with expected width', function() {
    expect(element.children().eq(0).css('width')).toBe('22%');
  });
});

describe('progressbar directive with data-binding', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.progressbar'));
  beforeEach(module('template/progressbar/progress.html', 'template/progressbar/bar.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.percent = 33;
    element = $compile('<progress percent="percent" animate="false"></progress>')($rootScope);
    $rootScope.$digest();
  }));

  it('has a "progress" css class', function() {
    expect(element.hasClass('progress')).toBe(true);
  });

  it('contains one child element with "bar" css class', function() {
    expect(element.children().length).toBe(1);
    expect(element.children().eq(0).hasClass('bar')).toBe(true);
  });

  it('has a "bar" element with expected width', function() {
    expect(element.children().eq(0).css('width')).toBe('33%');
  });

  it('changes width when bind value changes', function() {
    $rootScope.percent = 55;
    $rootScope.$digest();
    expect(element.children().length).toBe(1);
    expect(element.children().eq(0).css('width')).toBe('55%');
    expect(element.children().eq(0).hasClass('bar')).toBe(true);

    $rootScope.percent += 11;
    $rootScope.$digest();
    expect(element.children().eq(0).css('width')).toBe('66%');

    $rootScope.percent = 0;
    $rootScope.$digest();
    expect(element.children().eq(0).css('width')).toBe('0%');
  });

  it('can handle correctly objects value && class', function() {
    $rootScope.percent = {
      value: 45,
      type: 'warning'
    };
    $rootScope.$digest();

    expect(element.children().length).toBe(1);
    expect(element.hasClass('progress')).toBe(true);

    var barElement = element.children().eq(0);
    expect(barElement.css('width')).toBe('45%');
    expect(barElement.hasClass('bar')).toBe(true);
    expect(barElement.hasClass('bar-warning')).toBe(true);
  });

});

describe('stacked progressbar directive', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.progressbar'));
  beforeEach(module('template/progressbar/progress.html', 'template/progressbar/bar.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.stacked = [12, 22, 33];
    element = $compile('<progress percent="stacked" animate="false"></progress>')($rootScope);
    $rootScope.$digest();
  }));

  it('has a "progress" css class', function() {
    expect(element.hasClass('progress')).toBe(true);
  });

  it('contains tree child elements with "bar" css class each', function() {
    expect(element.children().length).toBe(3);
    expect(element.children().eq(0).hasClass('bar')).toBe(true);
    expect(element.children().eq(1).hasClass('bar')).toBe(true);
    expect(element.children().eq(2).hasClass('bar')).toBe(true);
  });

  it('has a elements with expected width', function() {
    expect(element.children().eq(0).css('width')).toBe('12%');
    expect(element.children().eq(1).css('width')).toBe('22%');
    expect(element.children().eq(2).css('width')).toBe('33%');
  });

  it('changes width when bind value changes', function() {
    $rootScope.stacked[1] = 35;
    $rootScope.$digest();

    expect(element.children().length).toBe(3);
    expect(element.children().eq(0).css('width')).toBe('12%');
    expect(element.children().eq(1).css('width')).toBe('35%');
    expect(element.children().eq(2).css('width')).toBe('33%');
  });

  it('can remove bars', function() {
    $rootScope.stacked.pop();
    $rootScope.$digest();

    expect(element.children().length).toBe(2);

    expect(element.children().eq(0).css('width')).toBe('12%');
    expect(element.children().eq(1).css('width')).toBe('22%');
  });

  it('can handle correctly object changes', function() {
    $rootScope.stacked[1] = {
      value: 29,
      type: 'danger'
    };
    $rootScope.$digest();

    expect(element.children().length).toBe(3);

    var barElement;

    barElement = element.children().eq(0);
    expect(barElement.css('width')).toBe('12%');
    expect(barElement.hasClass('bar')).toBe(true);
    expect(barElement.hasClass('bar-danger')).toBe(false);

    barElement = element.children().eq(1);
    expect(barElement.css('width')).toBe('29%');
    expect(barElement.hasClass('bar')).toBe(true);
    expect(barElement.hasClass('bar-danger')).toBe(true);

    barElement = element.children().eq(2);
    expect(barElement.css('width')).toBe('33%');
    expect(barElement.hasClass('bar')).toBe(true);
    expect(barElement.hasClass('bar-danger')).toBe(false);
  });

  it('can handle mixed objects with custom classes', function() {
    $rootScope.stacked = [
      { value: 15, type: 'info' },
      11, 
      { value: 9, type: 'danger' },
      { value: 22, type: 'warning' },
      5
    ];
    $rootScope.$digest();

    expect(element.children().length).toBe(5);

    var barElement;

    barElement = element.children().eq(0);
    expect(barElement.css('width')).toBe('15%');
    expect(barElement.hasClass('bar-info')).toBe(true);

    barElement = element.children().eq(1);
    expect(barElement.css('width')).toBe('11%');
    expect(barElement.hasClass('bar-info')).toBe(false);

    barElement = element.children().eq(2);
    expect(barElement.css('width')).toBe('9%');
    expect(barElement.hasClass('bar-danger')).toBe(true);

    barElement = element.children().eq(3);
    expect(barElement.css('width')).toBe('22%');
    expect(barElement.hasClass('bar-warning')).toBe(true);

    barElement = element.children().eq(4);
    expect(barElement.css('width')).toBe('5%');
    expect(barElement.hasClass('bar-warning')).toBe(false);
  });

});

describe('stacked progressbar directive handlers', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.progressbar'));
  beforeEach(module('template/progressbar/progress.html', 'template/progressbar/bar.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.stacked = [20, 30, 40]; // total: 90
    $rootScope.fullHandler = jasmine.createSpy('fullHandler');
    $rootScope.emptyHandler = jasmine.createSpy('emptyHandler');
    element = $compile('<progress percent="stacked" on-full="fullHandler()" on-empty="emptyHandler()" animate="false"></progress>')($rootScope);
    $rootScope.$digest();
  }));


  it("should not fire at start", function () {
    expect($rootScope.fullHandler).not.toHaveBeenCalled();
    expect($rootScope.emptyHandler).not.toHaveBeenCalled();
  });

  it("should fire callback when full", function () {
    $rootScope.stacked.push(10); // total: 100
    $rootScope.$digest();

    expect($rootScope.fullHandler).toHaveBeenCalled();
    expect($rootScope.emptyHandler).not.toHaveBeenCalled();
  });

  it("should fire callback when empties", function () {
    $rootScope.stacked = 0;
    $rootScope.$digest();

    expect($rootScope.fullHandler).not.toHaveBeenCalled();
    expect($rootScope.emptyHandler).toHaveBeenCalled();
  });

});

describe('stacked progressbar directive with auto-types', function () {
  var $rootScope, element;
  var config = {};
  beforeEach(module('ui.bootstrap.progressbar'));
  beforeEach(module('template/progressbar/progress.html', 'template/progressbar/bar.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_, progressConfig) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.stacked = [12, 22, {value: 33}, {value: 5}, 11];
    element = $compile('<progress percent="stacked" animate="false" auto-type="true"></progress>')($rootScope);
    $rootScope.$digest();

    angular.extend(config, progressConfig);
  }));
  afterEach(inject(function(progressConfig) {
    // return it to the original state
    angular.extend(progressConfig, config);
  }));

  it('has a "progress" css class', function() {
    expect(element.hasClass('progress')).toBe(true);
  });

  it('contains tree child elements with "bar" css class each', function() {
    expect(element.children().length).toBe(5);
    for (var i = 0; i < 5; i++) {
      expect(element.children().eq(i).hasClass('bar')).toBe(true);
    }
  });

  it('has elements with expected width', function() {
    expect(element.children().eq(0).css('width')).toBe('12%');
    expect(element.children().eq(1).css('width')).toBe('22%');
    expect(element.children().eq(2).css('width')).toBe('33%');
    expect(element.children().eq(3).css('width')).toBe('5%');
    expect(element.children().eq(4).css('width')).toBe('11%');
  });

  it('has elements with automatic types', function() {
    var stackedTypes = config.stackedTypes;

    for (var i = 0; i < stackedTypes.length; i++) {
      expect(element.children().eq(i).hasClass('bar-' + stackedTypes[i])).toBe(true);
    }
  });

  it('ignore automatic type if one is specified', function() {
    $rootScope.stacked[1] = {
      value: 18,
      type: 'something'
    };
    $rootScope.$digest();

    var stackedTypes = config.stackedTypes;

    var bar = element.children().eq(1);
    expect(bar.css('width')).toBe('18%');
    expect(bar.hasClass('bar-' + stackedTypes[1])).toBe(false);
    expect(bar.hasClass('bar-something')).toBe(true);
  });


  it('can provide automatic classes to be applied', function() {
    $rootScope.stacked[1] = {
      value: 18,
      type: 'something'
    };
    $rootScope.$digest();

    var stackedTypes = config.stackedTypes;

    var bar = element.children().eq(1);
    expect(bar.css('width')).toBe('18%');
    expect(bar.hasClass('bar-' + stackedTypes[1])).toBe(false);
    expect(bar.hasClass('bar-something')).toBe(true);
  });

  it('can bypass default configuration for stacked classes from attribute', function() {
    element = $compile('<progress percent="stacked" stacked-types="\'danger\', \'warning\', \'success\'" auto-type="true" animate="false"></progress>')($rootScope);
    $rootScope.$digest();

    var stackedTypes = config.stackedTypes;

    expect(element.children().eq(0).hasClass('bar-danger')).toBe(true);
    expect(element.children().eq(0).hasClass('bar-' + stackedTypes[0])).toBe(false);

    expect(element.children().eq(1).hasClass('bar-warning')).toBe(true);
    expect(element.children().eq(2).hasClass('bar-success')).toBe(true);
  });

});