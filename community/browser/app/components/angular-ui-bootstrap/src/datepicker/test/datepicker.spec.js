describe('datepicker directive', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.datepicker'));
  beforeEach(module('template/datepicker/datepicker.html'));
  beforeEach(module('template/datepicker/popup.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.date = new Date("September 30, 2010 15:30:00");
    element = $compile('<datepicker ng-model="$parent.date"></datepicker>')($rootScope);
    $rootScope.$digest();
  }));

  function getTitle() {
    return element.find('th').eq(1).find('button').first().text();
  }

  function clickTitleButton(times) {
    var el = element.find('th').eq(1).find('button');
    for (var i = 0, n = times || 1; i < n; i++) {
      el.click();
    }
  }

  function clickPreviousButton(times) {
    var el = element.find('th').eq(0).find('button').eq(0);
    for (var i = 0, n = times || 1; i < n; i++) {
      el.click();
    }
  }

  function clickNextButton(times) {
    var el = element.find('th').eq(2).find('button').eq(0);

    for (var i = 0, n = times || 1; i < n; i++) {
      el.click();
    }
  }

  function getLabelsRow() {
    return element.find('thead').find('tr').eq(1);
  }

  function getLabels() {
    var els = getLabelsRow().find('th');

    var labels = [];
    for (var i = 1, n = els.length; i < n; i++) {
      labels.push( els.eq(i).text() );
    }
    return labels;
  }

  function getWeeks() {
    var rows = element.find('tbody').find('tr');
    var weeks = [];
    for (var i = 0, n = rows.length; i < n; i++) {
      weeks.push( rows.eq(i).find('td').eq(0).first().text() );
    }
    return weeks;
  }

  function getOptions() {
    var tr = element.find('tbody').find('tr');
    var rows = [];

    for (var j = 0, numRows = tr.length; j < numRows; j++) {
      var cols = tr.eq(j).find('td'), days = [];
      for (var i = 1, n = cols.length; i < n; i++) {
        days.push( cols.eq(i).find('button').text() );
      }
      rows.push(days);
    }
    return rows;
  }

  function _getOptionEl(rowIndex, colIndex) {
    return element.find('tbody').find('tr').eq(rowIndex).find('td').eq(colIndex + 1);
  }

  function clickOption(rowIndex, colIndex) {
    _getOptionEl(rowIndex, colIndex).find('button').click();
  }

  function isDisabledOption(rowIndex, colIndex) {
    return _getOptionEl(rowIndex, colIndex).find('button').prop('disabled');
  }

  function getAllOptionsEl() {
    var tr = element.find('tbody').find('tr'), rows = [];
    for (var i = 0; i < tr.length; i++) {
      var td = tr.eq(i).find('td'), cols = [];
      for (var j = 0; j < td.length; j++) {
        cols.push( td.eq(j + 1) );
      }
      rows.push(cols);
    }
    return rows;
  }

  function expectSelectedElement( row, col ) {
    var options = getAllOptionsEl();
    for (var i = 0, n = options.length; i < n; i ++) {
      var optionsRow = options[i];
      for (var j = 0; j < optionsRow.length; j ++) {
        expect(optionsRow[j].find('button').hasClass('btn-info')).toBe( i === row && j === col );
      }
    }
  }

  it('is a `<table>` element', function() {
    expect(element.prop('tagName')).toBe('TABLE');
    expect(element.find('thead').find('tr').length).toBe(2);
  });

  it('shows the correct title', function() {
    expect(getTitle()).toBe('September 2010');
  });

  it('shows the label row & the correct day labels', function() {
    expect(getLabelsRow().css('display')).not.toBe('none');
    expect(getLabels()).toEqual(['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']);
  });

  it('renders the calendar days correctly', function() {
    expect(getOptions()).toEqual([
      ['29', '30', '31', '01', '02', '03', '04'],
      ['05', '06', '07', '08', '09', '10', '11'],
      ['12', '13', '14', '15', '16', '17', '18'],
      ['19', '20', '21', '22', '23', '24', '25'],
      ['26', '27', '28', '29', '30', '01', '02']
    ]);
  });

  it('renders the week numbers based on ISO 8601', function() {
    expect(getWeeks()).toEqual(['34', '35', '36', '37', '38']);
  });

  it('value is correct', function() {
    expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));
  });

  it('has `selected` only the correct day', function() {
    expectSelectedElement( 4, 4 );
  });

  it('has no `selected` day when model is cleared', function() {
    $rootScope.date = null;
    $rootScope.$digest();

    expect($rootScope.date).toBe(null);
    expectSelectedElement( null, null );
  });

  it('does not change current view when model is cleared', function() {
    $rootScope.date = null;
    $rootScope.$digest();

    expect($rootScope.date).toBe(null);
    expect(getTitle()).toBe('September 2010');
  });

  it('`disables` visible dates from other months', function() {
    var options = getAllOptionsEl();
    for (var i = 0; i < 5; i ++) {
      for (var j = 0; j < 7; j ++) {
        expect(options[i][j].find('button').find('span').hasClass('muted')).toBe( ((i === 0 && j < 3) || (i === 4 && j > 4)) );
      }
    }
  });

  it('updates the model when a day is clicked', function() {
    clickOption(2, 3);
    expect($rootScope.date).toEqual(new Date('September 15, 2010 15:30:00'));
  });

  it('moves to the previous month & renders correctly when `previous` button is clicked', function() {
    clickPreviousButton();

    expect(getTitle()).toBe('August 2010');
    expect(getLabels()).toEqual(['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']);
    expect(getOptions()).toEqual([
      ['01', '02', '03', '04', '05', '06', '07'],
      ['08', '09', '10', '11', '12', '13', '14'],
      ['15', '16', '17', '18', '19', '20', '21'],
      ['22', '23', '24', '25', '26', '27', '28'],
      ['29', '30', '31', '01', '02', '03', '04']
    ]);

    expectSelectedElement( null, null );
  });

  it('updates the model only when when a day is clicked in the `previous` month', function() {
    clickPreviousButton();
    expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));

    clickOption(2, 3);
    expect($rootScope.date).toEqual(new Date('August 18, 2010 15:30:00'));
  });

  it('moves to the next month & renders correctly when `next` button is clicked', function() {
    clickNextButton();

    expect(getTitle()).toBe('October 2010');
    expect(getLabels()).toEqual(['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']);
    expect(getOptions()).toEqual([
      ['26', '27', '28', '29', '30', '01', '02'],
      ['03', '04', '05', '06', '07', '08', '09'],
      ['10', '11', '12', '13', '14', '15', '16'],
      ['17', '18', '19', '20', '21', '22', '23'],
      ['24', '25', '26', '27', '28', '29', '30'],
      ['31', '01', '02', '03', '04', '05', '06']
    ]);

    expectSelectedElement( 0, 4 );
  });

  it('updates the model only when when a day is clicked in the `next` month', function() {
    clickNextButton();
    expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));

    clickOption(2, 3);
    expect($rootScope.date).toEqual(new Date('October 13, 2010 15:30:00'));
  });

  it('updates the calendar when a day of another month is selected', function() {
    clickOption(4, 5);
    expect($rootScope.date).toEqual(new Date('October 01, 2010 15:30:00'));
    expect(getTitle()).toBe('October 2010');
    expect(getLabels()).toEqual(['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']);
    expect(getOptions()).toEqual([
      ['26', '27', '28', '29', '30', '01', '02'],
      ['03', '04', '05', '06', '07', '08', '09'],
      ['10', '11', '12', '13', '14', '15', '16'],
      ['17', '18', '19', '20', '21', '22', '23'],
      ['24', '25', '26', '27', '28', '29', '30'],
      ['31', '01', '02', '03', '04', '05', '06']
    ]);

    expectSelectedElement( 0, 5 );
  });

  describe('when `model` changes', function () {
    function testCalendar() {
      expect(getTitle()).toBe('November 2005');
      expect(getOptions()).toEqual([
        ['30', '31', '01', '02', '03', '04', '05'],
        ['06', '07', '08', '09', '10', '11', '12'],
        ['13', '14', '15', '16', '17', '18', '19'],
        ['20', '21', '22', '23', '24', '25', '26'],
        ['27', '28', '29', '30', '01', '02', '03']
      ]);

      expectSelectedElement( 1, 1 );
    }

    describe('to a Date object', function() {
      it('updates', function() {
        $rootScope.date = new Date('November 7, 2005 23:30:00');
        $rootScope.$digest();
        testCalendar();
        expect(angular.isDate($rootScope.date)).toBe(true);
      });
    });

    describe('not to a Date object', function() {

      it('to a Number, it updates calendar', function() {
        $rootScope.date = parseInt((new Date('November 7, 2005 23:30:00')).getTime(), 10);
        $rootScope.$digest();
        testCalendar();
        expect(angular.isNumber($rootScope.date)).toBe(true);
      });

      it('to a string that can be parsed by Date, it updates calendar', function() {
        $rootScope.date = 'November 7, 2005 23:30:00';
        $rootScope.$digest();
        testCalendar();
        expect(angular.isString($rootScope.date)).toBe(true);
      });

      it('to a string that cannot be parsed by Date, it gets invalid', function() {
        $rootScope.date = 'pizza';
        $rootScope.$digest();
        expect(element.hasClass('ng-invalid')).toBeTruthy();
        expect(element.hasClass('ng-invalid-date')).toBeTruthy();
        expect($rootScope.date).toBe('pizza');
      });
    });
  });

  it('loops between different modes', function() {
    expect(getTitle()).toBe('September 2010');

    clickTitleButton();
    expect(getTitle()).toBe('2010');

    clickTitleButton();
    expect(getTitle()).toBe('2001 - 2020');

    clickTitleButton();
    expect(getTitle()).toBe('September 2010');
  });

  describe('month selection mode', function () {
    beforeEach(function() {
      clickTitleButton();
    });

    it('shows the year as title', function() {
      expect(getTitle()).toBe('2010');
    });

    it('shows months as options', function() {
      expect(getLabels()).toEqual([]);
      expect(getOptions()).toEqual([
        ['January', 'February', 'March'],
        ['April', 'May', 'June'],
        ['July', 'August', 'September'],
        ['October', 'November', 'December']
      ]);
    });

    it('does not change the model', function() {
      expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));
    });

    it('has `selected` only the correct month', function() {
      expectSelectedElement( 2, 2 );
    });

    it('moves to the previous year when `previous` button is clicked', function() {
      clickPreviousButton();

      expect(getTitle()).toBe('2009');
      expect(getLabels()).toEqual([]);
      expect(getOptions()).toEqual([
        ['January', 'February', 'March'],
        ['April', 'May', 'June'],
        ['July', 'August', 'September'],
        ['October', 'November', 'December']
      ]);

      expectSelectedElement( null, null );
    });

    it('moves to the next year when `next` button is clicked', function() {
      clickNextButton();

      expect(getTitle()).toBe('2011');
      expect(getLabels()).toEqual([]);
      expect(getOptions()).toEqual([
        ['January', 'February', 'March'],
        ['April', 'May', 'June'],
        ['July', 'August', 'September'],
        ['October', 'November', 'December']
      ]);

      expectSelectedElement( null, null );
    });

    it('renders correctly when a month is clicked', function() {
      clickPreviousButton(5);
      expect(getTitle()).toBe('2005');

      clickOption(3, 1);
      expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));
      expect(getTitle()).toBe('November 2005');
      expect(getOptions()).toEqual([
        ['30', '31', '01', '02', '03', '04', '05'],
        ['06', '07', '08', '09', '10', '11', '12'],
        ['13', '14', '15', '16', '17', '18', '19'],
        ['20', '21', '22', '23', '24', '25', '26'],
        ['27', '28', '29', '30', '01', '02', '03']
      ]);

      clickOption(2, 3);
      expect($rootScope.date).toEqual(new Date('November 16, 2005 15:30:00'));
    });
  });

  describe('year selection mode', function () {
    beforeEach(function() {
      clickTitleButton(2);
    });

    it('shows the year range as title', function() {
      expect(getTitle()).toBe('2001 - 2020');
    });

    it('shows years as options', function() {
      expect(getLabels()).toEqual([]);
      expect(getOptions()).toEqual([
        ['2001', '2002', '2003', '2004', '2005'],
        ['2006', '2007', '2008', '2009', '2010'],
        ['2011', '2012', '2013', '2014', '2015'],
        ['2016', '2017', '2018', '2019', '2020']
      ]);
    });

    it('does not change the model', function() {
      expect($rootScope.date).toEqual(new Date('September 30, 2010 15:30:00'));
    });

    it('has `selected` only the selected year', function() {
      expectSelectedElement( 1, 4 );
    });

    it('moves to the previous year set when `previous` button is clicked', function() {
      clickPreviousButton();

      expect(getTitle()).toBe('1981 - 2000');
      expect(getLabels()).toEqual([]);
      expect(getOptions()).toEqual([
        ['1981', '1982', '1983', '1984', '1985'],
        ['1986', '1987', '1988', '1989', '1990'],
        ['1991', '1992', '1993', '1994', '1995'],
        ['1996', '1997', '1998', '1999', '2000']
      ]);
      expectSelectedElement( null, null );
    });

    it('moves to the next year set when `next` button is clicked', function() {
      clickNextButton();

      expect(getTitle()).toBe('2021 - 2040');
      expect(getLabels()).toEqual([]);
      expect(getOptions()).toEqual([
        ['2021', '2022', '2023', '2024', '2025'],
        ['2026', '2027', '2028', '2029', '2030'],
        ['2031', '2032', '2033', '2034', '2035'],
        ['2036', '2037', '2038', '2039', '2040']
      ]);

      expectSelectedElement( null, null );
    });
  });

  describe('attribute `starting-day`', function () {
    beforeEach(function() {
      $rootScope.startingDay = 1;
      element = $compile('<datepicker ng-model="$parent.date" starting-day="startingDay"></datepicker>')($rootScope);
      $rootScope.$digest();
    });

    it('shows the day labels rotated', function() {
      expect(getLabels()).toEqual(['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']);
    });

    it('renders the calendar days correctly', function() {
      expect(getOptions()).toEqual([
        ['30', '31', '01', '02', '03', '04', '05'],
        ['06', '07', '08', '09', '10', '11', '12'],
        ['13', '14', '15', '16', '17', '18', '19'],
        ['20', '21', '22', '23', '24', '25', '26'],
        ['27', '28', '29', '30', '01', '02', '03']
      ]);
    });

    it('renders the week numbers correctly', function() {
      expect(getWeeks()).toEqual(['35', '36', '37', '38', '39']);
    });
  });

  describe('attribute `show-weeks`', function () {
    var weekHeader, weekElement;
    beforeEach(function() {
      $rootScope.showWeeks = false;
      element = $compile('<datepicker ng-model="$parent.date" show-weeks="showWeeks"></datepicker>')($rootScope);
      $rootScope.$digest();

      weekHeader = getLabelsRow().find('th').eq(0);
      weekElement = element.find('tbody').find('tr').eq(1).find('td').eq(0);
    });

    it('hides week numbers based on variable', function() {
      expect(weekHeader.text()).toEqual('#');
      expect(weekHeader.css('display')).toBe('none');
      expect(weekElement.css('display')).toBe('none');
    });

    it('toggles week numbers', function() {
      $rootScope.showWeeks = true;
      $rootScope.$digest();
      expect(weekHeader.text()).toEqual('#');
      expect(weekHeader.css('display')).not.toBe('none');
      expect(weekElement.css('display')).not.toBe('none');

      $rootScope.showWeeks = false;
      $rootScope.$digest();
      expect(weekHeader.text()).toEqual('#');
      expect(weekHeader.css('display')).toBe('none');
      expect(weekElement.css('display')).toBe('none');
    });
  });

  describe('min attribute', function () {
    beforeEach(function() {
      $rootScope.mindate = new Date("September 12, 2010");
      element = $compile('<datepicker ng-model="$parent.date" min="mindate"></datepicker>')($rootScope);
      $rootScope.$digest();
    });

    it('disables appropriate days in current month', function() {
      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(isDisabledOption(i, j)).toBe( (i < 2) );
        }
      }
    });

    it('disables appropriate days when min date changes', function() {
      $rootScope.mindate = new Date("September 5, 2010");
      $rootScope.$digest();
      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(isDisabledOption(i, j)).toBe( (i < 1) );
        }
      }
    });

    it('invalidates when model is a disabled date', function() {
      $rootScope.mindate = new Date("September 5, 2010");
      $rootScope.date = new Date("September 2, 2010");
      $rootScope.$digest();
      expect(element.hasClass('ng-invalid')).toBeTruthy();
      expect(element.hasClass('ng-invalid-date-disabled')).toBeTruthy();
    });

    it('disables all days in previous month', function() {
      clickPreviousButton();
      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(isDisabledOption(i, j)).toBe( true );
        }
      }
    });

    it('disables no days in next month', function() {
      clickNextButton();
      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(isDisabledOption(i, j)).toBe( false );
        }
      }
    });

    it('disables appropriate months in current year', function() {
      clickTitleButton();
      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 3; j ++) {
          expect(isDisabledOption(i, j)).toBe( (i < 2 || (i === 2 && j < 2)) );
        }
      }
    });

    it('disables all months in previous year', function() {
      clickTitleButton();
      clickPreviousButton();
      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 3; j ++) {
          expect(isDisabledOption(i, j)).toBe( true );
        }
      }
    });

    it('disables no months in next year', function() {
      clickTitleButton();
      clickNextButton();
      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 3; j ++) {
          expect(isDisabledOption(i, j)).toBe( false );
        }
      }
    });

    it('enables everything before if it is cleared', function() {
      $rootScope.mindate = null;
      $rootScope.date = new Date("December 20, 1949");
      $rootScope.$digest();

      clickTitleButton();
      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 3; j ++) {
          expect(isDisabledOption(i, j)).toBe( false );
        }
      }
    });

  });

  describe('max attribute', function () {
    beforeEach(function() {
      $rootScope.maxdate = new Date("September 25, 2010");
      element = $compile('<datepicker ng-model="$parent.date" max="maxdate"></datepicker>')($rootScope);
      $rootScope.$digest();
    });

    it('disables appropriate days in current month', function() {
      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(isDisabledOption(i, j)).toBe( (i === 4) );
        }
      }
    });

    it('disables appropriate days when max date changes', function() {
      $rootScope.maxdate = new Date("September 18, 2010");
      $rootScope.$digest();
      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(isDisabledOption(i, j)).toBe( (i > 2) );
        }
      }
    });

    it('invalidates when model is a disabled date', function() {
      $rootScope.maxdate = new Date("September 18, 2010");
      $rootScope.$digest();
      expect(element.hasClass('ng-invalid')).toBeTruthy();
      expect(element.hasClass('ng-invalid-date-disabled')).toBeTruthy();
    });

    it('disables no days in previous month', function() {
      clickPreviousButton();
      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(isDisabledOption(i, j)).toBe( false );
        }
      }
    });

    it('disables all days in next month', function() {
      clickNextButton();
      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(isDisabledOption(i, j)).toBe( true );
        }
      }
    });

    it('disables appropriate months in current year', function() {
      clickTitleButton();
      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 3; j ++) {
          expect(isDisabledOption(i, j)).toBe( (i > 2 || (i === 2 && j > 2)) );
        }
      }
    });

    it('disables no months in previous year', function() {
      clickTitleButton();
      clickPreviousButton();
      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 3; j ++) {
          expect(isDisabledOption(i, j)).toBe( false );
        }
      }
    });

    it('disables all months in next year', function() {
      clickTitleButton();
      clickNextButton();
      for (var i = 0; i < 4; i ++) {
        for (var j = 0; j < 3; j ++) {
          expect(isDisabledOption(i, j)).toBe( true );
        }
      }
    });

    it('enables everything after if it is cleared', function() {
      $rootScope.maxdate = null;
      $rootScope.$digest();
      for (var i = 0; i < 5; i ++) {
        for (var j = 0; j < 7; j ++) {
          expect(isDisabledOption(i, j)).toBe( false );
        }
      }
    });
  });

  describe('date-disabled expression', function () {
    beforeEach(function() {
      $rootScope.dateDisabledHandler = jasmine.createSpy('dateDisabledHandler');
      element = $compile('<datepicker ng-model="$parent.date" date-disabled="dateDisabledHandler(date, mode)"></datepicker>')($rootScope);
      $rootScope.$digest();
    });

    it('executes the dateDisabled expression for each visible day plus one for validation', function() {
      expect($rootScope.dateDisabledHandler.calls.length).toEqual(35 + 1);
    });

    it('executes the dateDisabled expression for each visible month plus one for validation', function() {
      $rootScope.dateDisabledHandler.reset();
      clickTitleButton();
      expect($rootScope.dateDisabledHandler.calls.length).toEqual(12 + 1);
    });

    it('executes the dateDisabled expression for each visible year plus one for validation', function() {
      clickTitleButton();
      $rootScope.dateDisabledHandler.reset();
      clickTitleButton();
      expect($rootScope.dateDisabledHandler.calls.length).toEqual(20 + 1);
    });
  });

  describe('formatting attributes', function () {
    beforeEach(function() {
      element = $compile('<datepicker ng-model="$parent.date" day-format="\'d\'" day-header-format="\'EEEE\'" day-title-format="\'MMMM, yy\'" month-format="\'MMM\'" month-title-format="\'yy\'" year-format="\'yy\'" year-range="10"></datepicker>')($rootScope);
      $rootScope.$digest();
    });

    it('changes the title format in `day` mode', function() {
      expect(getTitle()).toBe('September, 10');
    });

    it('changes the title & months format in `month` mode', function() {
      clickTitleButton();

      expect(getTitle()).toBe('10');
      expect(getOptions()).toEqual([
        ['Jan', 'Feb', 'Mar'],
        ['Apr', 'May', 'Jun'],
        ['Jul', 'Aug', 'Sep'],
        ['Oct', 'Nov', 'Dec']
      ]);
    });

    it('changes the title, year format & range in `year` mode', function() {
      clickTitleButton(2);

      expect(getTitle()).toBe('01 - 10');
      expect(getOptions()).toEqual([
        ['01', '02', '03', '04', '05'],
        ['06', '07', '08', '09', '10']
      ]);
    });

    it('shows day labels', function() {
      expect(getLabels()).toEqual(['Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday']);
    });

    it('changes the day format', function() {
      expect(getOptions()).toEqual([
        ['29', '30', '31', '1', '2', '3', '4'],
        ['5', '6', '7', '8', '9', '10', '11'],
        ['12', '13', '14', '15', '16', '17', '18'],
        ['19', '20', '21', '22', '23', '24', '25'],
        ['26', '27', '28', '29', '30', '1', '2']
      ]);
    });
  });

  describe('setting datepickerConfig', function() {
    var originalConfig = {};
    beforeEach(inject(function(datepickerConfig) {
      angular.extend(originalConfig, datepickerConfig);
      datepickerConfig.startingDay = 6;
      datepickerConfig.showWeeks = false;
      datepickerConfig.dayFormat = 'd';
      datepickerConfig.monthFormat = 'MMM';
      datepickerConfig.yearFormat = 'yy';
      datepickerConfig.yearRange = 10;
      datepickerConfig.dayHeaderFormat = 'EEEE';
      datepickerConfig.dayTitleFormat = 'MMMM, yy';
      datepickerConfig.monthTitleFormat = 'yy';

      element = $compile('<datepicker ng-model="$parent.date"></datepicker>')($rootScope);
      $rootScope.$digest();
    }));
    afterEach(inject(function(datepickerConfig) {
      // return it to the original state
      angular.extend(datepickerConfig, originalConfig);
    }));

    it('changes the title format in `day` mode', function() {
      expect(getTitle()).toBe('September, 10');
    });

    it('changes the title & months format in `month` mode', function() {
      clickTitleButton();

      expect(getTitle()).toBe('10');
      expect(getOptions()).toEqual([
        ['Jan', 'Feb', 'Mar'],
        ['Apr', 'May', 'Jun'],
        ['Jul', 'Aug', 'Sep'],
        ['Oct', 'Nov', 'Dec']
      ]);
    });

    it('changes the title, year format & range in `year` mode', function() {
      clickTitleButton(2);

      expect(getTitle()).toBe('01 - 10');
      expect(getOptions()).toEqual([
        ['01', '02', '03', '04', '05'],
        ['06', '07', '08', '09', '10']
      ]);
    });

    it('changes the `starting-day` & day headers & format', function() {
      expect(getLabels()).toEqual(['Saturday', 'Sunday', 'Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday']);
      expect(getOptions()).toEqual([
        ['28', '29', '30', '31', '1', '2', '3'],
        ['4', '5', '6', '7', '8', '9', '10'],
        ['11', '12', '13', '14', '15', '16', '17'],
        ['18', '19', '20', '21', '22', '23', '24'],
        ['25', '26', '27', '28', '29', '30', '1']
      ]);
    });

    it('changes initial visibility for weeks', function() {
      expect(getLabelsRow().find('th').eq(0).css('display')).toBe('none');
      var tr = element.find('tbody').find('tr');
      for (var i = 0; i < 5; i++) {
        expect(tr.eq(i).find('td').eq(0).css('display')).toBe('none');
      }
    });

  });

  describe('controller', function () {
    var ctrl, $attrs;
    beforeEach(inject(function($controller) {
      $rootScope.dateDisabled = null;
      $attrs = {};
      ctrl = $controller('DatepickerController', { $scope: $rootScope, $attrs: $attrs });
    }));

    describe('modes', function() {
      var currentMode;

      it('to be an array', function() {
        expect(ctrl.modes.length).toBe(3);
      });

      describe('`day`', function() {
        beforeEach(inject(function() {
          currentMode = ctrl.modes[0];
        }));

        it('has the appropriate name', function() {
          expect(currentMode.name).toBe('day');
        });

        it('returns the correct date objects', function() {
          var objs = currentMode.getVisibleDates(new Date('September 1, 2010'), new Date('September 30, 2010')).objects;
          expect(objs.length).toBe(35);
          expect(objs[1].selected).toBeFalsy();
          expect(objs[32].selected).toBeTruthy();
        });

        it('can compare two dates', function() {
          expect(currentMode.compare(new Date('September 30, 2010'), new Date('September 1, 2010'))).toBeGreaterThan(0);
          expect(currentMode.compare(new Date('September 1, 2010'), new Date('September 30, 2010'))).toBeLessThan(0);
          expect(currentMode.compare(new Date('September 30, 2010 15:30:00'), new Date('September 30, 2010 20:30:00'))).toBe(0);
        });
      });

      describe('`month`', function() {
        beforeEach(inject(function() {
          currentMode = ctrl.modes[1];
        }));

        it('has the appropriate name', function() {
          expect(currentMode.name).toBe('month');
        });

        it('returns the correct date objects', function() {
          var objs = currentMode.getVisibleDates(new Date('September 1, 2010'), new Date('September 30, 2010')).objects;
          expect(objs.length).toBe(12);
          expect(objs[1].selected).toBeFalsy();
          expect(objs[8].selected).toBeTruthy();
        });

        it('can compare two dates', function() {
          expect(currentMode.compare(new Date('October 30, 2010'), new Date('September 01, 2010'))).toBeGreaterThan(0);
          expect(currentMode.compare(new Date('September 01, 2010'), new Date('October 30, 2010'))).toBeLessThan(0);
          expect(currentMode.compare(new Date('September 01, 2010'), new Date('September 30, 2010'))).toBe(0);
        });
      });

      describe('`year`', function() {
        beforeEach(inject(function() {
          currentMode = ctrl.modes[2];
        }));

        it('has the appropriate name', function() {
          expect(currentMode.name).toBe('year');
        });

        it('returns the correct date objects', function() {
          var objs = currentMode.getVisibleDates(new Date('September 1, 2010'), new Date('September 01, 2010')).objects;
          expect(objs.length).toBe(20);
          expect(objs[1].selected).toBeFalsy();
          expect(objs[9].selected).toBeTruthy();
        });

        it('can compare two dates', function() {
          expect(currentMode.compare(new Date('September 1, 2011'), new Date('October 30, 2010'))).toBeGreaterThan(0);
          expect(currentMode.compare(new Date('October 30, 2010'), new Date('September 1, 2011'))).toBeLessThan(0);
          expect(currentMode.compare(new Date('November 9, 2010'), new Date('September 30, 2010'))).toBe(0);
        });
      });
    });

    describe('`isDisabled` function', function() {
      var date = new Date("September 30, 2010 15:30:00");

      it('to return false if no limit is set', function() {
        expect(ctrl.isDisabled(date, 0)).toBeFalsy();
      });

      it('to handle correctly the `min` date', function() {
        ctrl.minDate = new Date('October 1, 2010');
        expect(ctrl.isDisabled(date, 0)).toBeTruthy();
        expect(ctrl.isDisabled(date)).toBeTruthy();

        ctrl.minDate = new Date('September 1, 2010');
        expect(ctrl.isDisabled(date, 0)).toBeFalsy();
      });

      it('to handle correctly the `max` date', function() {
        ctrl.maxDate = new Date('October 1, 2010');
        expect(ctrl.isDisabled(date, 0)).toBeFalsy();

        ctrl.maxDate = new Date('September 1, 2010');
        expect(ctrl.isDisabled(date, 0)).toBeTruthy();
        expect(ctrl.isDisabled(date)).toBeTruthy();
      });

      it('to handle correctly the scope `dateDisabled` expression', function() {
        $rootScope.dateDisabled = function() {
          return false;
        };
        $rootScope.$digest();
        expect(ctrl.isDisabled(date, 0)).toBeFalsy();

        $rootScope.dateDisabled = function() {
          return true;
        };
        $rootScope.$digest();
        expect(ctrl.isDisabled(date, 0)).toBeTruthy();
      });
    });
  });

  describe('as popup', function () {
    var divElement, inputEl, dropdownEl, changeInputValueTo, $document;

    function assignElements(wrapElement) {
      inputEl = wrapElement.find('input');
      dropdownEl = wrapElement.find('ul');
      element = dropdownEl.find('table');
    }

    beforeEach(inject(function(_$document_, $sniffer) {
      $document = _$document_;
      $rootScope.date = new Date("September 30, 2010 15:30:00");
      var wrapElement = $compile('<div><input ng-model="date" datepicker-popup><div>')($rootScope);
      $rootScope.$digest();
      assignElements(wrapElement);

      changeInputValueTo = function (el, value) {
        el.val(value);
        el.trigger($sniffer.hasEvent('input') ? 'input' : 'change');
        $rootScope.$digest();
      };
    }));

    it('to display the correct value in input', function() {
      expect(inputEl.val()).toBe('2010-09-30');
    });

    it('does not to display datepicker initially', function() {
      expect(dropdownEl.css('display')).toBe('none');
    });

    it('displays datepicker on input focus', function() {
      inputEl.focus();
      expect(dropdownEl.css('display')).not.toBe('none');
    });

    it('renders the calendar correctly', function() {
      expect(getLabelsRow().css('display')).not.toBe('none');
      expect(getLabels()).toEqual(['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat']);
      expect(getOptions()).toEqual([
        ['29', '30', '31', '01', '02', '03', '04'],
        ['05', '06', '07', '08', '09', '10', '11'],
        ['12', '13', '14', '15', '16', '17', '18'],
        ['19', '20', '21', '22', '23', '24', '25'],
        ['26', '27', '28', '29', '30', '01', '02']
      ]);
    });

    it('updates the input when a day is clicked', function() {
      clickOption(2, 3);
      expect(inputEl.val()).toBe('2010-09-15');
      expect($rootScope.date).toEqual(new Date('September 15, 2010 15:30:00'));
    });

    it('updates the input correctly when model changes', function() {
      $rootScope.date = new Date("January 10, 1983 10:00:00");
      $rootScope.$digest();
      expect(inputEl.val()).toBe('1983-01-10');
    });

    it('closes the dropdown when a day is clicked', function() {
      inputEl.focus();
      expect(dropdownEl.css('display')).not.toBe('none');

      clickOption(2, 3);
      expect(dropdownEl.css('display')).toBe('none');
    });

    it('updates the model when input value changes', function() {
      changeInputValueTo(inputEl, 'March 5, 1980');
      expect($rootScope.date.getFullYear()).toEqual(1980);
      expect($rootScope.date.getMonth()).toEqual(2);
      expect($rootScope.date.getDate()).toEqual(5);
    });

    it('closes when click outside of calendar', function() {
      $document.find('body').click();
      expect(dropdownEl.css('display')).toBe('none');
    });

    describe('toggles programatically by `open` attribute', function () {
      beforeEach(inject(function() {
        $rootScope.open = true;
        var wrapElement = $compile('<div><input ng-model="date" datepicker-popup open="open"><div>')($rootScope);
        $rootScope.$digest();
        assignElements(wrapElement);
      }));

      it('to display initially', function() {
        expect(dropdownEl.css('display')).not.toBe('none');
      });

      it('to close / open from scope variable', function() {
        expect(dropdownEl.css('display')).not.toBe('none');
        $rootScope.open = false;
        $rootScope.$digest();
        expect(dropdownEl.css('display')).toBe('none');

        $rootScope.open = true;
        $rootScope.$digest();
        expect(dropdownEl.css('display')).not.toBe('none');
      });
    });

    describe('custom format', function () {
      beforeEach(inject(function() {
        var wrapElement = $compile('<div><input ng-model="date" datepicker-popup="dd-MMMM-yyyy"><div>')($rootScope);
        $rootScope.$digest();
        assignElements(wrapElement);
      }));

      it('to display the correct value in input', function() {
        expect(inputEl.val()).toBe('30-September-2010');
      });

      it('updates the input when a day is clicked', function() {
        clickOption(2, 3);
        expect(inputEl.val()).toBe('15-September-2010');
        expect($rootScope.date).toEqual(new Date('September 15, 2010 15:30:00'));
      });

      it('updates the input correctly when model changes', function() {
        $rootScope.date = new Date("January 10, 1983 10:00:00");
        $rootScope.$digest();
        expect(inputEl.val()).toBe('10-January-1983');
      });
    });

    describe('use with ng-required directive', function() {
      beforeEach(inject(function() {
        $rootScope.date = '';
        var wrapElement = $compile('<div><input ng-model="date" datepicker-popup ng-required="true"><div>')($rootScope);
        $rootScope.$digest();
        assignElements(wrapElement);
      }));

      it('should be invalid initially', function() {
        expect(inputEl.hasClass('ng-invalid')).toBeTruthy();
      });
      it('should be valid if model has been specified', function() {
        $rootScope.date = new Date();
        $rootScope.$digest();
        expect(inputEl.hasClass('ng-valid')).toBeTruthy();
      });
    });

  });

});

describe('datepicker directive with empty initial state', function () {
  var $rootScope, element;
  beforeEach(module('ui.bootstrap.datepicker'));
  beforeEach(module('template/datepicker/datepicker.html'));
  beforeEach(inject(function(_$compile_, _$rootScope_) {
    $compile = _$compile_;
    $rootScope = _$rootScope_;
    $rootScope.date = null;
    element = $compile('<datepicker ng-model="$parent.date"></datepicker>')($rootScope);
    $rootScope.$digest();
  }));

  it('is a `<table>` element', function() {
    expect(element.prop('tagName')).toBe('TABLE');
    expect(element.find('thead').find('tr').length).toBe(2);
  });

  it('is shows rows with days', function() {
    expect(element.find('tbody').find('tr').length).toBeGreaterThan(3);
  });
});