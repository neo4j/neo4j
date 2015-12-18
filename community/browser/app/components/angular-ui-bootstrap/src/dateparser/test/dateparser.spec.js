describe('date parser', function () {
  var dateParser;

  beforeEach(module('ui.bootstrap.dateparser'));
  beforeEach(inject(function (_dateParser_) {
    dateParser = _dateParser_;
  }));

  function expectParse(input, format, date) {
    expect(dateParser.parse(input, format)).toEqual(date);
  }

  describe('wih custom formats', function() {
    it('should work correctly for `dd`, `MM`, `yyyy`', function() {
      expectParse('17.11.2013', 'dd.MM.yyyy', new Date(2013, 10, 17, 0));
      expectParse('31.12.2013', 'dd.MM.yyyy', new Date(2013, 11, 31, 0));
      expectParse('08-03-1991', 'dd-MM-yyyy', new Date(1991, 2, 8, 0));
      expectParse('03/05/1980', 'MM/dd/yyyy', new Date(1980, 2, 5, 0));
      expectParse('10.01/1983', 'dd.MM/yyyy', new Date(1983, 0, 10, 0));
      expectParse('11-09-1980', 'MM-dd-yyyy', new Date(1980, 10, 9, 0));
      expectParse('2011/02/05', 'yyyy/MM/dd', new Date(2011, 1, 5, 0));
    });

    it('should work correctly for `yy`', function() {
      expectParse('17.11.13', 'dd.MM.yy', new Date(2013, 10, 17, 0));
      expectParse('02-05-11', 'dd-MM-yy', new Date(2011, 4, 2, 0));
      expectParse('02/05/80', 'MM/dd/yy', new Date(2080, 1, 5, 0));
      expectParse('55/02/05', 'yy/MM/dd', new Date(2055, 1, 5, 0));
      expectParse('11-08-13', 'dd-MM-yy', new Date(2013, 7, 11, 0));
    });

    it('should work correctly for `M`', function() {
      expectParse('8/11/2013', 'M/dd/yyyy', new Date(2013, 7, 11, 0));
      expectParse('07.11.05', 'dd.M.yy', new Date(2005, 10, 7, 0));
      expectParse('02-5-11', 'dd-M-yy', new Date(2011, 4, 2, 0));
      expectParse('2/05/1980', 'M/dd/yyyy', new Date(1980, 1, 5, 0));
      expectParse('1955/2/05', 'yyyy/M/dd', new Date(1955, 1, 5, 0));
      expectParse('02-5-11', 'dd-M-yy', new Date(2011, 4, 2, 0));
    });

    it('should work correctly for `MMM`', function() {
      expectParse('30.Sep.10', 'dd.MMM.yy', new Date(2010, 8, 30, 0));
      expectParse('02-May-11', 'dd-MMM-yy', new Date(2011, 4, 2, 0));
      expectParse('Feb/05/1980', 'MMM/dd/yyyy', new Date(1980, 1, 5, 0));
      expectParse('1955/Feb/05', 'yyyy/MMM/dd', new Date(1955, 1, 5, 0));
    });

    it('should work correctly for `MMMM`', function() {
      expectParse('17.November.13', 'dd.MMMM.yy', new Date(2013, 10, 17, 0));
      expectParse('05-March-1980', 'dd-MMMM-yyyy', new Date(1980, 2, 5, 0));
      expectParse('February/05/1980', 'MMMM/dd/yyyy', new Date(1980, 1, 5, 0));
      expectParse('1949/December/20', 'yyyy/MMMM/dd', new Date(1949, 11, 20, 0));
    });

    it('should work correctly for `d`', function() {
      expectParse('17.November.13', 'd.MMMM.yy', new Date(2013, 10, 17, 0));
      expectParse('8-March-1991', 'd-MMMM-yyyy', new Date(1991, 2, 8, 0));
      expectParse('February/5/1980', 'MMMM/d/yyyy', new Date(1980, 1, 5, 0));
      expectParse('1955/February/5', 'yyyy/MMMM/d', new Date(1955, 1, 5, 0));
      expectParse('11-08-13', 'd-MM-yy', new Date(2013, 7, 11, 0));
    });
  });

  describe('wih predefined formats', function() {
    it('should work correctly for `shortDate`', function() {
      expectParse('9/3/10', 'shortDate', new Date(2010, 8, 3, 0));
    });

    it('should work correctly for `mediumDate`', function() {
      expectParse('Sep 3, 2010', 'mediumDate', new Date(2010, 8, 3, 0));
    });

    it('should work correctly for `longDate`', function() {
      expectParse('September 3, 2010', 'longDate', new Date(2010, 8, 3, 0));
    });

    it('should work correctly for `fullDate`', function() {
      expectParse('Friday, September 3, 2010', 'fullDate', new Date(2010, 8, 3, 0));
    });
  });

  describe('with edge case', function() {
    it('should not work for invalid number of days in February', function() {
      expect(dateParser.parse('29.02.2013', 'dd.MM.yyyy')).toBeUndefined();
    });

    it('should work for 29 days in February for leap years', function() {
      expectParse('29.02.2000', 'dd.MM.yyyy', new Date(2000, 1, 29, 0));
    });

    it('should not work for 31 days for some months', function() {
      expect(dateParser.parse('31-04-2013', 'dd-MM-yyyy')).toBeUndefined();
      expect(dateParser.parse('November 31, 2013', 'MMMM d, yyyy')).toBeUndefined();
    });
  });

  it('should not parse non-string inputs', function() {
    expect(dateParser.parse(123456, 'dd.MM.yyyy')).toBe(123456);
    var date = new Date();
    expect(dateParser.parse(date, 'dd.MM.yyyy')).toBe(date);
  });

  it('should not parse if no format is specified', function() {
    expect(dateParser.parse('21.08.1951', '')).toBe('21.08.1951');
  });
});
