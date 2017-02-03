/*
 * Copyright (c) 2017 Public Library of Science
 *
 * Permission is hereby granted, free of charge, to any person obtaining a
 * copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation
 * the rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

package org.plos.repo.util;

import java.text.AttributedCharacterIterator;
import java.text.DateFormatSymbols;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class SimpleDateFormatThreadSafe extends SimpleDateFormat {

  private static final long serialVersionUID = 5448371898056188202L;
  ThreadLocal<SimpleDateFormat> localSimpleDateFormat;

  public SimpleDateFormatThreadSafe() {
    super();
    localSimpleDateFormat = new ThreadLocal<SimpleDateFormat>() {
      protected SimpleDateFormat initialValue() {
        return new SimpleDateFormat();
      }
    };
  }

  public SimpleDateFormatThreadSafe(final String pattern) {
    super(pattern);
    localSimpleDateFormat = new ThreadLocal<SimpleDateFormat>() {
      protected SimpleDateFormat initialValue() {
        return new SimpleDateFormat(pattern);
      }
    };
  }

  public SimpleDateFormatThreadSafe(final String pattern, final DateFormatSymbols formatSymbols) {
    super(pattern, formatSymbols);
    localSimpleDateFormat = new ThreadLocal<SimpleDateFormat>() {
      protected SimpleDateFormat initialValue() {
        return new SimpleDateFormat(pattern, formatSymbols);
      }
    };
  }

  public SimpleDateFormatThreadSafe(final String pattern, final Locale locale) {
    super(pattern, locale);
    localSimpleDateFormat = new ThreadLocal<SimpleDateFormat>() {
      protected SimpleDateFormat initialValue() {
        return new SimpleDateFormat(pattern, locale);
      }
    };
  }

  public Object parseObject(String source) throws ParseException {
    return localSimpleDateFormat.get().parseObject(source);
  }

  public String toString() {
    return localSimpleDateFormat.get().toString();
  }

  public Date parse(String source) throws ParseException {
    return localSimpleDateFormat.get().parse(source);
  }

  public Object parseObject(String source, ParsePosition pos) {
    return localSimpleDateFormat.get().parseObject(source, pos);
  }

  public void setCalendar(Calendar newCalendar) {
    localSimpleDateFormat.get().setCalendar(newCalendar);
  }

  public Calendar getCalendar() {
    return localSimpleDateFormat.get().getCalendar();
  }

  public void setNumberFormat(NumberFormat newNumberFormat) {
    localSimpleDateFormat.get().setNumberFormat(newNumberFormat);
  }

  public NumberFormat getNumberFormat() {
    return localSimpleDateFormat.get().getNumberFormat();
  }

  public void setTimeZone(TimeZone zone) {
    localSimpleDateFormat.get().setTimeZone(zone);
  }

  public TimeZone getTimeZone() {
    return localSimpleDateFormat.get().getTimeZone();
  }

  public void setLenient(boolean lenient) {
    localSimpleDateFormat.get().setLenient(lenient);
  }

  public boolean isLenient() {
    return localSimpleDateFormat.get().isLenient();
  }

  public void set2DigitYearStart(Date startDate) {
    localSimpleDateFormat.get().set2DigitYearStart(startDate);
  }

  public Date get2DigitYearStart() {
    return localSimpleDateFormat.get().get2DigitYearStart();
  }

  public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition pos) {
    return localSimpleDateFormat.get().format(date, toAppendTo, pos);
  }

  public AttributedCharacterIterator formatToCharacterIterator(Object obj) {
    return localSimpleDateFormat.get().formatToCharacterIterator(obj);
  }

  public Date parse(String text, ParsePosition pos) {
    return localSimpleDateFormat.get().parse(text, pos);
  }

  public String toPattern() {
    return localSimpleDateFormat.get().toPattern();
  }

  public String toLocalizedPattern() {
    return localSimpleDateFormat.get().toLocalizedPattern();
  }

  public void applyPattern(String pattern) {
    localSimpleDateFormat.get().applyPattern(pattern);
  }

  public void applyLocalizedPattern(String pattern) {
    localSimpleDateFormat.get().applyLocalizedPattern(pattern);
  }

  public DateFormatSymbols getDateFormatSymbols() {
    return localSimpleDateFormat.get().getDateFormatSymbols();
  }

  public void setDateFormatSymbols(DateFormatSymbols newFormatSymbols) {
    localSimpleDateFormat.get().setDateFormatSymbols(newFormatSymbols);
  }

  public Object clone() {
    return localSimpleDateFormat.get().clone();
  }

  public int hashCode() {
    return localSimpleDateFormat.get().hashCode();
  }

  public boolean equals(Object obj) {
    return localSimpleDateFormat.get().equals(obj);
  }

}