package com.paic.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Utilities for using time functions convenience.
 */
public class TimeUtils {
  
  /**
   * Format a Date to a formated string.
   * Pattern example: "yyyy-MM-dd' 'HH:mm:ss.SSS".
   * @param date the Date
   * @param pattern the format pattern string
   * @return the formated time string
   */
  public static String formatDateTime(Date date, String pattern) {
    SimpleDateFormat df = new SimpleDateFormat(pattern);
    return df.format(date);
  }
  
  /**
   * Parse a formated string time to Date.
   * Pattern example: "yyyy-MM-dd' 'HH:mm:ss.SSS"
   * @param dateStr the date string
   * @param pattern the format pattern string
   * @return the date
   */
  public static Date parseDateTime(String dateStr, String pattern) 
          throws ParseException {
    SimpleDateFormat df = new SimpleDateFormat(pattern);
    return df.parse(dateStr);
  }
  
}
