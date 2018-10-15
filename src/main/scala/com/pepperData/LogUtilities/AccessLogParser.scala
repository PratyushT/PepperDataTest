package com.pepperData.LogUtilities

import java.util.regex.Pattern
import java.text.SimpleDateFormat
import java.util.Locale
import scala.util.control.Exception._
import java.util.regex.Matcher

@SerialVersionUID(100L)
class AccessLogParser extends Serializable {

    private val ddd = "\\d{1,3}"                      
    private val ip = s"($ddd\\.$ddd\\.$ddd\\.$ddd)?"  
    private val client = "(\\S+)"                    
    private val user = "(\\S+)"
    private val dateTime = "(\\[.+?\\])"              
    private val requestType = "\"(.*?)"                
    private val requestURI = "(.*?)\""  
    private val status = "(\\d{3})"
    private val bytes = "(\\S+)"                    
    private val referer = "\"(.*?)\""
    private val agent = "\"(.*?)\""
    private val regex = s"$ip $client $user $dateTime $requestType $requestURI $status $bytes $referer $agent"
    private val p = Pattern.compile(regex)
    
    def parseRecordReturningNullObjectOnFailure(record: String): AccessLogRecord = {
        val matcher = p.matcher(record)
        if (matcher.find) {
            buildAccessLogRecord(matcher)
        } else {
            AccessLogRecord("", "", "", "", "", "", "", "", "", "")
        }
    }
    
    private def buildAccessLogRecord(matcher: Matcher) = {
        AccessLogRecord(
            matcher.group(1),
            matcher.group(2),
            matcher.group(3),
            matcher.group(4),
            matcher.group(5),
            matcher.group(6),
            matcher.group(7),
            matcher.group(8),
            matcher.group(9),
            matcher.group(10))
    }
}



