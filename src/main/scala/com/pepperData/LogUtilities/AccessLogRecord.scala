package com.pepperData.LogUtilities

case class AccessLogRecord (
    clientIpAddress: String,         // should be an ip address, but may also be the hostname if hostname-lookups are enabled
    ClientIdentity: String,   // typically `-`
    remoteUser: String,              // typically `-`
    dateTime: String,                // [day/month/year:hour:minute:second zone]
    requestType: String,                 // `GET /foo ...`
    requestURL: String,
    httpStatusCode: String,          // 200, 404, etc.
    bytesSent: String,               // may be `-`
    referer: String,                 // where the visitor came from
    userAgent: String                // long string to represent the browser and OS
)

