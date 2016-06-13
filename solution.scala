

// Read the log file
val log_file = sc.textFile("./data/2015_07_22_mktplace_shop_web_log_sample.log")

// Parse each line. Extract timestamp, IP address, and URL.
def csvParse(line: String) = {
  var row = line.split(" ")
  (row(2).split(":")(0), (row(0), row(12)))
}

// Group requests by IP
val requests_by_ip = log_file.map(csvParse).groupByKey().cache()

// Prase time stamp
def praseTimestamp(timestamp: String) = {
  java.sql.Timestamp.valueOf(timestamp.replace("T", " ").replace("Z", ""))
}

// Get duration in seconds between two time stamps
def getDuration(timestamp1: String, timestamp2: String) = {
  (praseTimestamp(timestamp1).getTime - praseTimestamp(timestamp2).getTime) / 1000.0
}

// Convert a list of requests to list of session
def sessionize(key_request_list: (String, scala.collection.Iterable[(String, String)]),
  session_limit: Double) = {

  val key = key_request_list._1
  val request_list = key_request_list._2.toArray.sortWith(_._1 < _._1)
  val session_list = new scala.collection.mutable.ListBuffer[(String, (Double,
    scala.collection.mutable.Set[String]))]
  
  var session_counter = 0
  
  var current_session_id = key + "_" + session_counter
  var current_length = 0.0
  var current_urls = scala.collection.mutable.Set[String]()
  
  val url = request_list(0)._2
  current_urls += url
  
  for (i <- 1 until request_list.length) {
    val url = request_list(i)._2
    val duration = getDuration(request_list(i)._1, request_list(i-1)._1)
    if (duration <= session_limit) {
      current_length += duration
      current_urls += url
    } else {
      session_list.append((current_session_id, (current_length, current_urls)))
      session_counter += 1
      current_session_id = key + "_" + session_counter
      current_length = 0.0
      current_urls.clear
      current_urls += url
    }
  }
  
  session_list.append((current_session_id, (current_length, current_urls)))

  session_list.toList
}

// Sessionize the requests by IP
val SESSION_LIMIT = 15*60
val sessions = requests_by_ip.flatMap(kv => sessionize(kv, SESSION_LIMIT)).cache()

System.out.println("Number of sessions: " + sessions.count())

System.out.println("Average session time (seconds): " + sessions.map(kv => kv._2._1).mean())

System.out.println("Sample sessions:")
sessions.take(5).foreach{s =>
  System.out.println("   Session: " + s._1)
  s._2._2.foreach(u => System.out.println("\t" + u))
}

// Get a list of (IP, longest session of this IP)
val ip_max_session = sessions.
  map(kv => (kv._1.split("_")(0), kv._2._1)).
  reduceByKey(_ max _).map(kv => (kv._2, kv._1))

// List IPs with longest sessions
System.out.println("Most engaged users:")
val N = 20
ip_max_session.takeOrdered(N)(Ordering[(Double, String)].reverse).foreach{user =>
  System.out.println("   User: " + user._2 + "\t Max session length: " + user._1)
}
