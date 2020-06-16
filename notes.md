
# Notes
* when setting up channels, it's easy to set them up so that deadlock can occur
* if the rx channel for the DB thread is shared between the MAIN and LOGSINK threads, lots of incoming messages could accumulate in the database before the ACKs have a chance to be processed. An alternative is to have the DB thread trying to receive on two channels so that both will be processed fairly.
* auto vaccuum is disabled by default. This means the database size will be a HWM for the number of pages required.
  * https://www.techonthenet.com/sqlite/auto_vacuum.php
* Q: Do we want backpressure for data coming into the database? What about for data going out the log sink?
  * the answer to this is probably "yes, once we can do something about it" -- specifically, once we can bring up another LogDriver to give us more throughput, it would probably be useful. Allowing a TSP to write log data to the PUSH socket and then not reading it because the channel to the database is full might not be what we want. (On the other hand, it might be what we want as then the TSP could be horizontally scaled at that point)
* on 100k runs I observe a point in the test where the write performance gets somewhat poor for a period of time. During this time the used RAM increases, the database file size grows, disk write speed decreases. After a while "regular" performance resumes. I wonder if this is tokio, linux file caching, something else running on my machine?


  |inserts | commit mode | time    |
  | 10000  | rollback    | 42.899  |
  | 10000  | WAL         | 12.026  |





# Done so far

* Decide on database record schema
* Generate input data and write it to a file
* cleaned up threading shutdown
* remove recrypt, replace with input file
* add deletes for acks
  - ran into issue where a ton of incoming messages would get piled up
* observed somewhat slow performance. Switched the database to WAL instead of Rollback mode. 

# Next 
* find a way to better record stats over time. CSV?
  * https://github.com/eminence/procfs
  * process memory usage
  * dbfile size
  * inserts/deletes per time slice
  
* experiment more with selects
* create a periodic select to simulate retry
* split db rx into two channels - see if there's a way we can go back to bounded channels
  * Will fairer processing of inserts/deletes change the performance characteristics
* introduce "occasional" non acks
* investigate other pragma options. 
    * we could probably turn off indexes as read performance isn't critical - https://stackoverflow.com/questions/15778716/sqlite-insert-speed-slows-as-number-of-records-increases-due-to-an-index
    * could turn on auto-vaccum
* 

# Summary of Findings
* DB file size grows to a HWM. Freed pages are re-used.
