create or replace VDS
profile_analysis.profileTicksNodes as
SELECT
   "to_timestamp"("t"."tickMs" / 1000.0) AS "tickTs"
 , "t".*
 , "q"."queryId"
 , "q"."userId"
 , "q"."finalState"
 , "q"."totalFragments"
 , "q"."queueName"
 , "q"."queryType"
 , "n"."endpointAddress"
 , "n"."endpointFabricPort"
 , ROUND(CAST("n"."maxMemoryUsed" AS DOUBLE) / 1024.0 / 1024.0, 1) AS "maxMemMb"
 , "q"."queryTextFirstChunk"                                       AS "query"
FROM
   (
      SELECT
         "tickMs"
       , "qNum"
       , "stateDurationMs"
      FROM
         "profile_analysis"."source"."ticks"
      WHERE
         "queryState" = 8
   )
   AS "t"
   INNER JOIN
      "profile_analysis"."source"."nodes" AS "n"
      ON
         "n"."qNum" = "t"."qNum"
   INNER JOIN
      "profile_analysis"."source"."queries" AS "q"
      ON
         "t"."qNum" = "q"."qNum"
;
