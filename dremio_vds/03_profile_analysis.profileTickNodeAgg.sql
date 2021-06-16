create or replace VDS 
profile_analysis.profileTickNodeAgg 
AS
SELECT
   "to_timestamp"("tickMs" / 1000.0) AS "tickTs"
 , "tickMs"
 , "queryId"
 , "userId"
 , "queueName"
 , "finalState"
 , "queryType"
 , COUNT(*)               AS "nodeCount"
 , AVG("fragments")       AS "avgFragmentsPerNode"
 , MAX("fragments")       AS "maxFragmentsPerNode"
 , ROUND(AVG("memMb"), 1) AS "avgMemMbPerNode"
 , MAX("memMb")           AS "maxMemMbPerNode"
FROM
   (
      SELECT
         "tickMs"
       , "queryId"
       , "userId"
       , "queueName"
       , "finalState"
       , "queryType"
       , "endpointAddress"
            || ':'
            || CAST("endpointFabricPort" AS VARCHAR) AS "nodeId"
       , COUNT(*)                                    AS "queryCount"
       , SUM("totalFragments")                       AS "fragments"
       , SUM("maxMemMb")                             AS "memMb"
      FROM
         "profile_analysis"."profileTicksNodes"
      GROUP BY
         1
       , 2
       , 3
       , 4
       , 5
       , 6
       , 7
   )
   AS "x"
GROUP BY
   1
 , 2
 , 3
 , 4
 , 5
 , 6
 , 7
 ;