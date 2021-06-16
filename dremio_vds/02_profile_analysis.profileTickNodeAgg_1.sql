create or replace VDS
profile_analysis.profileTickNodeAgg_1 
as
SELECT
   "to_timestamp"("tickMs" / 1000.0) AS "tickTs"
 , "tickMs"
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
 , 8
 ;