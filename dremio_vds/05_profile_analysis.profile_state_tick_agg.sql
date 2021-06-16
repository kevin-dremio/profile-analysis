CREATE OR REPLACE VDS 
profile_analysis.profile_state_tick_agg 
AS
SELECT
   "to_timestamp"("tickMs" / 1000.0) AS "ts"
 , "tickMs"
 , "q"."userId"
 , "q"."finalstate"
 , "lpad"(CAST("queryState" AS VARCHAR), 2, '0')
      || ' ('
      || "n"."stateName"
      || ')'                        AS "queryState"
 , COUNT(*)                         AS "totalQueries"
 , MAX("stateDurationMs")           AS "maxDuration"
 , ROUND(AVG("stateDurationMs"), 1) AS "avgDuration"
 , MIN("stateDurationMs")           AS "minDuration"
 , SUM("stateDurationMs")           AS "sumDuration"
FROM
   "profile_analysis"."source"."ticks" AS "t"
   INNER JOIN
      "profile_analysis"."source"."queries" AS "q"
      ON
         "t"."qNum" = "q"."qNum"
   LEFT JOIN
      "profile_analysis"."source"."state_names" AS "n"
      ON
         "t"."queryState" = "n"."id"
GROUP BY
   1
 , 2
 , 3
 , 4
 , 5
ORDER BY
   1
 , 5
;