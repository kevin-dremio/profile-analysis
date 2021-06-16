CREATE OR REPLACE VDS 
profile_analysis.profileStateTick 
AS
SELECT
   "to_timestamp"("tickMs" / 1000.0) AS "ts"
 , "tickMs"
 , "q"."userId"
 , "q"."queryType"
 , "q"."queryId"
 , "q"."finalstate"     AS "finalState"
 , "q"."totalFragments" AS "totalFragments"
 , "q"."queueName"
 , "lpad"(CAST("queryState" AS VARCHAR), 2, '0')
      || ' ('
      || REPLACE("n"."stateName", '_', ' ')
      || ')' AS "queryState"
 , CASE
      WHEN "stateDurationMs" > 0
         THEN ROUND(CAST("stateDurationMs" AS DOUBLE) / 1000.0, 3)
         ELSE 0.0
   END                       AS "stateDurationSec"
 , "q"."queryTextFirstChunk" AS "queryText"
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
;