CREATE SEQUENCE analytics_report_generation AS bigint START WITH 1;
SELECT setval('analytics_report_generation', greatest(coalesce(max(generation),0),1),
              coalesce(max(generation),0) > 0) FROM analytics_report_build;
