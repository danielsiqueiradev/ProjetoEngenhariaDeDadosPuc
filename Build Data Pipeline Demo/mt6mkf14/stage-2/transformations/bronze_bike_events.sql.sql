-- Databricks notebook source
-- Bronze: load raw bike-share CSV files from a managed volume directory.
CREATE OR REFRESH MATERIALIZED VIEW bronze_bike_events_mt6mkf14_s2
COMMENT "Raw rides loaded from CSV files uploaded by the build-data-pipeline demo."
AS
SELECT *
FROM read_files("/Volumes/dbacademy/default/puc", format => "csv", header => true, delimiter => ";");