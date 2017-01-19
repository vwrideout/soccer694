# soccer694
MSAN694 Group Project

Run spark-submit final_table_setup.py to create and join desired dataframes and save as "Players" table on disk. Need only run once.
After table created you can use sqlContext.sql(<SQL STATEMENT>) to query Players table using SQL commands, which returns query results
as Spark DataFrame (see table_query notebook for example).

When running on cluster, be sure to swap out comments on final_table_setup.py to get csv's from s3.

Players Table Schema:


 |-- id: integer (nullable = false)
 
 |-- player_api_id: integer (nullable = true)
 
 |-- player_fifa_api_id: integer (nullable = true)
 
 |-- player_name: string (nullable = true)
 
 |-- birthday: timestamp (nullable = true)
 
 |-- height: float (nullable = true)
 
 |-- weight: integer (nullable = true)
 
 |-- date: timestamp (nullable = true)
 
 |-- overall_rating: integer (nullable = true)
 
 |-- potential: integer (nullable = true)
 
 |-- preferred_foot: string (nullable = true)
 
 |-- attacking_work_rate: string (nullable = true)
 
 |-- defensive_work_rate: string (nullable = true)
 
 |-- crossing: integer (nullable = true)
 
 |-- finishing: integer (nullable = true)
 
 |-- heading_accuracy: integer (nullable = true)
 
 |-- short_passing: integer (nullable = true)
 
 |-- volleys: integer (nullable = true)
 
 |-- dribbling: integer (nullable = true)
 
 |-- curve: integer (nullable = true)
 
 |-- free_kick_accuracy: integer (nullable = true)
 
 |-- long_passing: integer (nullable = true)
 
 |-- ball_control: integer (nullable = true)
 
 |-- acceleration: integer (nullable = true)
 
 |-- sprint_speed: integer (nullable = true)
 
 |-- agility: integer (nullable = true)
 
 |-- reactions: integer (nullable = true)
 
 |-- balance: integer (nullable = true)
 
 |-- shot_power: integer (nullable = true)
 
 |-- jumping: integer (nullable = true)
 
 |-- stamina: integer (nullable = true)
 
 |-- strength: integer (nullable = true)
 
 |-- long_shots: integer (nullable = true)
 
 |-- aggression: integer (nullable = true)
 
 |-- interceptions: integer (nullable = true)
 
 |-- positioning: integer (nullable = true)
 
 |-- vision: integer (nullable = true)
 
 |-- penalties: integer (nullable = true)
 
 |-- marking: integer (nullable = true)
 
 |-- standing_tackle: integer (nullable = true)
 
 |-- sliding_tackle: integer (nullable = true)
 
 |-- gk_diving: integer (nullable = true)
 
 |-- gk_handling: integer (nullable = true)
 
 |-- gk_kicking: integer (nullable = true)
 
 |-- gk_positioning: integer (nullable = true)
 
 |-- gk_reflexes: integer (nullable = true)
 
 |-- age: integer (nullable = true)
