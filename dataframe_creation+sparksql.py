from pyspark import SparkContext, SparkConf
from pyspark.sql import Row
from datetime import datetime
from pyspark.sql.types import *
from pyspark.sql import SQLContext

app_name = "i_dunno"
player_file = "soccer/Player.csv"
attributes_file = "soccer/Player_Attributes.csv"

conf = SparkConf().setMaster("local").setAppName(app_name)
sc = SparkContext(conf = conf)
sqlContext = SQLContext(sc)

def toIntSafe(inval):
	try:
		return int(inval)
	except ValueError:
		return None

def toTimeSafe(inval):
	try:
		return datetime.strptime(inval, "%Y-%m-%d %H:%M:%S")
	except ValueError:
		return None

def toFloatSafe(inval):
	try:
		return float(inval)
	except ValueError:
		return 

def stringToPlayer(s):
	return Row(
		toIntSafe(s[0]),
		toIntSafe(s[1]),
		s[2].strip('"'),
		toIntSafe(s[3]),
		toTimeSafe(s[4].strip('"')),
		toFloatSafe(s[5]),
		toIntSafe(s[6]))

def stringToAttributes(s):
	return Row(
		toIntSafe(s[0]),
		toIntSafe(s[1]),
		toIntSafe(s[2]),
		toTimeSafe(s[3].strip('"')),
		toIntSafe(s[4]),
		toIntSafe(s[5]),
		s[6],
		s[7],
		s[8],
		*[toIntSafe(field) for field in s[9:]]
		)

playerSchema = StructType([
	StructField("id", IntegerType(), False),
	StructField("player_api_id", IntegerType(), True),
	StructField("player_name", StringType(), True),
	StructField("player_fifa_api_id", IntegerType(), True),
	StructField("birthday", TimestampType(), True),
	StructField("height", FloatType(), True),
	StructField("weight", IntegerType(), True)
	])

finalColumns = ["crossing","finishing","heading_accuracy","short_passing","volleys","dribbling","curve","free_kick_accuracy","long_passing","ball_control","acceleration","sprint_speed","agility","reactions","balance","shot_power","jumping","stamina","strength","long_shots","aggression","interceptions","positioning","vision","penalties","marking","standing_tackle","sliding_tackle","gk_diving","gk_handling","gk_kicking","gk_positioning","gk_reflexes"]

attributesSchema = StructType([
	StructField("id", IntegerType(), False),
	StructField("player_fifa_api_id", IntegerType(), True),
	StructField("player_api_id", IntegerType(), True),
	StructField("date", TimestampType(), True),
	StructField("overall_rating", IntegerType(), True),
	StructField("potential", IntegerType(), True),
	StructField("preferred_foot", StringType(), True),
	StructField("attacking_work_rate", StringType(), True),
	StructField("defensive_work_rate", StringType(), True)] + [StructField(s, IntegerType(), True) for s in finalColumns])

player_rdd = sc.textFile(player_file)
player_header = player_rdd.first()
player_rdd = player_rdd.filter(lambda row: row != player_header)
attributes_rdd = sc.textFile(attributes_file)
attributes_header = attributes_rdd.first()
attributes_rdd = attributes_rdd.filter(lambda row: row != attributes_header)

player_df = sqlContext.createDataFrame(player_rdd.map(lambda row: stringToPlayer(row.split(','))), playerSchema)
attributes_df = sqlContext.createDataFrame(attributes_rdd.map(lambda row: stringToAttributes(row.split(','))), attributesSchema)

#Combining DataFrame- output dataframe has all the attributes of the playes
player_merge=player_df.join(attributes_df,player_df.player_api_id==attributes_df.player_api_id,'left')
#Calculation of Age
df2=player_merge.withColumn('age',datediff('date','birthday')/365.0)
df2=df2.withColumn("intage",df2["age"].cast(IntegerType())).drop('age').withColumnRenamed("intage","age")
