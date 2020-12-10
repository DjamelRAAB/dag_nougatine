from pyspark.sql import SparkSession 
import  pyspark.sql.functions as F
import sys
import re

def init_spark():
  # initialisation de la spark seesion et recuperation du spark context
  try :
    spark = SparkSession.builder.appName("collect_data").getOrCreate()
    sc = spark.sparkContext
  except Exception as e:
    sys.exit(1)
  return spark,sc


def init_logger(sc):
  # initialiasation du loger
  """
  sc : SparkContext
  """
  try :
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)
    log.info("pyspark script logger initialized")
  except Exception as e:
    sys.exit(1)
  return log

def save_in_hive (log,spark,dataFrame,name_table) :
      # save a dataframe into a hive table 
  """ 
  log : Logger spark
  spark : SparkSession 
  dataFrame : Dataframe
  """
  try :
    dataFrame.write.saveAsTable('iabd2_group6.'+name_table)
    log.info("save de la table "+name_table+" ... OK")
  except Exception as e :
    log.error("save de la table "+name_table+" ... KO")
    log.error("erreur : " + str(e))
    sys.exit(1)



def get_top_artist_from_top_playlist(log,tb_artists_releases_full):
    try :
        df = tb_artists_releases_full.select("name","followers","popularity","date")
        # withColumn("followers",F.lit(int(str(re.findall(r'\d+', str(F.col("followers")))))))
        log.info("get_top_artist_from_top_playlist .. ok")
    except Exception as e:
        log.error("get_top_artist_from_top_playlist ... KO")
        log.error("erreur : " + str(e))
        sys.exit(1)
    return df


def main():
    spark,sc = init_spark()
    log  = init_logger(sc)
    list_name = ["albums_releases_full","artists_releases_full","tracks_featured_full","tracks_releases_full","playlists_featured_full","artists_featured_full"]
    tb_artists_releases_full = spark.table("iabd2_group6.tb_artists_releases_full")
    df_top = get_top_artist_from_top_playlist(log,tb_artists_releases_full)
    save_in_hive (log,spark,df_top,"top_artist2")
    print(df_top.count())

if __name__ == '__main__':
  main()
