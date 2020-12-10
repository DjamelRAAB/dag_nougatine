from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import datetime 
import sys

def init_spark():
  # initialisation de la spark seesion et recuperation du spark context
  try :
    spark = SparkSession.builder.appName("collect_data").config("spark.sql.warehouse.dir", "hdfs://d271ee89-3c06-4d40-b9d6-d3c1d65feb57.priv.instances.scw.cloud:8020/user/hive/warehouse/").enableHiveSupport().getOrCreate()
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

def load_csv_file(log,spark,name_file):
  # load d'un fichier
  """
  log : Logger spark
  spark : SparkSession
  """
  try :
    df = spark.read.options(header='True', inferSchema='True', delimiter=',').csv(name_file)
    log.info("load de la table "+name_file+" ... OK")
  except Exception as e :
    log.error("load de la table "+name_file+" ... KO")
    log.error("erreur : " + str(e))
    sys.exit(1)
  return df

def save_in_hive (log,spark,dataFrame,name_table,date) :
  # save a dataframe into a hive table 
  """ 
  log : Logger spark
  spark : SparkSession 
  dataFrame : Dataframe
  """
  try :
    dataFrame.withColumn("date",F.lit(date)).write.saveAsTable('iabd2_group6.'+name_table)
    log.info("save de la table "+name_table+" ... OK")
  except Exception as e :
    log.error("save de la table "+name_table+" ... KO")
    log.error("erreur : " + str(e))
    sys.exit(1)

def get_top_artist_from_top_playlist(log,df_artists_releases_full):
    try :
        df = df_artists_releases_full.select("name","followers","popularity","date")
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
  date=datetime.date.today().strftime("%Y%m%d")
  list_name = ["albums_releases_full","artists_releases_full","tracks_featured_full","tracks_releases_full","playlists_featured_full","artists_featured_full"]
  for name in list_name :
        exec('df_'+ name +' = load_csv_file(log,spark,"/user/iabd2_group6/data/'+date+'/df_'+name+'.csv")')
        exec('save_in_hive(log,spark,df_'+ name +' ,"tb_'+name+'_v1",date )')
  df_artist_up = get_top_artist_from_top_playlist(log,df_artists_releases_full).coalesce()
  df_artist_already_up = get_top_artist_from_top_playlist(log,df_artists_featured_full).coalesce()

  df_artist_up.write.csv('df_artist_up.csv')
  df_artist_already_up.write.csv('df_artist_already_up.csv')


if __name__ == '__main__':
  main()
