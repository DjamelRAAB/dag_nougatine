from pyspark.sql import SparkSession
import sys

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
  return df

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



def main():
  spark,sc = init_spark()
  log  = init_logger(sc)
  list_name = ["albums_releases_full","artists_releases_full","tracks_featured_full","tracks_releases_full","playlists_featured_full","artists_featured_full"]
  for name in list_name :
        exec('df_'+ name +' = load_csv_file(log,spark,"/user/iabd2_group6/df_'+name+'.csv")')
        exec('save_in_hive(log,spark,df_'+ name +' ,"tb_'+name+'" )')
  
if __name__ == '__main__':
  main()
