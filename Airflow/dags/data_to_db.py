from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import regexp_replace, col, lit, concat_ws, upper,udf, array, struct, explode, monotonically_increasing_id, to_timestamp
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType,FloatType, BooleanType
from pyspark.sql.window import Window
import requests
import time, re
import argparse
import pandas as pd
from datetime import datetime
from pandas import json_normalize
import json
# Spark session
# print(1)



# Database info
USERNAME = 'airflow'
PASSWORD = 'airflow'
URL = "jdbc:postgresql://host.docker.internal:5432/airflow"

def get_tickers(demo, init = False):
    if not init: tickers = get_table("listing_companies", True).select('ticker').rdd.flatMap(lambda x: x).collect()
    else:
        # API request config for SSI API endpoints
        headers = {
                'Connection': 'keep-alive',
                'sec-ch-ua': '"Not A;Brand";v="99", "Chromium";v="98", "Google Chrome";v="98"',
                'DNT': '1',
                'sec-ch-ua-mobile': '?0',
                'X-Fiin-Key': 'KEY',
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                'X-Fiin-User-ID': 'ID',
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36',
                'X-Fiin-Seed': 'SEED',
                'sec-ch-ua-platform': 'Windows',
                'Origin': 'https://iboard.ssi.com.vn',
                'Sec-Fetch-Site': 'same-site',
                'Sec-Fetch-Mode': 'cors',
                'Sec-Fetch-Dest': 'empty',
                'Referer': 'https://iboard.ssi.com.vn/',
                'Accept-Language': 'en-US,en;q=0.9,vi-VN;q=0.8,vi;q=0.7'
                }
        
        url = 'https://fiin-core.ssi.com.vn/Master/GetListOrganization?language=vi'
        r = requests.get(url, headers).json()
        listing_companies_df = pd.DataFrame(r['items'])
        tickers = tuple(listing_companies_df.ticker )
        # Take 3 tickers
        if demo:
            print("For demoing, We will only take the first following 3 tickers:")
            print(tickers)
            tickers= tickers[:3]
    return tickers
# response function - udf
def executeRestApi(url):
    res = None
    # Make API request, get response object back, create dataframe from above schema.
    res = requests.get(url)


    if res != None:
        return json.loads(res.text)

    return None

# response function - udf (no schema ver.)
def executeRestApiNoSchema(url, df_schema_str, ticker, params=None):
    res = None
    df_schema_dict = json.loads(df_schema_str)
    # Make API request, get response object back, create dataframe from above schema.
    res = requests.get(url, params = params)
    
    if res != None:
        res_dict = json.loads(res.text)
        if isinstance(res_dict, list): 
            if len(res_dict)==0:
                return {"data_array":[dict(zip(df_schema_dict.keys(), [None if col != "ticker" else ticker for col in df_schema_dict.keys()]))]}
            res_text = res.text
            df_to_db = dict(zip(res_dict[0].keys() ,format_df_db(list(res_dict[0].keys()))))
            for key, value in df_to_db.items():
                res_text = res_text.replace(key, value)
            result=[]
            for res_dict in json.loads(res_text):
              result.append(dict(zip(df_schema_dict.keys() , list(map(lambda x: float(res_dict.get(x)) if ((df_schema_dict.get(x) == 'float' or df_schema_dict.get(x) == 'double') and res_dict.get(x) != None)  else res_dict.get(x), df_schema_dict.keys()) ))))

            return {"data_array":result}
        else:
            res_dict = json.loads(res.text)
            res_dict = dict(zip( format_df_db(list(res_dict.keys())) , res_dict.values()))
            res_values = list(map(lambda x: float(res_dict.get(x)) if ((df_schema_dict.get(x) == 'float' or df_schema_dict.get(x) == 'double') and res_dict.get(x) != None)  else res_dict.get(x), df_schema_dict.keys()))

        return dict(zip(df_schema_dict.keys(), res_values))
    else:
        return dict(zip(df_schema_dict.keys(), [None if col != "ticker" else ticker for col in df_schema_dict.keys()]))
    
def flatten_struct(schema, prefix=""):
    result = []
    for elem in schema:
        if isinstance(elem.dataType, StructType):
            result += flatten_struct(elem.dataType, prefix + elem.name + ".")
        else:
            result.append(col(prefix + elem.name).alias(elem.name))
    return result


def get_schema_table(table):
    ticker = "ACB"
    i=1
    if table == "balance_sheet":
        request = requests.get('https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{}/{}'.format(ticker, 'balancesheet'), params={'yearly': i, 'isAll':'true'})
    elif table == "income_statement":
        request = requests.get('https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{}/{}'.format(ticker, 'incomestatement'), params={'yearly': i, 'isAll':'true'})
    elif table == "cash_flow":
        request = requests.get('https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{}/{}'.format(ticker, 'cashflow'), params={'yearly': i, 'isAll':'true'})
    elif table == "financial_ratio":
        request = requests.get('https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{}/financialratio?yearly={}&isAll={}'.format(ticker, i, True))
    elif table == "general_rating":
        request = requests.get('https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/{}/{}?fType=TICKER'.format(ticker,'general'))
    elif table == "business_model_rating":
        request = requests.get('https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/{}/{}?fType=TICKER'.format(ticker,'business-model'))
    elif table == "business_operation_rating":
      request = requests.get('https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/{}/{}?fType=TICKER'.format(ticker,'business-operation'))
    elif table == "financial_health_rating":
        request = requests.get('https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/{}/{}?fType=TICKER'.format(ticker,'financial-health'))
    elif table == "valuation_rating":
        request = requests.get('https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/{}/{}?fType=TICKER'.format(ticker,'valuation'))
    elif table == "industry_financial_health":
      request = requests.get('https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/{}/financial-health?fType=INDUSTRY'.format(ticker))
    elif table == 'listing_companies':
      request = requests.get('https://apipubaws.tcbs.com.vn/tcanalysis/v1/ticker/{}/overview'.format(ticker))
      
    request_text = request.text.replace("null", "None")

    if request_text[0] == "[":
        list_data =list(eval(request_text))
        db_cols_sorted = format_df_db(list(list_data[0].keys()))
        table_df = get_table(table)
        db_cols_selected = list(set(db_cols_sorted)&set(table_df.columns))
        return table_df.limit(0).select(*db_cols_selected).toDF(*db_cols_selected)
    else:
        request_text = request_text.replace("None","")
        list_data = request.json()
        db_cols_sorted = format_df_db(list(list_data.keys()))
        table_df = get_table(table)
        db_cols_selected = list(set(db_cols_sorted)&set(table_df.columns))
        return table_df.limit(0).select(*db_cols_selected).toDF(*db_cols_selected)


df_to_db =  {}
db_to_df = {}
def format_df_db(fields):
    global df_to_db, db_to_df
    for i in range(len(fields)):
        df = fields[i]
        fields[i] = fields[i][0].lower() + fields[i][1:]
        fields[i] = re.sub(r"(\w)([A-Z])", r"\1_\2", fields[i])
        fields[i] = fields[i].lower()
        db = fields[i]
        df_to_db[df]=db
        db_to_df[db]=df
    return(fields)
def format_db_df(fields):
    global df_to_db, db_to_df
    fields = map(lambda x: db_to_df[x], fields)
    return(fields)

def get_table(table, full=False, calculation = False):
    if calculation == True and full == False:
        df = spark.read \
            .format("jdbc") \
            .option("url", URL) \
            .option("driver", "org.postgresql.Driver") \
            .option("query", 
                    """SELECT * FROM  stock_history sh  
                        WHERE "time_stamp"  >= NOW() -   INTERVAL '30 day'""") \
            .option("user", USERNAME) \
            .option("password", PASSWORD) \
            .load()
    if full:
        df = spark.read \
            .format("jdbc") \
            .option("url", URL) \
            .option("driver", "org.postgresql.Driver") \
            .option("dbtable", table) \
            .option("user", USERNAME) \
            .option("password", PASSWORD) \
            .load()
    else:
        df = spark.read \
            .format("jdbc") \
            .option("url", URL) \
            .option("driver", "org.postgresql.Driver") \
            .option("query", "SELECT * FROM " + table + " LIMIT 1") \
            .option("user", USERNAME) \
            .option("password", PASSWORD) \
            .load()
    return df

def cast(df, table):
    table_df = get_table(table)
    df = df.select(*table_df.columns)
    schema = table_df.dtypes
    
    for column in schema:
        df = df.withColumn(column[0], col(column[0]).cast(column[1]))

    return df


def data_to_db(df, table, mode, cascadeTruncate=False):
    # print('data_to_db')
    df.printSchema()
    df = df.toDF(*format_df_db(df.columns)) # Change columns' name
    df = cast(df, table)
    df = df.select(*df.columns) # Shuffle columns to be the same as table
    df.printSchema()
    # print('data_to_db2')
    df.write.option("cascadeTruncate", str(cascadeTruncate))\
        .format('jdbc')\
        .options(
            url=URL,
            driver='org.postgresql.Driver',
            dbtable=table,
            user=USERNAME,
            password=PASSWORD)\
        .mode(mode)\
        .save()
    # print('data_to_db3')
def clean_time(df, column):
    time_stamp = regexp_replace(column, 'T', ' ')
    time_stamp = regexp_replace(time_stamp, '\.(.+)', '')
    return df.withColumn(column, time_stamp)

def get_ratio(demo, report_type, table):
    global spark
    spark = SparkSession \
        .builder.appName("Import "+ table)\
        .getOrCreate()
    tickers = get_tickers(demo)
    '''
    Use for cashflow, incomestatement, balancesheet, financialratio
    '''
    schema_df = get_schema_table(table)
    schema_dict = dict(schema_df.dtypes)
    
    schema_df = schema_df.withColumn("data_array",array(struct(*schema_df.columns)))
    schema_df = schema_df.select('data_array')
    # schema_df.printSchema()
    schema = schema_df.schema
    # schema_df.show()
    udf_executeRestApi = udf(executeRestApiNoSchema, schema)

    # requests
    RestApiRequest = Row("url", "df_schema", "ticker_name")
    request_df = spark.createDataFrame([
                RestApiRequest('https://apipubaws.tcbs.com.vn/tcanalysis/v1/finance/{}/{}?yearly={}&isAll={}'.format(ticker, report_type, i, True), \
                                json.dumps(schema_dict), ticker) for i in range(2) for ticker in tickers 
            ])\
            .withColumn("execute", udf_executeRestApi(col("url"), col("df_schema"), col("ticker_name")))
    # request_df.show(truncate=False)
    df = request_df.select(explode(request_df.execute.data_array))
    df = df.select(flatten_struct(df.schema))


    #Clean
    df = df.orderBy(col("ticker").asc(),col("year").desc(), col("quarter").desc())
    df = df.withColumn("ticker", upper(col("ticker")))
    df=df.dropna(subset=["ticker","year","quarter"])
    #Write to db
    data_to_db(df, table, 'overwrite', True)
    spark.stop()
    return df

def get_rating(demo,rating_type, table):
    global spark
    spark = SparkSession \
        .builder.appName("Import "+ table)\
        .getOrCreate()
    tickers = get_tickers(demo)

    '''
    Use for general, business-model, business-operation, financial-health, valuation, industry-health
    '''
    schema_df = get_schema_table(table)
    schema = schema_df.schema
    schema_dict = dict(schema_df.dtypes)

    udf_executeRestApi = udf(executeRestApiNoSchema, schema)

    # requests
    RestApiRequest = Row("url", "df_schema", "ticker_name")

    if rating_type == 'industry-health':
        request_df = spark.createDataFrame([
                    RestApiRequest('https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/{}/financial-health?fType=INDUSTRY'.format(ticker), \
                                   json.dumps(schema_dict), ticker) for i in range(2) for ticker in tickers
                ])\
                .withColumn("execute", udf_executeRestApi(col("url"), col("df_schema"), col("ticker_name")))
    else:
        request_df = spark.createDataFrame([
                    RestApiRequest('https://apipubaws.tcbs.com.vn/tcanalysis/v1/rating/{}/{}?fType=TICKER'.format(ticker, rating_type), \
                                   json.dumps(schema_dict), ticker) for i in range(2) for ticker in tickers
                ])\
                .withColumn("execute", udf_executeRestApi(col("url"), col("df_schema"), col("ticker_name")))
    # request_df.show(truncate=False)
    df = request_df.select(flatten_struct(request_df.schema))
    # df.printSchema()
    df = df.drop("url", "df_schema", "ticker_name")

    #Clean
    df = df.dropDuplicates(["ticker"])
    
    df = df.withColumn("ticker", upper(col("ticker")))
    d = datetime.now()
    currentYear = datetime.now().year
    currentMonth = datetime.now().month
    df = df.withColumn("year", lit(currentYear))
    df = df.withColumn("quarter", lit((currentMonth-1)//3+1))
    df=df.dropna(subset=["ticker","year","quarter"])
    
    df = df.orderBy(col("ticker").asc(), col("year").desc(), col("quarter").desc())
    
    #Write to db
    data_to_db(df, table, 'append')
    spark.stop()
    return df


def get_stock_history(demo,start_date, end_date):
    global spark
    spark = SparkSession \
        .builder.appName("Import history")\
        .getOrCreate()
    tickers = get_tickers(demo)

    #Convert date to timestamp
    fd = int(time.mktime(time.strptime(start_date, "%Y-%m-%d")))
    td = int(time.mktime(time.strptime(end_date, "%Y-%m-%d")))

    schema = StructType([
        StructField("ticker", StringType(), True),
        StructField("data", ArrayType(
            StructType([
            StructField("open", FloatType()),
            StructField("high", FloatType()),
            StructField("low", FloatType()),
            StructField("close", FloatType()),
            StructField("volume", IntegerType()),
            StructField("tradingDate", StringType()),
            ])
        ))
    ])
    
        #
    udf_executeRestApi = udf(executeRestApi, schema)

    RestApiRequest = Row("url")
    request_df = spark.createDataFrame([
                RestApiRequest('https://apipubaws.tcbs.com.vn/stock-insight/v1/stock/bars-long-term?ticker={}&type=stock&resolution=D&from={}&to={}'.format(ticker, fd, td) ) for ticker in tickers
            ])\
            .withColumn("execute", udf_executeRestApi(col("url")))
    
    df = request_df.select(request_df.execute.ticker,explode(request_df.execute.data)) \
            .withColumnRenamed('execute.ticker', 'ticker')
    # request_df.show(truncate=False)
    # df.printSchema()
    df =  df.select(flatten_struct(df.schema)).withColumnRenamed('tradingDate', 'time_stamp')
    # df.printSchema()
    df = df.select("ticker", to_timestamp('time_stamp').alias("time_stamp"), "open", "high", "low", "close", "volume")
    
    #clean
    df = clean_time(df, 'time_stamp')

    df = df.orderBy(col("ticker").asc(),col("time_stamp").asc())

    df = df.withColumn("ticker", upper(col("ticker")))

    if demo: df.show()
    #Write to db
    data_to_db(df, 'stock_history', 'append')
    spark.stop()
    return df


def get_intraday_transaction(demo=False):
    global spark
    spark = SparkSession \
        .builder.appName("Import transaction")\
        .getOrCreate()
    tickers = get_tickers(demo)

    schema = StructType([
    StructField("page", IntegerType(), True),
    StructField("size", IntegerType()),
    StructField("headIndex", IntegerType()),
    StructField("numberOfItems", IntegerType()),
    StructField("total", IntegerType()),
    StructField("ticker", StringType()),       
    StructField("data", ArrayType(
        StructType([
        StructField("p", FloatType()),
        StructField("v", IntegerType()),
        StructField("cp", FloatType()),
        StructField("rcp", FloatType()),
        StructField("a", StringType()),
        StructField("ba", FloatType()),
        StructField("sa", FloatType()),
        StructField("hl", BooleanType()),
        StructField("pcp", FloatType()),
        StructField("t", StringType()),
        ])
    ))
    ])

    udf_executeRestApi = udf(executeRestApi, schema)
    
    # requests
    RestApiRequest = Row("url")
    d = datetime.now()
    today = re.sub(" (.+)","", str(d))

    if d.weekday() > 4:     #today is weekend
        request_df = spark.createDataFrame([
                    RestApiRequest('https://apipubaws.tcbs.com.vn/stock-insight/v1/intraday/{}/his/paging?page={}&size={}&headIndex=-1'.format(ticker, 0, 1000000)) for ticker in tickers
                ])\
                .withColumn("execute", udf_executeRestApi(col("url")))
    else:   #today is weekday
        request_df = spark.createDataFrame([
                    RestApiRequest('https://apipubaws.tcbs.com.vn/stock-insight/v1/intraday/{}/his/paging?page={}&size={}'.format(ticker, 0, 1000000)) for ticker in tickers
                    ])\
                .withColumn("execute", udf_executeRestApi(col("url")))
    # request_df.show(truncate=False)
    df = request_df.select(request_df.execute.ticker,explode(request_df.execute.data)) \
            .withColumnRenamed('execute.ticker', 'ticker')\
            
    # df.printSchema()
    df =  df.select(flatten_struct(df.schema))
    # df.printSchema()
    df = df.withColumn('time_stamp', concat_ws(' ', lit(today), col('t')))
    df = df.select("p", "v", "cp", "rcp", "a", "ba", "sa", "hl", "pcp", "time_stamp", "ticker") \
            .withColumnRenamed("p","price")\
            .withColumnRenamed("v","volume")

    
    df = df.withColumn("ticker", upper(col("ticker")))
    df = df.orderBy(col("ticker").asc() , col("time_stamp").asc())
    df = df.withColumn("id", monotonically_increasing_id())
    #Write to db
    data_to_db(df, 'stock_intraday_transaction', 'append')
    spark.stop()
    return df


def get_listing_companies(demo):
    global spark
    spark = SparkSession \
        .builder.appName("Import companies")\
        .getOrCreate()
    
    schema = StructType([
    StructField("exchange", StringType(), True),
    StructField("short_name", StringType()),
    StructField("industry_id", IntegerType()),
    StructField("industry_idv2", IntegerType()),
    StructField("industry", StringType()),
    StructField("industry_en", StringType()),       
    StructField("established_year", IntegerType()),      
    StructField("no_employees", IntegerType()),      
    StructField("no_shareholders", IntegerType()), 
    StructField("foreign_percent", FloatType()),
    StructField("website", StringType()),
    StructField("stock_rating", FloatType()),
    StructField("delta_in_week", FloatType()),
    StructField("delta_in_month", FloatType()),
    StructField("delta_in_year", FloatType()),
    StructField("outstanding_share", FloatType()),
    StructField("issue_share", IntegerType()),
    StructField("company_type", StringType()),
    StructField("ticker", StringType()),          
    ])
    
    tickers = get_tickers(demo, init=True )
    schema_df = spark.createDataFrame([],schema=schema)
    schema = schema_df.schema
    schema_dict = dict(schema_df.dtypes)
    schema_dict = dict(zip([ re.sub(r"(\w)([A-Z])", r"\1_\2", key) for key in schema_dict.keys()],schema_dict.values()))
    udf_executeRestApi = udf(executeRestApiNoSchema, schema)
    # print("executing udf")
    # print(tickers)
    # requests
    RestApiRequest = Row("url", "df_schema", "ticker_name")
    request_df = spark.createDataFrame([
                RestApiRequest('https://apipubaws.tcbs.com.vn/tcanalysis/v1/ticker/{}/overview'.format(ticker), json.dumps(schema_dict), ticker) for ticker in tickers
              ])\
              .withColumn("execute", udf_executeRestApi(col("url"), col("df_schema"), col("ticker_name")))
    # print("after exec   ")
    # request_df.show(truncate=False)
    df = request_df.drop("url", "df_schema", "ticker_name")
    df = df.select("execute.*")
    #Clean
    # df = df.orderBy(col("ticker").asc())
    df = df.withColumn("ticker", upper(col("ticker")))
    
    listing_df = df.select("ticker", "exchange", "short_name", "industry_id", "industry_idv2", "industry", "industry_en", "established_year", "company_type")
    info_df = df.select("ticker","no_employees", "no_shareholders","foreign_percent","website","stock_rating","delta_in_week","delta_in_month", "delta_in_year", "outstanding_share", "issue_share")

    #  Only get new tickers
    listing_df = listing_df.join(get_table('listing_companies',True), ['ticker'], 'left_anti')


    #Write to db

    data_to_db(listing_df, 'listing_companies', 'append')
    data_to_db(info_df, 'companies_info', 'overwrite', True)
    spark.stop()
    return df

def calculate_indicators(init):
    global spark
    spark = SparkSession \
        .builder.appName("Calculating indicators")\
        .getOrCreate()

    df = get_table("stock_history", full=True)

    df = df.withColumn('long_timestamp', F.to_timestamp('time_stamp').cast("long"))

    # 2629800 is the number of seconds in one month
    w = Window().partitionBy('ticker').orderBy('long_timestamp').rangeBetween(-2629800, 0)

    df = df.withColumn(
        'sma',F.avg('volume').over(w)
    )


    df = df.withColumn("distance", (col("close") - col("sma"))**2)
    df = df.withColumn(
        'Deviation',
        F.sqrt(F.sum('distance').over(w)/30)
    )
    df = df.withColumn(
        'higher_bollinger',
        col("volume")-col("deviation")
    )
    df = df.withColumn(
        'lower_bollinger',
        col("volume")+col("deviation")
    )
    df = df.withColumn(
        'typical_price',
        (col("high")+col("low")+col("close"))/3
    )
    df = df.withColumn(
        'lag_typical_price',
        F.lag('typical_price').over(Window().partitionBy('ticker').orderBy('long_timestamp'))
    )
    df = df.withColumn('positive',
                F.when(col('lag_typical_price') < col("typical_price"), col("typical_price")) 
                .otherwise("0"))
    df = df.withColumn('negative',
                F.when(col('lag_typical_price') > col("typical_price"), col("typical_price")) 
                .otherwise("0"))
    df = df.withColumn(
        'ratio',
        F.sum('positive').over(w)/F.sum('negative').over(w)
    )


    df = df.withColumn(
        'money_flow',
        100-(100/(1-col("ratio")))
    )
    # df.printSchema()
    df = df.select("ticker", "time_stamp", "sma", "lower_bollinger", "higher_bollinger", "money_flow")

    if init:
        data_to_db(df, 'derived_stock_history', 'append')
    else:
        df = df.withColumn("row", F.row_number().over(Window.partitionBy("ticker").orderBy(F.desc(col("time_stamp"))))) \
            .filter(col("row") == 1).drop("row")
        data_to_db(df, 'derived_stock_history', 'append')
        if demo: df.show()
    
    spark.stop()
    return df




if __name__ == '__main__':
    # print(2)
    parser = argparse.ArgumentParser(description='Get data and write to table')

    parser.add_argument('--table', 
                        type=str,
                        help='Table')
    
    parser.add_argument('--init', 
                        type=str,
                        help='Whether it is initialize or not')
    
    parser.add_argument('--start_date', 
                        type=str,
                        help='Start date of stock history')
                        
    parser.add_argument('--end_date', 
                        type=str,
                        help='End date of stock history')
    
    parser.add_argument('--yearly', 
                        type=str,
                        help='Whether this is a yearly report call')
    
    parser.add_argument('--calculation', 
                        type=str,
                        help='Whether this is a calculation call')
    
    parser.add_argument('--demo', 
                        type=str,
                        help='Whether this is a demo call')
    

    args = parser.parse_args()
    # print(4)
    init = bool(args.init)
    yearly = bool(args.yearly)
    calculation = bool(args.calculation)
    demo = bool(args.demo)
    if args.table == 'balance_sheet':
        get_ratio(demo,'balancesheet', 'balance_sheet')
    elif args.table == 'income_statement':
        get_ratio(demo,'incomestatement', 'income_statement', )
    elif args.table == 'cash_flow':
        get_ratio(demo,'cashflow', 'cash_flow')
    elif args.table == 'financial_ratio':
        get_ratio(demo, 'financialratio', 'financial_ratio')
    elif args.table == 'general_rating':
        get_rating(demo,'general', 'general_rating')
    elif args.table == 'business_model_rating':
        get_rating(demo,'business-model', 'business_model_rating')
    elif args.table == 'business_operation_rating':
        get_rating(demo,'business-operation', 'business_operation_rating')
    elif args.table == 'financial_health_rating':
        get_rating(demo,'financial-health', 'financial_health_rating')
    elif args.table == 'valuation_rating':
        get_rating(demo,'valuation', 'valuation_rating')
    elif args.table == 'industry_financial_health':
        get_rating(demo,'industry-health', 'industry_financial_health')
    elif args.table == 'stock_history':
        get_stock_history(demo,args.start_date, args.end_date)
    elif args.table == 'stock_intraday_transaction':
        get_intraday_transaction(demo)
    elif args.table == 'listing_companies':
        get_listing_companies(demo)
    elif calculation:
        calculate_indicators(init)
