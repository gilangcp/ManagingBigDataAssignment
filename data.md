# Data

## Dutch Tweets
We use the Twitter data described by ([Tjong-Kim-Sang and Van den Bosch 2013][3]). 

[3]: http://ifarm.nl/erikt/papers/clin2013.pdf "Erik Tjong Kim Sang and Antal van den Bosch. Dealing with big data: The case of Twitter. In: Computational Linguistics in the Netherlands Journal 3, ISSN: 2211-4009, pages 121-134, 2013."


Access: 
* ``/data/twitterNL`` (format: One json object per line, read via ``sqlContext.read.json()``)
* ``/user/alyr/twitter`` (format: parquet files per year, read via ``sqlContext.read.parquet()``)

Schema: 

    root: struct Status
     |-- contributors: string (nullable = true)
     |-- coordinates: struct (nullable = true)
     |    |-- coordinates: array (nullable = true)
     |    |    |-- element: double (containsNull = true)
     |    |-- type: string (nullable = true)
     |-- created_at: string (nullable = true)
     |-- entities: struct (nullable = true)
     |    |-- hashtags: array (nullable = true)
     |    |-- media: array (nullable = true)
     |    |-- symbols: array (nullable = true)
     |    |-- urls: array (nullable = true)
     |    |-- user_mentions: array (nullable = true)
     |-- extended_entities: struct (nullable = true)
     |    |-- media: array (nullable = true)
     |-- favorite_count: long (nullable = true)
     |-- favorited: boolean (nullable = true)
     |-- filter_level: string (nullable = true)
     |-- geo: struct (nullable = true)
     |    |-- coordinates: array (nullable = true)
     |    |    |-- element: double (containsNull = true)
     |    |-- type: string (nullable = true)
     |-- id: long (nullable = true)
     |-- id_str: string (nullable = true)
     |-- in_reply_to_screen_name: string (nullable = true)
     |-- in_reply_to_status_id: long (nullable = true)
     |-- in_reply_to_status_id_str: string (nullable = true)
     |-- in_reply_to_user_id: long (nullable = true)
     |-- in_reply_to_user_id_str: string (nullable = true)
     |-- is_quote_status: boolean (nullable = true)
     |-- lang: string (nullable = true)
     |-- place: struct (nullable = true)
     |-- possibly_sensitive: boolean (nullable = true)
     |-- quoted_status: struct Status (recursive)
     |-- quoted_status_id: long (nullable = true)
     |-- quoted_status_id_str: string (nullable = true)
     |-- retweet_count: long (nullable = true)
     |-- retweeted: boolean (nullable = true)
     |-- retweeted_status: struct Status
     |-- source: string (nullable = true)
     |-- text: string (nullable = true)
     |-- timestamp_ms: string (nullable = true)
     |-- truncated: boolean (nullable = true)
     |-- twinl_lang: string (nullable = true)
     |-- twinl_source: array (nullable = true)
     |    |-- element: string (containsNull = true)
     |-- user: struct User (see below)

     
     struct User
     |-- contributors_enabled: boolean (nullable = true)
     |-- created_at: string (nullable = true)
     |-- default_profile: boolean (nullable = true)
     |-- default_profile_image: boolean (nullable = true)
     |-- description: string (nullable = true)
     |-- favourites_count: long (nullable = true)
     |-- follow_request_sent: string (nullable = true)
     |-- followers_count: long (nullable = true)
     |-- following: string (nullable = true)
     |-- friends_count: long (nullable = true)
     |-- geo_enabled: boolean (nullable = true)
     |-- id: long (nullable = true)
     |-- id_str: string (nullable = true)
     |-- is_translator: boolean (nullable = true)
     |-- lang: string (nullable = true)
     |-- listed_count: long (nullable = true)
     |-- location: string (nullable = true)
     |-- name: string (nullable = true)
     |-- notifications: string (nullable = true)
     |-- profile_background_color: string (nullable = true)
     |-- profile_background_image_url: string (nullable = true)
     |-- profile_background_image_url_https: string (nullable = true)
     |-- profile_background_tile: boolean (nullable = true)
     |-- profile_banner_url: string (nullable = true)
     |-- profile_image_url: string (nullable = true)
     |-- profile_image_url_https: string (nullable = true)
     |-- profile_link_color: string (nullable = true)
     |-- profile_sidebar_border_color: string (nullable = true)
     |-- profile_sidebar_fill_color: string (nullable = true)
     |-- profile_text_color: string (nullable = true)
     |-- profile_use_background_image: boolean (nullable = true)
     |-- protected: boolean (nullable = true)
     |-- screen_name: string (nullable = true)
     |-- statuses_count: long (nullable = true)
     |-- time_zone: string (nullable = true)
     |-- url: string (nullable = true)
     |-- utc_offset: long (nullable = true)
     |-- verified: boolean (nullable = true) 
     

## Dutch Websites

Access: ``/data/commoncrawl/`` (format: One json object per line, read via ``sqlContext.read.json()``)
    
Schema: 

    root
     |-- text: string (nullable = true)
     |-- url: string (nullable = true)
     
Statistics:

* Number of pages: 27,745,047

## Volkskrant Articles

Access: ``/data/volkskrant``

Format: One json object per line (read via sqlContext.read.json())

Schema:

    +---------+---------+-------+
    | col_name|data_type|comment|
    +---------+---------+-------+
    | category|   string|       |
    |      day|   bigint|       |
    |full_html|   string|       |
    |     href|   string|       |
    |    month|   bigint|       |
    |     text|   string|       |
    |     time|   string|       |
    |timeofday|   string|       |
    |    title|   string|       |
    |     year|   bigint|       |
    +---------+---------+-------+

Statistics:

* Number of articles: 491,852
* Average title length [tokens]: 6.06
* Average text length [tokens]: 242.56 

Distribution over category and year:

    +----------------+------+    +----+------+                                                      
    |        category| count|    |year| count| 
    +----------------+------+    +----+------+ 
    | Beeldende Kunst|  2646|    |2010| 62741| 
    |      Voorpagina|  2975|    |2011| 82315| 
    |     Mode & Mooi|  1014|    |2012| 99963| 
    |      Binnenland| 58397|    |2013|100679| 
    |            Film|  3117|    |2014| 67723| 
    |       Televisie|  2778|    |2015| 45976| 
    |        Politiek| 12471|    |2016| 32455| 
    |            Foto|  6271|    +----+------+ 
    |        Cartoons|   119|    
    |           Sport| 78558|    
    |Koken &amp; Eten|  2849|    
    |        Voordeel|  1445|     
    |          Reizen|  1420|     
    |            Vonk|   517|     
    |          Boeken|  3522|     
    |        Economie| 28792|     
    |         Archief|134973|     
    |      Wetenschap| 11112|     
    |       Recensies| 17900|     
    |         Theater|  1545|     
    |      Buitenland| 70624|     
    |          Opinie| 19006|     
    |            Tech|  2504|     
    |        Magazine| 14872|     
    |           Media|  7702|     
    |          Muziek|  4520|     
    |       Festivals|   203|     
    +----------------+------+ 



## Ship Position Data

Access: `/data/aisUT` (format: one json object per line, use sqlContext.read.json())

Schema:

     root
     |-- callsign: string (nullable = true)
     |-- cog: long (nullable = true)
     |-- destination: string (nullable = true)
     |-- dimbow: long (nullable = true)
     |-- dimport: long (nullable = true)
     |-- dimstarboard: long (nullable = true)
     |-- dimstern: long (nullable = true)
     |-- draught: long (nullable = true)
     |-- eta_day: long (nullable = true)
     |-- eta_hour: long (nullable = true)
     |-- eta_minute: long (nullable = true)
     |-- eta_month: long (nullable = true)
     |-- heading: long (nullable = true)
     |-- imo: long (nullable = true)
     |-- lat: double (nullable = true)
     |-- lat2: string (nullable = true)
     |-- lon: double (nullable = true)
     |-- lon2: string (nullable = true)
     |-- mmsi: long (nullable = true)
     |-- nav_status: long (nullable = true)
     |-- rot_angle: double (nullable = true)
     |-- rot_direction: string (nullable = true)
     |-- shipname: string (nullable = true)
     |-- shiptype: long (nullable = true)
     |-- sog: long (nullable = true)
     |-- timestamp: long (nullable = true)
     |-- ts: long (nullable = true)
     |-- type: long (nullable = true)

## Road Sensor Data

Access: `/data/cbs/loopraw` (format csv)

Example: 

    RWS01_MONIBAS_0091hrl0763ra,1,11B,2014-12-27 05:51:00,2014-12-27 05:52:00,,,1,,,,,,,1,,0.000000,,,arithmeticAverageOfSamplesInATimePeriod,,0091hrl0763ra,,2,,southBound,100,60,lane2,greaterThan 12.20,52.6225,4.72241,8,5.5,B,negative,10609,150,P3.1,Tunnel,N9,Ring Alkmaar,Regulierstunnel,n.n.,generatedValue,,,mainCarriageway,Niet bepaald,Niet bepaald,"Nog niet gegenereerd"
    RWS01_MONIBAS_0091hrl0763ra,1,12B,2014-12-27 05:51:00,2014-12-27 05:52:00,,,1,,,,,,,1,,0.000000,,,arithmeticAverageOfSamplesInATimePeriod,,0091hrl0763ra,,2,,southBound,100,60,lane2,anyVehicle,52.6225,4.72241,8,5.5,B,negative,10609,150,P3.1,Tunnel,N9,Ring Alkmaar,Regulierstunnel,n.n.,generatedValue,,,mainCarriageway,Niet bepaald,Niet bepaald,"Nog niet gegenereerd"
    RWS01_MONIBAS_0091hrl0785ra,1,1B,2014-12-2...
