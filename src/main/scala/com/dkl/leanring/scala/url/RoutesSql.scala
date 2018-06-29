package com.dkl.leanring.scala.url

/**
 * 路线分析用到的sql语句
 */
object RoutesSql {
  //该sql将route_freq表里的出入口收费站名字和toll_station对应的标准名字取出来
  val table_route = """
          (select
          		A.id,
          		A.en_raw_name,
          		A.ex_raw_name,
          		A.province_code,
          		A.station_name as en_station_name,
          		B.station_name as ex_station_name
          from
          (
          select
          		route_freq.id,
          		route_freq.en_raw_name,
          		route_freq.ex_raw_name,
          		route_freq.province_code,
          		toll_station.station_name
          from
          		route_freq,toll_station
          where
          		route_freq.en_raw_name=toll_station.raw_name
          and
          		route_freq.province_code = toll_station.province_code
          )A,
          (
          select
          		route_freq.id,
          		route_freq.en_raw_name,
          		route_freq.ex_raw_name,
          		route_freq.province_code,
          		toll_station.station_name
          from
          		route_freq,toll_station
          where
          		route_freq.ex_raw_name=toll_station.raw_name
          and
          		route_freq.province_code = toll_station.province_code
          )B
          where A.id= B.id
          )route
          """
  //该sql根据table_route取出来的出入口标准名字去临时表gaode将对应的经纬取出来
  val longitudeAndLatitudes = """
        select
               A.id,
               A.lng as lng1,
               A.lat as lat1,
               B.lng as lng2,
               B.lat as lat2,
               stationId1,
               stationId2
        from
        (select
                route.id,
                gaode.id as stationId1,
                lng,
                lat
          from
                route,gaode
          where
                en_station_name = station
          and
                route.province_code=gaode.province
        )A,
        (select
                route.id,
                gaode.id as stationId2,
                lng,
                lat
          from
                route,gaode
          where
                ex_station_name = station
          and
                route.province_code=gaode.province
        )B
        where A.id = B.id
        """
}