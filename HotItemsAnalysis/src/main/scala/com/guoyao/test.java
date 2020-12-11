package com.guoyao;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.Arrays;

import java.util.*;


public class test {
//     private Jedis jedis=null;
//     private static GeometryFactory geoFactory = new GeometryFactory();
//     private static SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//     Map hashMap = new HashMap();
//     public boolean processRow(StepMetaInterface smi, StepDataInterface sdi) throws KettleException {
//         //FileWriter fw = null;
//
//         BufferedWriter fw = null;
//         if (first) {
//             first = false;
//             //jedis = new Jedis("192.168.0.230");
//
//
//
// 	    /* TODO: Your code here. (Using info fields)get(Fields.In, "draught").setValue(r, value);
//
// 	    FieldHelper infoField = get(Fields.Info, "info_field_name");
//
// 	    RowSet infoStream = findInfoRowSet("info_stream_tag");
//
// 	    Object[] infoRow = null;
//
// 	    int infoRowCount = 0;
//
// 	    // Read all rows from info step before calling getRow() method, which returns first row from any
// 	    // input rowset. As rowMeta for info and input steps varies getRow() can lead to errors.
// 	    while((infoRow = getRowFrom(infoStream)) != null){
//
// 	      // do something with info data
// 	      infoRowCount++;
// 	    }
// 	    */
//         }
//         try {
//             Object[] r = getRow();
//             //System.out.println(r);
//             r = Arrays.copyOfRange(r, 0, 14);
//
//             if (r == null) {
//                 setOutputDone();
//                 return false;
//             }
//
//
//             // It is always safest to call createOutputRow() to ensure that your output row's Object[] is large
//             // enough to handle any new fields you are creating in this step.
//             //r = createOutputRow(r, data.outputRowMeta.size());
//
//
// 	  /* TODO: Your code here. (See Sample)
//
// 	  // Get the value from an input field
// 	  String foobar = get(Fields.In, "a_fieldname").getString(r);
//
// 	  foobar += "bar";
//
// 	  // Set a value in a new output field
// 	  get(Fields.Out, "output_fieldname").setValue(r, foobar);
//
// 	  */
//
// 	/*
// 	从输入来的AIS拿到  各个字段。
// 	*/
//             String mmsi = get(Fields.In, "mmsi").getString(r);
//             String updatetime = get(Fields.In, "updatetime").getString(r);
//             String lon = get(Fields.In, "lon").getString(r);
//             String lat = get(Fields.In, "lat").getString(r);
//             String course = get(Fields.In, "course").getString(r);
//             String speed = get(Fields.In, "speed").getString(r);
//             String heading = get(Fields.In, "heading").getString(r);
//             String rot = get(Fields.In, "rot").getString(r);
//             String status = get(Fields.In, "status").getString(r);
//             String static_info_updatetime = get(Fields.In, "static_info_updatetime").getString(r);
//             String eta = get(Fields.In, "eta").getString(r);
//             String dest = get(Fields.In, "dest").getString(r);
//             String destination_tidied = get(Fields.In, "destination_tidied").getString(r);
//             String draught = get(Fields.In, "draught").getString(r);
//
//             String redis_dest=(String)hashMap.get(mmsi+"_dest");
//             String redis_destination_tidied=(String)hashMap.get(mmsi+"_destination_tidied");
//             String redis_draught=(String)hashMap.get(mmsi+"_draught");
//             String redis_eta =(String)hashMap.get(mmsi+"_eta");
//             String redis_static_info_updatetime =(String)hashMap.get(mmsi+"_static_info_updatetime");
//             //String maxDraught = (String)hashMap.get(mmsi+"_draught");
//             String last_lon = (String)hashMap.get(mmsi+"_lon");
//             String last_lat = (String)hashMap.get(mmsi+"_lat");
//             String last_updatetime = (String)hashMap.get(mmsi+"_updatetime");
//             if(last_lon == null){
//                 hashMap.put(mmsi+"_lon",lon);
//                 hashMap.put(mmsi+"_lat",lat);
//                 hashMap.put(mmsi+"_updatetime",updatetime);
//             }
//             //先查redis看是否有mmsi对应的键值，没有put，有的话对比value值是否一致，一致就不put，不一致就put更改redis对应的mmsi的键值。
// 		/*String redis_dest=jedis.hget("ADIMS:ETL:"+mmsi,"dest");
// 		String redis_destination_tidied=jedis.hget("ADIMS:ETL:"+mmsi,"destination_tidied");
// 		String redis_draught=jedis.hget("ADIMS:ETL:"+mmsi,"draught");
// 		String redis_eta = jedis.hget("ADIMS:ETL:"+mmsi, "eta");
// 		String maxDraught = jedis.hget("ADIMS:Entity.TShips:"+mmsi, "draught");
// 		String last_lon = jedis.hget("ADIMS:Entity:LAST:"+mmsi, "lon");
// 		String last_lat = jedis.hget("ADIMS:Entity:LAST:"+mmsi, "lat");
// 		String last_updatetime = jedis.hget("ADIMS:Entity:LAST:"+mmsi, "updatetime");
// 		if(last_lon == null){
// 			jedis.hset("ADIMS:Entity:LAST:"+mmsi,"lon", lon);
// 			jedis.hset("ADIMS:Entity:LAST:"+mmsi,"lat", lat);
// 			jedis.hset("ADIMS:Entity:LAST:"+mmsi,"updatetime", updatetime);
// 		}*/
//
//             //double maxDraught2 = Double.parseDouble(maxDraught);
//
//             // 吃水深度
//             if("".equals(draught)||draught == null ||"0".equals(draught)){    	//如果ais中draught吃水深度为空或不正确
//                 if(redis_draught != null&&!"".equals(redis_draught)){		//如果redis中是正确的话
//                     get(Fields.In, "draught").setValue(r, redis_draught);  //
//                     draught = redis_draught;   	//将redis的赋给ais的
//                 }else{
//                     return true;
//                 }
//
//             }else{
//                 if(redis_draught == null||"".equals(redis_draught)||!draught.equals(redis_draught)){  //如果ais中数据正确，但redis中不正确或者与redis中数据不符，将redis改为ais的
//                     //jedis.hset("ADIMS:ETL:"+mmsi,"draught", draught);
//                     hashMap.put(mmsi+"_draught",draught);
//                 }
//             }
//
//
//             // 预计到达时间
//             if("".equals(eta)||eta == null ||"0".equals(eta)){
//                 if(redis_eta != null&&!"".equals(redis_eta)){		//如果ais中eta数据不正确，redis中数据正确，那么将ais改为redis正确的。
//                     //eta = redis_eta;
//                     get(Fields.In, "eta").setValue(r, redis_eta);
//                 }
//
//             }else{
//                 if(redis_eta == null||"".equals(redis_eta)||!eta.equals(redis_eta)){	//如果ais中eta数据正确，redis中数据不正确，将redis改为ais正确的
//                     //jedis.hset("ADIMS:ETL:"+mmsi,"eta", eta);
//                     hashMap.put(mmsi+"_eta",eta);
//                 }
//             }
//
//
//
//             // 目的港
//             if("".equals(dest)||dest == null ||"0".equals(dest)){
//                 if(redis_dest != null||!"".equals(redis_dest)){
//                     get(Fields.In, "dest").setValue(r, redis_dest);
//                     //dest = redis_dest;
//                 }
//
//             }else{
//                 if(redis_dest == null||"".equals(redis_dest)||!dest.equals(redis_dest)){
//                     //jedis.hset("ADIMS:ETL:"+mmsi,"dest", dest);
//                     hashMap.put(mmsi+"_dest",dest);
//
//                 }
//             }
//
//
//             // 目的港修正
//             if("".equals(destination_tidied)||destination_tidied == null ||"0".equals(destination_tidied)){
//                 if(redis_destination_tidied != null||!"".equals(redis_destination_tidied)){
//                     //destination_tidied = redis_destination_tidied;
//                     get(Fields.In, "destination_tidied").setValue(r, redis_destination_tidied);
//                 }
//
//             }else{
//                 if(redis_destination_tidied == null||"".equals(redis_destination_tidied)||!destination_tidied.equals(redis_destination_tidied)){
//                     //jedis.hset("ADIMS:ETL:"+mmsi,"destination_tidied", destination_tidied);
//                     hashMap.put(mmsi+"_destination_tidied",destination_tidied);
//                 }
//             }
//
//
//
//             // 静态信息更新时间
//             if("".equals(static_info_updatetime)||static_info_updatetime == null ||"0".equals(static_info_updatetime)){
//                 if(redis_static_info_updatetime != null||!"".equals(redis_static_info_updatetime)){
//                     //destination_tidied = redis_destination_tidied;
//                     get(Fields.In, "static_info_updatetime").setValue(r, redis_static_info_updatetime);
//                 }
//
//             }else{
//                 if(redis_static_info_updatetime == null||"".equals(redis_static_info_updatetime)||!static_info_updatetime.equals(redis_static_info_updatetime)){
//                     //jedis.hset("ADIMS:ETL:"+mmsi,"destination_tidied", destination_tidied);
//                     hashMap.put(mmsi+"_static_info_updatetime",static_info_updatetime);
//                 }
//             }
//
//
// // ====================================================================================================
//
//
//             //过滤
//             //判断吃水是否在区间内
//             if(draught !=null&&!"".equals(draught)){
//
//                 double draught_ = Double.parseDouble(draught);
//
//                 //if(draught_ >= 0.5 *maxDraught2 && draught_<=1.2*maxDraught2){
//
//                 if(last_lon != null&&last_lat!=null&&last_updatetime != null){
//
//                     String last_point = "POINT("+last_lon+" "+last_lat+")";
//                     String point = "POINT("+lon+" "+lat+")";
//                     boolean flag = isIgnore(last_updatetime,Double.parseDouble(last_lon),Double.parseDouble(last_lat),updatetime,Double.parseDouble(lon),Double.parseDouble(lat));
//                     if(!flag){
//                         hashMap.put(mmsi+"_lon",lon);
//                         hashMap.put(mmsi+"_lat",lat);
//                         hashMap.put(mmsi+"_updatetime",updatetime);
//                         File file2 =new File("F:/bulksample917");
//                         if  (!file2.exists()  && !file2.isDirectory())
//                         {
//                             file2.mkdir();
//                         }
//                         File file = new File("F:/bulksample917/"+mmsi+".txt");
//                         if (!file.exists()) {
//                             file.createNewFile();
//                         }
//
//                         //fw = new FileWriter(file, true);
//                         fw = new BufferedWriter (new OutputStreamWriter (new FileOutputStream (file,true),"UTF-8"));
//
//                         String str = new JSONArray(r).toString();
//                         str = str.replace("\"","");
//                         fw.write(str.substring(1,str.length()-1) + "\r\n");
//                     }
//                 }
//             }
//
//         } catch (Exception e) {
//             e.printStackTrace();
//         } finally {
//             if (fw != null) {
//                 fw.close();
//             }
//
//
//
//         }
//         return true;
//
//     }
//
//     // 是否过滤掉该点 ：true-过滤；false-保留
//     private static boolean isIgnore(String preUpdateTime, Double last_lon,Double last_lat, String curUpdateTime, Double lon,Double lat) {
//         try {
//             Double distance = GetDistance(last_lon,last_lat,lon,lat);
//             Date pre = format.parse(preUpdateTime);
//             Date cur = format.parse(curUpdateTime);
//             //间隔时间超过十天过滤
//             //if ((cur.getTime() - pre.getTime()) > 240 * 60 * 60 * 1000) {
//             // return true;
//             //}
//             //固定值：12.85m/s
//             //两点距离异常过滤
//             if (distance > 12.85 / 1000 * (cur.getTime() - pre.getTime())) {
// //	            if (speed * 1.852 * 60 * 60 * 1000 * (cur.getTime() - pre.getTime()) < distance) {
//                 return true;
//             }
//         } catch (java.text.ParseException e) {
//             e.printStackTrace();
//         }
//         return false;
//     }
//
//     public static Double calculateDistance(String pointAStr, String pointBStr) {
//         Point pointA = null;
//         Point pointB = null;
//         try {
//             WKTReader reader = new WKTReader(geoFactory);
//             pointA = (Point)reader.read(pointAStr);
//             pointB = (Point)reader.read(pointBStr);
//
//             return pointA.distance(pointB) / 180 * Math.PI * 6371000;
//         } catch (ParseException e) {
//             e.printStackTrace();
//         }
//         return 0.0;
//     }
//
//     //获取两个点直接的距离
//     public static double GetDistance(double lon1, double lat1, double lon2, double lat2) {
//         double radLat1 = rad(lat1);
//         double radLat2 = rad(lat2);
//         double a = radLat1 - radLat2;
//         double b = rad(lon1) - rad(lon2);
//         double s = 2 * Math.asin(Math.sqrt(
//                 Math.pow(Math.sin(a / 2), 2) + Math.cos(radLat1) * Math.cos(radLat2) * Math.pow(Math.sin(b / 2), 2)));
//         s = s * 6378137;
//         //s = Math.round(s * 10000) / 10000;
//         return s;
//
//     }
//
//     private static double rad(double d) {
//         return d * Math.PI / 180.0;
//     }
}
