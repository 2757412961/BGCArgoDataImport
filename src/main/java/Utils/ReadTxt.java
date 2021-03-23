package Utils;

/**
 * @author ：Jesse.Qi
 * @date ：Created in 2019/9/10 21:50
 * @description：
 * @modified By：
 * @version: $
 */

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.Set;

public class ReadTxt extends ReadBase {

    private static ReadTxt ReadTxtInstance = null;
    private static ArrayList<String> metaUrl = new ArrayList<String>();
    private static ArrayList<String> profileUrl = new ArrayList<String>();
    private static ArrayList<String> all_metadata = new ArrayList<String>();
    ArrayList<String> unsureList = new ArrayList<String>();

    protected ReadTxt() {
    }

    public static ReadTxt getInstance() {
        if (ReadTxtInstance == null) {
            ReadTxtInstance = new ReadTxt();
        }

        return ReadTxtInstance;
    }

    public String getDataIdentity() {
        return "ReadTxt";
    }

    public ArrayList<String> makemetaUrl() {
        for (int i = 0; i < profileUrl.size(); i++) { // aoml/1900022/1900022_meta.nc
            String[] splited = profileUrl.get(i).split("/");
            if (splited.length > 1) {
                metaUrl.add(splited[0] + "/" + splited[1] + "/" + splited[1] + "_meta.nc");
            }
        }

        Set<String> middleHashSet = new LinkedHashSet<String>(metaUrl);
        metaUrl = new ArrayList<String>(middleHashSet);

        return metaUrl;
    }

    public ArrayList<String> getmetaUrl() {
        return metaUrl;
    }

    public ArrayList<String> getProfileUrl() { // csio/2902753/profiles/MR2902753_001.nc
        return profileUrl;
    }

    //第一次运行，通过ar_index_global_meta.txt（1W+）填写元数据表all_metadata
    public void Read_allmetaIndex(String allmetaIndex) {
//        allmetaIndex = "F:\\BGCArgo\\BgcData\\part_ar_index_global_meta.txt";
        File file = new File(allmetaIndex);
        String fileName = file.getName();
        ArrayList<String> insertdataList = new ArrayList<String>();
        FileReader fr = null;
        BufferedReader br = null;
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String tmpLine = br.readLine();
            while (tmpLine != null && tmpLine.startsWith("# ")) {
                tmpLine = br.readLine();
            }
            tmpLine = br.readLine();  //跳过列名行
            while (tmpLine != null) {
                String[] splited = tmpLine.split(",");// 数据是用,进行分隔的
                if (splited.length == 4) {
                    insertdataList.add(splited[0].substring(splited[0].lastIndexOf("/") + 1, splited[0].lastIndexOf("_")));
                    //platform_number
                    insertdataList.add(splited[2]);//institution
                    insertdataList.add(splited[0]);//t_file
                    insertdataList.add(splited[1]);//profiler_type
                    insertdataList.add(splited[3]);//date_update
                    insertdataList.add(getNow());
                    Insert_allmeta(insertdataList);  //2020.4更新：会先判断数据库中是否存在该元数据，已存在则不进行操作
//                    all_metadata.add(splited[0].substring(splited[0].lastIndexOf("/") + 1, splited[0].lastIndexOf("_")));
//                    LogUtils.getInstance().logInfo("Insert into all_metadata " + splited[0]);
                    insertdataList.clear();
                }
                tmpLine = br.readLine();
            }
        } catch (Exception e) {
            LogUtils.getInstance().logException(e, fileName + "读取文件失败");
        } finally {
            if (br != null) {
                try {
                    br.close();
                    fr.close();
                } catch (Exception e) {
                    LogUtils.getInstance().logException(e);
                }
            }
        }
//        String sql = "select platform_number from all_metadata";
//        ArrayList<String> database=DBUtils.getInstance().executeQueryList(sql);
//        ArrayList<String> wrong=new ArrayList<String>();
//        for (int i = 0; i < database.size(); i++) {
//            if(!all_metadata.contains(database.get(i)))
//            {wrong.add(database.get(i));}
//        }
    }

    //每周通过ar_index_this_week_meta.txt，更新all_metadata
    public void Read_allmetaIndex_week(String allmetaWeek) {
        File file = new File(allmetaWeek);
        String fileName = file.getName();
        ArrayList<String> mataweekList = new ArrayList<String>();
        FileReader fr = null;
        BufferedReader br = null;
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String tmpLine = br.readLine();
            while (tmpLine != null && tmpLine.startsWith("# ")) {
                tmpLine = br.readLine();
            }
            tmpLine = br.readLine();  //跳过列名行
            while (tmpLine != null) {
                String[] splited = tmpLine.split(",");// 数据是用,进行分隔的
                if (splited.length == 4) {
                    mataweekList.add(splited[0].substring(splited[0].lastIndexOf("/") + 1, splited[0].lastIndexOf("_")));
                    //platform_number
                    mataweekList.add(splited[2]);//institution
                    mataweekList.add(splited[0]);//t_file
                    mataweekList.add(splited[1]);//profiler_type
                    mataweekList.add(splited[3]);//date_update
                    mataweekList.add(getNow());
                    Update_allmeta(mataweekList);
                    mataweekList.clear();
                }
                tmpLine = br.readLine();
            }
        } catch (Exception e) {
            LogUtils.getInstance().logException(e, fileName + "读取文件失败");
        } finally {
            if (br != null) {
                try {
                    br.close();
                    fr.close();
                } catch (Exception e) {
                    LogUtils.getInstance().logException(e);
                }
            }
        }
    }

    //插入all_meta表(第一次和每周)
    public void Insert_allmeta(ArrayList<String> insertdataList) {
        String SQL = "";
        if (!DBUtils.getInstance().isExistData("all_metadata", "platform_number", insertdataList.get(0))) {
            SQL = "Insert into all_metadata (\n" +
                    "\tplatform_number, institution, t_file, profiler_type, date_update,  time_write, \"isBGC\")\n" +
                    "\tVALUES ('" + insertdataList.get(0) + "','" + insertdataList.get(1) + "','" + insertdataList.get(2) + "','"
                    + insertdataList.get(3) + "',to_timestamp('" + insertdataList.get(4) + "','YYYYMMDDHH24MISS'),"
                    + "to_timestamp('" + insertdataList.get(5) + "','YYYYMMDDHH24MISS')," + "false" + ");";
            DBUtils.getInstance().execute(SQL);
            LogUtils.getInstance().logInfo("Insert into all_metadata " + insertdataList.get(0));
        } else {
            LogUtils.getInstance().logInfo("all_meta: platform_number " + insertdataList.get(0) + " already exits in Database!");
        }

    }

    //第一次运行，通过argo_bio-profile_index.txt反馈bgc信息给all_metadata，同时填写元数据表bgc_metadata的部分内容
    public void Read_bgcIndex(String ProfileIndex) {
//        ProfileIndex = "F:\\BGCArgo\\BgcData\\part_argo_merge-profile_index.txt";
//        ProfileIndex = "F:\\BGCArgo\\BgcData\\part_argo_synthetic-profile_index.txt";
        File file = new File(ProfileIndex);
        String fileName = file.getName();
        ArrayList<String> bgcList = new ArrayList<String>();
        ArrayList<String> bgcProfile = new ArrayList<String>();
        FileReader fr = null;
        BufferedReader br = null;
        int reversed = 0;
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String tmpLine = br.readLine();
            while (tmpLine != null && tmpLine.startsWith("# ")) {
                tmpLine = br.readLine();
            }
            tmpLine = br.readLine();  //跳过列名行
            while (tmpLine != null) {
                if (tmpLine.contains("D.nc")) {
                    reversed++;
                    tmpLine = br.readLine();
                    continue; //跳过反向检测的浮标
                }
                String[] splited = tmpLine.split(",");// 数据是用,进行分隔的
                if (splited.length == 10) {
                    bgcList.add(splited[0].substring(splited[0].indexOf("/") + 1, splited[0].indexOf("profiles") - 1));
                    profileUrl.add(splited[0]);
                    for (int i = 0; i < splited.length; i++) {
                        bgcProfile.add(splited[i]);
                    }//这个遍历可以直接转成一句话的
//                    Update_bgcProfile(bgcProfile);  // 2020.4更新：会先判断bgc_profile中是否存在该剖面数据，存在则更新，不存在则插入
                    Insert_bgcProfile(bgcProfile);  // 20200912 启用, Update_bgcProfile字段不匹配
                    bgcProfile.clear();
                }
                tmpLine = br.readLine();
            }
        } catch (Exception e) {
            LogUtils.getInstance().logException(e, fileName + "读取文件失败");
        } finally {
            if (br != null) {
                try {
                    br.close();
                    fr.close();
                } catch (Exception e) {
                    LogUtils.getInstance().logException(e);
                }
            }
        }
        LogUtils.getInstance().logInfo("Reverse Profile：" + reversed);
        Set<String> middleHashSet = new LinkedHashSet<String>(bgcList);
        bgcList = new ArrayList<String>(middleHashSet);
        Improve_allmeta(bgcList); //更新all_metadata表的bgc信息
        Insert_bgcmeta(bgcList);  //2020.4更新：会先判断bgc_metadata中是否存在该元数据，存在则更新，不存在则插入
//        Insert_bgcProfile(ProfileIndex);//通过bgc_Index插入bgc_Profile表,已在循环中完成
        lastCycle(bgcList);//判断最近剖面,且需要返回5个字段给bgc_metadata——4.20已检查
    }

    //每周通过ar_index_this_week_prof.txt，更新bgcprofile表
    public void Read_bgcIndex_week(String ProfileWeek) {
        File file = new File(ProfileWeek);
        String fileName = file.getName();
        ArrayList<String> bgcList = new ArrayList<String>();//
        ArrayList<String> bgcProfile = new ArrayList<String>();
        unsureList.clear();
        String metaName = "";
        FileReader fr = null;
        BufferedReader br = null;
        try {
            fr = new FileReader(file);
            br = new BufferedReader(fr);
            String tmpLine = br.readLine();
            while (tmpLine != null && tmpLine.startsWith("# ")) {
                tmpLine = br.readLine();
            }
            tmpLine = br.readLine();  //跳过列名行
            while (tmpLine != null) {
                String[] splited = tmpLine.split(",");// 数据是用,进行分隔的
                if (splited.length == 8) {
                    metaName = splited[0].substring(splited[0].indexOf("/") + 1, splited[0].indexOf("profiles") - 1);
                    if (DBUtils.getInstance().isExistData("bgc_metadata", "platform_number", metaName)) {
                        bgcList.add(metaName);
                        profileUrl.add(splited[0]);
                        for (int i = 0; i < splited.length; i++) {
                            bgcProfile.add(splited[i]);
                        }
                        Update_bgcProfile(bgcProfile);//week的insert需要判断是否已有该浮标
                    }
                    unsureList.add(splited[0]); // 应该放在else子句里 ??
                    bgcProfile.clear();
                }
                tmpLine = br.readLine();
            }
        } catch (Exception e) {
            LogUtils.getInstance().logException(e, fileName + "读取文件失败");
        } finally {
            if (br != null) {
                try {
                    br.close();
                    fr.close();
                } catch (Exception e) {
                    LogUtils.getInstance().logException(e);
                }
            }
        }
//        unsureList;
//        Improve_allmeta(bgcList); //更新all_metadata表的bgc信息
        Update_bgcmeta(bgcList); //插入bgc_metadata表(第一次) // 4.22 没有去重，建议使用Set
        lastCycle(bgcList);
    }


    //完善all_metadata表的bgc信息(第一次和每周)
    public void Improve_allmeta(ArrayList<String> bgcList) {
        if (bgcList.size()<=0)return;
        String SQL = "Update  all_metadata set \"isBGC\"=true where (";
        for (int i = 0; i < bgcList.size() - 1; i++) {
            SQL += " platform_number='" + bgcList.get(i) + "'or ";
        }
        SQL += " platform_number='" + bgcList.get(bgcList.size() - 1) + "' )  ";
        DBUtils.getInstance().executeUpdate(SQL);
    }

    //更新all_metadata表(每周)
    public void Update_allmeta(ArrayList<String> mataweekList) {
        //首先执行语句，更新bgc_metadata 表
        String SQL = "Update  bgc_metadata set date_update=to_timestamp('" + mataweekList.get(4) + "','YYYYMMDDHH24MISS'), "
                + " time_write=to_timestamp('" + mataweekList.get(5) + "','YYYYMMDDHH24MISS') "
                + "where platform_number='" + mataweekList.get(0) + "'  ";
        if (DBUtils.getInstance().executeUpdate(SQL) > 0) {
            //如果成功更新bgc_metadata表，该meta必定需要下载，且all_metadata表也一起更新相应信息
            metaUrl.add(mataweekList.get(1));
            SQL = "Update  all_metadata set date_update=to_timestamp('" + mataweekList.get(4) + "','YYYYMMDDHH24MISS'), "
                    + " time_write=to_timestamp('" + mataweekList.get(5) + "','YYYYMMDDHH24MISS') "
                    + "where platform_number='" + mataweekList.get(0) + "' and \"isBGC\"=true ";
            DBUtils.getInstance().executeUpdate(SQL);
        } else {
            //如果bgc_metadata表中没有该meta，初步认定是非bgcmeta，试图更新all_metadata
            SQL = "Update  all_metadata set date_update=to_timestamp('" + mataweekList.get(4) + "','YYYYMMDDHH24MISS'), "
                    + " time_write=to_timestamp('" + mataweekList.get(5) + "','YYYYMMDDHH24MISS') "
                    + " where platform_number='" + mataweekList.get(0) + "' ";
            //该语句的效果为找出新增的浮标，注意，此时并不知道该浮标是否为bgc浮标
            if (DBUtils.getInstance().executeUpdate(SQL) < 1) {
                //如果all_metadata表中没有该meta，须插入，且标注isBgc为false
                Insert_allmeta(mataweekList);  //(该函数在4月18日有新的改动，造成多判断了一次)
            }
        }
    }

    //插入bgc_metadata表(第一次)
    public void Insert_bgcmeta(ArrayList<String> bgcList) {
        String SQL = "";
        String timeNow = getNow();
        for (String platform_number : bgcList) {

            if (!DBUtils.getInstance().isExistData("bgc_metadata", "platform_number", platform_number)) {

//            if (DBUtils.getInstance().isExistData("bgc_metadata", "profile_name", profile_name)) {
//            SQL="select platform_number from bgc_metadata where platform_number='" +bgcList.get(i)+"' ";
//            if(DBUtils.getInstance().executeQueryScalarInt(SQL)>0)
                SQL = "insert into bgc_metadata select platform_number,institution,t_file,profiler_type,date_update "
                        + "from all_metadata where platform_number='" + platform_number + "' ";
                //即将all_metadata中bgc的数据复制到bgc_metadata中
                DBUtils.getInstance().execute(SQL);
                SQL = "Update bgc_metadata set time_write=to_timestamp('" + timeNow + "','YYYYMMDDHH24MISS') " +
                        "where platform_number='" + platform_number + "' "; // 要加判断，否则全部数据都改掉了
                DBUtils.getInstance().executeUpdate(SQL);
                LogUtils.getInstance().logInfo("Insert into bgc_metadata " + platform_number);
                //     "where \"isBGC\"=true;";
            } else {
                SQL = "Update bgc_metadata set " +
                        "date_update=all_metadata.date_update," +
                        "profiler_type=all_metadata.profiler_type," +
                        "time_write=to_timestamp('" + getNow() + "','YYYYMMDDHH24MISS') " +
                        "from all_metadata " +
                        "where bgc_metadata.platform_number=all_metadata.platform_number "
                        + "and all_metadata.platform_number='" + platform_number + "' ";
                DBUtils.getInstance().executeUpdate(SQL);
                LogUtils.getInstance().logInfo("Update bgc_metadata " + platform_number);
            }
        }


//        DBUtils.getInstance().execute(SQL);
    }

    //更新bgc_metadata表(每周)
    public void Update_bgcmeta(ArrayList<String> bgcList) {
        //将all_metadata的原bgc剖面基础信息更新过来
        String SQL = "Update bgc_metadata set date_update=all_metadata.date_update," +
                "profiler_type=all_metadata.profiler_type," +
                "time_write=to_timestamp('" + getNow() + "','YYYYMMDDHH24MISS') " +
                "FROM all_metadata " +
                "where bgc_metadata.platform_number=all_metadata.platform_number ";
        //该sql没加date_update
        DBUtils.getInstance().executeUpdate(SQL);
//这个逐一检验要改掉
        for (String platform_number : bgcList) {
            //逐一检验
            SQL = "select * from bgc_metadata where platform_number='" + platform_number + "' ";
            if (DBUtils.getInstance().executeNonQuery(SQL) < 1) {
                ArrayList<String> temp = new ArrayList<String>();
                temp.add(platform_number);
                Insert_bgcmeta(temp);
            }
        }
    }

    //插入bgc_profile表(第一次)——4.20查看：暂时用不着这个函数 // 20200912 启用该函数
    public void Insert_bgcProfile(ArrayList<String> bgcProfile) {
//        String profile_name = bgcProfile.get(0).substring(bgcProfile.get(0).lastIndexOf("/M") + 3, bgcProfile.get(0).lastIndexOf(".nc"));
        String profile_name = bgcProfile.get(0).substring(bgcProfile.get(0).indexOf(".nc") - 11, bgcProfile.get(0).indexOf(".nc"));
        if (bgcProfile.get(2).equals("") || bgcProfile.get(3).equals("")) {
            LogUtils.getInstance().logInfo("No coordinate " + profile_name);
            return;
        }
        if (DBUtils.getInstance().isExistData("bgc_profile", "profile_name", profile_name)) {
            LogUtils.getInstance().logInfo("bgc_profile: profile_name " + profile_name + " already exits in Database!");
        } else {
            String[] splited = profile_name.split("_");
            String SQL = "Insert into bgc_profile (t_file,date,latitude,longitude,ocean,profiler_type,institution,parameters,parameter_data_mode,date_update,time_write,profile_name,platform_number,cycle_number )"
                    + " VALUES ('" + bgcProfile.get(0) + "',to_timestamp('" + bgcProfile.get(1) + "','YYYYMMDDHH24MISS'),'" + bgcProfile.get(2) + "','"
                    + bgcProfile.get(3) + "','" + bgcProfile.get(4) + "','" + bgcProfile.get(5) + "','" + bgcProfile.get(6) + "','" + bgcProfile.get(7) + "','" + bgcProfile.get(8) + "',"
                    + "to_timestamp('" + bgcProfile.get(9) + "','YYYYMMDDHH24MISS')," + "to_timestamp('" + getNow() + "','YYYYMMDDHH24MISS'),"
                    + "'" + profile_name + "','" + splited[0] + "','" + splited[1] + "')";
            DBUtils.getInstance().execute(SQL);
        }
    }

    //更新bgc_profile表(每周)，由于是用weekindex，缺少parameters,parameter_data_mode信息
    public void Update_bgcProfile(ArrayList<String> bgcProfile) {
        String profile_name = bgcProfile.get(0).substring(bgcProfile.get(0).lastIndexOf('/') + 2, bgcProfile.get(0).indexOf(".nc"));
//        String profile_name = bgcProfile.get(0).substring(bgcProfile.get(0).lastIndexOf("/M") + 3, bgcProfile.get(0).lastIndexOf(".nc"));
        String SQL = "";
        if (bgcProfile.get(2).equals("") || bgcProfile.get(3).equals("")) {
            LogUtils.getInstance().logInfo("No coordinate " + profile_name);
            return;
        }
        //判断是否已经存在
        if (DBUtils.getInstance().isExistData("bgc_profile", "profile_name", profile_name)) {
            SQL = "Update  bgc_profile set date_update=to_timestamp('" + bgcProfile.get(7) + "','YYYYMMDDHH24MISS'), "
                    + " time_write=to_timestamp('" + getNow() + "','YYYYMMDDHH24MISS'), "
                    + " date=to_timestamp('" + bgcProfile.get(1) + "','YYYYMMDDHH24MISS'), "
                    + " latitude=" + bgcProfile.get(2) + " , longitude=" + bgcProfile.get(3) + " , ocean='" + bgcProfile.get(4) + "' "
                    + " where profile_name='" + profile_name + "' ";
            DBUtils.getInstance().executeUpdate(SQL);
            LogUtils.getInstance().logInfo("Update bgc_profile " + profile_name);
        } else {
            String[] splited = profile_name.split("_");
            SQL = "Insert into bgc_profile (t_file,date,latitude,longitude,ocean,profiler_type,institution,date_update,time_write,profile_name,platform_number,cycle_number )"
                    + " VALUES ('" + bgcProfile.get(0) + "',to_timestamp('" + bgcProfile.get(1) + "','YYYYMMDDHH24MISS'),'" + bgcProfile.get(2) + "','"
                    + bgcProfile.get(3) + "','" + bgcProfile.get(4) + "','" + bgcProfile.get(5) + "','" + bgcProfile.get(6) + "',"
                    + "to_timestamp('" + bgcProfile.get(7) + "','YYYYMMDDHH24MISS')," + "to_timestamp('" + getNow() + "','YYYYMMDDHH24MISS'),"
                    + "'" + profile_name + "','" + splited[0] + "','" + splited[1] + "')";
            DBUtils.getInstance().execute(SQL);
            LogUtils.getInstance().logInfo("Insert into bgc_profile " + profile_name);
        }

    }

}
